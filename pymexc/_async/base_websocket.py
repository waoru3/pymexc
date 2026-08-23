import asyncio
import json
import logging
import time
import warnings
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, List, Optional, Union

import aiohttp
import websockets.client

from pymexc.base_websocket import (
    _WebSocketManager,
    SPOT,
    FUTURES,
)
from aiohttp import ClientSession, ClientTimeout

if TYPE_CHECKING:
    from .spot import HTTP

logger = logging.getLogger(__name__)


class _AsyncWebSocketManager(_WebSocketManager):
    endpoint: str

    def __init__(
        self,
        callback_function,
        ws_name,
        api_key=None,
        api_secret=None,
        subscribe_callback=None,
        ping_interval=20,
        ping_timeout=None,
        retries=10,
        restart_on_error=True,
        trace_logging=False,
        private_auth_expire=1,
        http_proxy_host=None,
        http_proxy_port=None,
        http_no_proxy=None,
        http_proxy_auth=None,
        http_proxy_timeout=None,
        loop=None,
        proto=True,  # Changed default to True - proto is required for proper WebSocket functionality
        extend_proto_body=False,
    ):
        super().__init__(
            callback_function,
            ws_name,
            api_key=api_key,
            api_secret=api_secret,
            subscribe_callback=subscribe_callback,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            retries=retries,
            restart_on_error=restart_on_error,
            trace_logging=trace_logging,
            private_auth_expire=private_auth_expire,
            http_proxy_host=http_proxy_host,
            http_proxy_port=http_proxy_port,
            http_no_proxy=http_no_proxy,
            http_proxy_auth=http_proxy_auth,
            http_proxy_timeout=http_proxy_timeout,
            proto=proto,
            extend_proto_body=extend_proto_body,
        )
        self.connected = False
        self.loop = loop or asyncio.get_event_loop()
        # pymexc left both unset until the first connect; the teardown paths below
        # dereference them, so define them up front.
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.session: Optional[ClientSession] = None
        # Socket-scoped ping loop. The spot listenKey renewal task lives in its
        # own slot (_listen_key_renewal_task, set by the spot client only):
        # sharing one slot blocked the ping loop and let the first socket close
        # kill renewal forever (TASK-29 D3).
        self._ping_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None
        # Serializes _connect so concurrent callers cannot race on self.ws/self.session.
        self._connect_lock = asyncio.Lock()
        self._connected_url: Optional[str] = None
        # Subscription messages already sent on the current socket (reset per socket).
        self._sent_subscriptions: List[dict] = []
        # True once the current socket finished auth + replay + ping startup.
        self._setup_complete = False
        # Sticky shutdown latch (close_all/__aexit__), unlike `exited` which the sync
        # base sets on every error. Only __aenter__ reopens.
        self._closing = False

        if ping_timeout:
            warnings.warn(
                "ping_timeout is deprecated for async websockets, please use just ping_interval.",
            )

    def exit(self):
        """
        Closes the websocket connection in an async-safe way.
        """
        self._cancel_ping_timer()
        # Never self-cancel: _send_ping failure reaches exit() from INSIDE the
        # ping task (via _on_error), and cancelling the current task would abort
        # the restart_on_error reconnect that _on_error drives next. The explicit
        # loop= keeps this sync method callable outside the loop too
        # (current_task() without it raises via get_running_loop()).
        if (
            self._ping_task
            and not self._ping_task.done()
            and self._ping_task is not asyncio.current_task(loop=self.loop)
        ):
            self._ping_task.cancel()
        self.exited = True
        self.connected = False
        
        if self.ws and not self.ws.closed:
            # Fire and forget close since we can't await in sync method
            self.loop.create_task(self.ws.close())

    async def _on_open(self):
        self.connected = True
        super()._on_open()

    async def _loop_recv(self, ws: aiohttp.ClientWebSocketResponse, session: ClientSession):
        """Read loop for ONE socket. `ws`/`session` are passed in explicitly so a loop
        that outlives its socket never touches the connection that replaced it."""
        try:
            async for msg in ws:
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    await self._on_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    if self.ws is ws:
                        # aiohttp puts the exception in .data; the message itself is not
                        # raisable and would derail the handler.
                        await self._on_error(msg.data)
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
        except Exception as e:
            # Only the loop of the CURRENT socket may drive error handling / reconnect;
            # a stale loop reporting its own teardown would tear down the new socket.
            if self.ws is ws:
                await self._on_error(e)
            else:
                logger.debug(f"Ignoring error from stale {self.ws_name} socket: {e!r}")
        finally:
            # Nested so session.close() still runs when _on_close raises (it reconnects).
            try:
                if self.ws is ws:
                    await self._on_close()
            finally:
                if session and not session.closed:
                    await session.close()

    async def _on_message(self, message: str | bytes):
        """
        Parse incoming messages.
        """
        _message = super()._on_message(message, parse_only=True)
        await self.callback(_message)

    def is_connected(self):
        return self.connected

    async def _connect(self, url):
        """
        Open the websocket: ONE attempt per call.

        `retries` is accepted for API compatibility but does not retry here;
        reconnect pacing belongs to the caller (TASK-29 D4 - the old retry
        loop never decremented and a failed handshake re-raised immediately).
        """
        # Serialize: a second connect must not overwrite self.ws/self.session mid-handshake.
        async with self._connect_lock:
            await self._connect_locked(url)

    async def _connect_locked(self, url):
        async def resubscribe_to_topics():
            # Send only what the CURRENT socket has not seen yet. On a fresh socket that
            # is everything; for a caller queued behind the lock it is just whatever was
            # appended after the first caller's replay read the list.
            # Drop entries for subscriptions that were unsubscribed since: they must be
            # replayed again if they come back, and the ledger must not grow forever.
            self._sent_subscriptions = [m for m in self._sent_subscriptions if m in self.subscriptions]

            for subscription_message in list(self.subscriptions):
                if subscription_message in self._sent_subscriptions:
                    continue
                await self.ws.send_json(subscription_message)
                self._sent_subscriptions.append(subscription_message)

        self.attempting_connection = True

        self.endpoint = url

        try:
            if self._closing:
                raise RuntimeError(f"WebSocket {self.ws_name} is closed")

            if self.is_connected() and self._connected_url != url:
                # While we waited for the lock another connect published a socket on a
                # DIFFERENT endpoint (spot carries the listenKey in the URL). Riding it
                # would send our subscriptions to the wrong endpoint, so drop it first.
                logger.info(
                    f"WebSocket {self.ws_name} dropping socket on {self._connected_url} in favour of {url}"
                )
                # Retire BEFORE closing: its read loop must see itself as stale, or it
                # would run current-socket policy (cancel the listenKey keep-alive via
                # _on_close, latch `exited` via _on_error, or reconnect on its own).
                old_ws = self.ws
                self.ws = None
                self.connected = False
                self._connected_url = None
                if old_ws is not None and not old_ws.closed:
                    await old_ws.close()

            if self.is_connected() and self._setup_complete:
                # Same url, already set up: another connect did auth + replay + ping while
                # we waited for the lock. Redoing them would duplicate all three - but a
                # subscription appended since its replay still has to get across. If that
                # connect FAILED partway, _setup_complete is false and we fall through to
                # run the tail ourselves.
                logger.debug(f"WebSocket {self.ws_name} already connected to {url}")
                await resubscribe_to_topics()
                return

            # One attempt per call (TASK-29 D4): reconnect pacing belongs to the
            # caller. `retries` is kept on the constructor for API compatibility
            # but no longer drives a loop here.
            logger.info(f"WebSocket {self.ws_name} attempting connection...")

            # total bounds the HANDSHAKE only (the live socket is unaffected) and must
            # stay below the caller's own subscribe deadline, so a slow handshake fails
            # here instead of being cancelled mid-flight.
            session = ClientSession(timeout=ClientTimeout(total=20))
            try:
                ws = await session.ws_connect(
                    url=url,
                    proxy=f"http://{self.proxy_settings['http_proxy_host']}:{self.proxy_settings['http_proxy_port']}"
                    if self.proxy_settings["http_proxy_host"]
                    else None,
                    proxy_auth=self.proxy_settings["http_proxy_auth"],
                )
            except BaseException:
                # Includes CancelledError: never leak a half-built session.
                await session.close()
                raise

            if self._closing:
                # Shutdown ran while we were handshaking; publishing now would
                # resurrect the connection behind close_all()/__aexit__'s back.
                await ws.close()
                await session.close()
                raise RuntimeError(f"WebSocket {self.ws_name} was closed during connect")

            # Publish only once both exist, then hand ownership to the read loop.
            self.session = session
            self.ws = ws
            self._connected_url = url
            self._sent_subscriptions = []
            self._setup_complete = False
            self._recv_task = self.loop.create_task(self._loop_recv(ws, session))
            self._recv_task.add_done_callback(self._log_recv_task_exception)

            # parse incoming messages
            await self._on_open()

            logger.info(f"WebSocket {self.ws_name} connected")

            try:
                # If given an api_key, authenticate.
                if self.api_key and self.api_secret:
                    await self._auth()

                await resubscribe_to_topics()
                await self._send_ping()
                if not self.is_connected():
                    # _send_ping swallows transport errors into _on_error() (which
                    # exits) instead of raising; surface that as a setup failure so
                    # the retirement below runs (TASK-29 D4).
                    raise RuntimeError(f"WebSocket {self.ws_name} setup ping failed")
                self._start_ping_loop()
            except BaseException:
                # Setup tail failed after the socket was published: retire the
                # partial socket so no is_connected() consumer sees a
                # half-configured connection (TASK-29 D4). Retire BEFORE closing so
                # the read loop sees itself as stale (same rule as the
                # different-url drop above). _setup_complete is already False.
                old_ws = self.ws
                self.ws = None
                self.connected = False
                self._connected_url = None
                if old_ws is not None and not old_ws.closed:
                    await old_ws.close()
                if not session.closed:
                    await session.close()
                raise
            self._setup_complete = True
        finally:
            # Must clear on failure/cancellation too, or _on_close/_on_error stay fenced
            # out of reconnecting forever.
            self.attempting_connection = False

    @staticmethod
    def _log_recv_task_exception(task: asyncio.Task) -> None:
        """Retrieve the read loop's exception so it never surfaces as
        'Task exception was never retrieved'. Nothing awaits this task."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning(f"WebSocket receive loop ended with error: {exc!r}")

    async def _auth(self):
        msg = super()._auth(parse_only=True)

        # Authenticate with API.
        await self.ws.send_json(msg)

    async def _on_error(self, error: Exception):
        try:
            super()._on_error(error, parse_only=True)
        except Exception as exc:
            # WebSocketError = protocol error on the live socket (delivered as an ERROR
            # frame); it is a transport failure like the rest, not a caller bug.
            recoverable = (aiohttp.ClientError, aiohttp.WebSocketError, ConnectionError, asyncio.TimeoutError)
            if isinstance(error, recoverable) and exc is error:
                logger.debug(
                    "Network error handled during async websocket operation: %s", error,
                )
            else:
                raise

        # Reconnect.
        if self.restart_on_error and not self.attempting_connection and not self._closing:
            self._reset()
            await self._connect(self.endpoint)

    async def _on_close(self):
        self.connected = False
        # Ping is socket-scoped: it dies with the socket. listenKey renewal is
        # account-scoped and must survive socket churn - it is cancelled only in
        # close_all()/__aexit__ (TASK-29 D3).
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        super()._on_close()
        
        # FIX: Trigger reconnection on graceful close (same as _on_error)
        # Only reconnect if not explicitly exited and restart_on_error is enabled
        if self.restart_on_error and not self.attempting_connection and not self.exited and not self._closing:
            self._reset()
            await self._connect(self.endpoint)

    async def __aenter__(self):
        """
        Async context manager entry - ensures connection is established.
        """
        self._closing = False
        self.exited = False

        # Connect if not already connected
        if not self.is_connected() and hasattr(self, 'endpoint'):
            await self._connect(self.endpoint)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit - ensures proper cleanup.
        """
        # Latch first: a handshake already in flight must not publish its socket after
        # us, and _on_close/_on_error must not reconnect while we tear down.
        self._closing = True
        self.exited = True

        # Unsubscribe from all topics if the method exists (defined in subclasses)
        if hasattr(self, 'unsubscribe_all'):
            try:
                await self.unsubscribe_all()
            except Exception as e:
                # Never skip the closes below - the socket may already be gone.
                logger.debug(f"unsubscribe_all during shutdown failed: {e}")

        # Close the websocket connection
        if self.ws and not self.ws.closed:
            await self.ws.close()

        # Close the session
        if self.session:
            await self.session.close()

        # Client-terminal teardown: cancel the socket-scoped ping loop AND the
        # account-scoped listenKey renewal (spot private client only).
        for task in (self._ping_task, getattr(self, "_listen_key_renewal_task", None)):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Mark as disconnected
        self.connected = False

        return False  # Don't suppress exceptions

    async def close_all(self):
        """
        Close all connections and cleanup resources.
        This method is called automatically when using context manager,
        but can also be called manually.
        """
        # Latch first: a handshake already in flight must not publish its socket after
        # us, and _on_close/_on_error must not reconnect while we tear down.
        self._closing = True
        self.exited = True

        # First unsubscribe from everything
        if hasattr(self, 'unsubscribe_all'):
            try:
                await self.unsubscribe_all()
            except Exception as e:
                logger.debug(f"unsubscribe_all during shutdown failed: {e}")
            except asyncio.CancelledError:
                pass  # Task was cancelled, that's OK

        # Close websocket
        if self.ws and not self.ws.closed:
            try:
                await self.ws.close()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.debug(f"Network error closing websocket: {e}")

        # Close session
        if self.session:
            try:
                await self.session.close()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.debug(f"Network error closing session: {e}")

        # Client-terminal teardown: cancel the socket-scoped ping loop AND the
        # account-scoped listenKey renewal (spot private client only).
        for task in (self._ping_task, getattr(self, "_listen_key_renewal_task", None)):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Reset state
        self.connected = False
        self.subscriptions = []
        self.callback_directory = {}

        logger.debug(f"WebSocket {self.ws_name if hasattr(self, 'ws_name') else ''} closed and cleaned up")

    async def _process_normal_message(self, message: dict):
        callback_function, callback_data = super()._process_normal_message(message=message, parse_only=True)

        if callback_function is None:
            return

        await callback_function(callback_data)

    def _start_ping_loop(self):
        if not self.ping_interval or self.ping_interval <= 0:
            return

        if self._ping_task and not self._ping_task.done():
            return

        self._ping_task = self.loop.create_task(self._ping_loop())

    async def _ping_loop(self):
        try:
            while True:
                interval = self.ping_interval if self.ping_interval and self.ping_interval > 0 else 0
                buffer = min(5, interval * 0.1) if interval else 0
                wait_time = max(0.1, interval - buffer) if interval else 0.1

                if not self.is_connected() or not self.ws or self.ws.closed:
                    await asyncio.sleep(wait_time)
                    continue

                await self._send_ping()
                await asyncio.sleep(wait_time)
        except asyncio.CancelledError:
            pass

    async def _send_ping(self):
        if not self.ping_interval or self.ping_interval <= 0:
            return

        if not self.ws or self.ws.closed:
            return

        try:
            await self.ws.send_str(self.custom_ping_message)
        except (aiohttp.ClientError, ConnectionError, asyncio.TimeoutError) as exc:
            logger.warning(f"Ping failed: {exc}. Triggering reconnection.")
            await self._on_error(exc)


# # # # # # # # # #
#                 #
#     FUTURES     #
#                 #
# # # # # # # # # #


class _FuturesWebSocketManager(_AsyncWebSocketManager):
    def __init__(self, ws_name, **kwargs):
        callback_function = (
            kwargs.pop("callback_function") if kwargs.get("callback_function") else self._handle_incoming_message
        )

        super().__init__(callback_function, ws_name, **kwargs)

    async def subscribe(self, topic, callback, params: dict = {}):
        normalized_topic = self._topic(topic)

        subscription_args = {"method": topic, "param": params}

        # Check for duplicate subscription using (method, param) - allows same topic for different symbols
        for sub in self.subscriptions:
            if sub.get("method") == topic and sub.get("param") == params:
                logger.debug(f"Already subscribed to {topic} with params {params}, skipping")
                return

        while not (self.is_connected() and self._setup_complete):
            # Both, not either (TASK-29 D6): `connected` alone admits sends
            # during the setup tail; `_setup_complete` alone stays True while
            # bot recovery retires the socket being replaced.
            # No connect is in flight. A setup-tail failure retires the socket;
            # it raises only to `_connect`'s caller, so nothing can satisfy this wait.
            # Reconnect pacing belongs to that caller (TASK-29 D6 / PR #11 review).
            if not self.is_connected() and not self.attempting_connection:
                raise RuntimeError(f"WebSocket {self.ws_name} has no connection in flight")
            await asyncio.sleep(0.1)

        # Record intent BEFORE the send await (TASK-29 D6): a replacement
        # socket's replay reads `subscriptions` and can run while our send is
        # in flight - the desired entry and its callback must already be
        # visible, or that replay misses this subscription entirely.
        ws = self.ws  # pin: sent-state is per-socket
        if normalized_topic not in self.callback_directory:
            self._set_callback(normalized_topic, callback)
        self.subscriptions.append(subscription_args)

        # If this send raises, desired intent stays recorded and the next
        # connect's replay delivers it; the caller still sees the error.
        await ws.send_json(subscription_args)

        # Sent state only if the send rode the still-current socket AND a
        # racing replay has not already recorded it; otherwise the
        # replacement's replay owns delivery and the ledger entry.
        if self.ws is ws and subscription_args not in self._sent_subscriptions:
            self._sent_subscriptions.append(subscription_args)
        self.last_subsctiption = normalized_topic

    async def unsubscribe(self, method: str | Callable) -> None:
        if not method:
            return

        if isinstance(method, str):
            # remove callback
            self._pop_callback(method)
            # send unsub message
            await self.ws.send_json({"method": f"unsub.{method}", "param": {}})

            # remove subscription from list
            for sub in self.subscriptions:
                if sub["method"] == f"sub.{method}":
                    self.subscriptions.remove(sub)
                    break

            logger.debug(f"Unsubscribed from {method}")
        else:
            # this is a func, get name
            topic_name = method.__name__.replace("_stream", "").replace("_", ".")

            return await self.unsubscribe(topic_name)

    async def unsubscribe_all(self) -> None:
        """
        Unsubscribe from all topics at once.
        """
        if not self.subscriptions:
            return

        # Get all topics from subscriptions
        topics_to_unsub = []
        for sub in self.subscriptions:
            if sub.get("method", "").startswith("sub."):
                topic = sub["method"].replace("sub.", "")
                topics_to_unsub.append(topic)

        # Unsubscribe from all topics
        for topic in topics_to_unsub:
            await self.unsubscribe(topic)

        # Clear all subscriptions and callbacks
        self.subscriptions.clear()
        self.callback_directory = {}

        logger.debug(f"Unsubscribed from all topics")

    async def _process_auth_message(self, message: dict):
        # If we get successful futures auth, notify user
        if message.get("data") == "success":
            logger.debug(f"Authorization for {self.ws_name} successful.")
            self.auth = True

        # If we get unsuccessful auth, notify user.
        elif message.get("data") != "success":  # !!!!
            logger.debug(f"Authorization for {self.ws_name} failed. Please check your API keys and restart.")

    async def _handle_incoming_message(self, message: dict):
        def is_auth_message():
            return message.get("channel", "") == "rs.login"

        def is_subscription_message():
            return message.get("channel", "").startswith("rs.sub") or message.get("channel", "") == "rs.personal.filter"

        def is_pong_message():
            return message.get("channel", "") in ("pong", "clientId")

        def is_error_message():
            return message.get("channel", "") == "rs.error"

        if is_auth_message():
            await self._process_auth_message(message)
        elif is_subscription_message():
            self._process_subscription_message(message)
        elif is_pong_message():
            pass
        elif is_error_message():
            print(f"WebSocket return error: {message}")
        else:
            await self._process_normal_message(message)

    async def custom_topic_stream(self, topic, callback):
        return await self.subscribe(topic=topic, callback=callback)


class _FuturesWebSocket(_FuturesWebSocketManager):
    def __init__(
        self,
        api_key: str = None,
        api_secret: str = None,
        loop: asyncio.AbstractEventLoop = None,
        subscribe_callback: Callable = None,
        **kwargs,
    ):
        self.ws_name = "FuturesV1"
        self.endpoint = FUTURES
        loop = loop or asyncio.get_event_loop()

        if subscribe_callback:
            loop.create_task(self.connect())

        super().__init__(
            self.ws_name,
            api_key=api_key,
            api_secret=api_secret,
            loop=loop,
            subscribe_callback=subscribe_callback,
            **kwargs,
        )

    async def connect(self):
        if not self.is_connected():
            await self._connect(self.endpoint)

    async def _ws_subscribe(self, topic, callback, params: list = []):
        await self.connect()
        await self.subscribe(topic, callback, params)


# # # # # # # # # #
#                 #
#       SPOT      #
#                 #
# # # # # # # # # #


class _SpotWebSocketManager(_AsyncWebSocketManager):
    def __init__(self, ws_name, **kwargs):
        callback_function = (
            kwargs.pop("callback_function") if kwargs.get("callback_function") else self._handle_incoming_message
        )
        super().__init__(callback_function, ws_name, **kwargs)

        # MEXC spot protocol pings are uppercase ({"method": "PING"}); futures
        # is lowercase, so the override is scoped to spot only (TASK-29 D3).
        # The sync client keeps the shared default untouched (TASK-238).
        self.custom_ping_message = json.dumps({"method": "PING"})

        self.private_topics = ["account", "deals", "orders"]

    async def subscribe(self, topic: str, callback: Callable, params_list: list, interval: str = None):
        # Build the full subscription params (includes symbol)
        full_params = [
            "@".join(
                [f"spot@{topic}.v3.api" + (".pb" if self.proto else "")]
                + ([str(interval)] if interval else [])
                + list(map(str, params.values()))
            )
            for params in params_list
        ]

        # Check for duplicate subscription using full param string (includes symbol)
        # This allows multiple symbols to subscribe to the same topic
        for param in full_params:
            if any(param in sub.get("params", []) for sub in self.subscriptions):
                logger.debug(f"Already subscribed to {param}, skipping")
                return

        subscription_args = {
            "method": "SUBSCRIPTION",
            "params": full_params,
        }

        while not (self.is_connected() and self._setup_complete):
            # Both, not either (TASK-29 D6): `connected` alone admits sends
            # during the setup tail; `_setup_complete` alone stays True while
            # bot recovery retires the socket being replaced.
            # No connect is in flight. A setup-tail failure retires the socket;
            # it raises only to `_connect`'s caller, so nothing can satisfy this wait.
            # Reconnect pacing belongs to that caller (TASK-29 D6 / PR #11 review).
            if not self.is_connected() and not self.attempting_connection:
                raise RuntimeError(f"WebSocket {self.ws_name} has no connection in flight")
            await asyncio.sleep(0.1)

        # Record intent BEFORE the send await (TASK-29 D6): a replacement
        # socket's replay reads `subscriptions` and can run while our send is
        # in flight - the desired entry and its callback must already be
        # visible, or that replay misses this subscription entirely.
        ws = self.ws  # pin: sent-state is per-socket
        if topic not in self.callback_directory:
            self._set_callback(topic, callback)
        self.subscriptions.append(subscription_args)

        # If this send raises, desired intent stays recorded and the next
        # connect's replay delivers it; the caller still sees the error.
        await ws.send_json(subscription_args)

        # Sent state only if the send rode the still-current socket AND a
        # racing replay has not already recorded it; otherwise the
        # replacement's replay owns delivery and the ledger entry.
        if self.ws is ws and subscription_args not in self._sent_subscriptions:
            self._sent_subscriptions.append(subscription_args)
        self.last_subsctiption = topic

    async def unsubscribe(self, *topics: str | Callable):
        if all([isinstance(topic, str) for topic in topics]):
            topics = [
                f"private.{topic}"
                if topic in self.private_topics
                else f"public.{topic}"
                # if user provide function .book_ticker_stream()
                .replace("book.ticker", "bookTicker")
                for topic in topics
            ]
            # remove callbacks
            for topic in topics:
                self._pop_callback(topic)

            # send unsub message
            await self.ws.send_json(
                {
                    "method": "UNSUBSCRIPTION",
                    "params": ["@".join([f"spot@{t}.v3.api" + (".pb" if self.proto else "")]) for t in topics],
                }
            )

            # remove subscriptions from list
            for i, sub in enumerate(self.subscriptions):
                new_params = [x for x in sub["params"] for _topic in topics if _topic not in x]
                if new_params:
                    self.subscriptions[i]["params"] = new_params
                else:
                    self.subscriptions.remove(sub)
                break

            logger.debug(f"Unsubscribed from {topics}")
        else:
            # some funcs in list
            topics = [
                x.__name__.replace("_stream", "").replace("_", ".") if getattr(x, "__name__", None) else x
                #
                for x in topics
            ]
            return await self.unsubscribe(*topics)

    async def unsubscribe_all(self) -> None:
        """
        Unsubscribe from all active subscriptions at once.
        """
        if not self.subscriptions:
            return

        # Collect all params from subscriptions
        all_params = []
        for sub in self.subscriptions:
            if sub.get("params"):
                all_params.extend(sub["params"])

        # Send unsubscribe message for all
        if all_params:
            await self.ws.send_json({
                "method": "UNSUBSCRIPTION",
                "params": all_params
            })

        # Clear all subscriptions and callbacks
        self.subscriptions.clear()
        self.callback_directory = {}

        logger.debug(f"Unsubscribed from all topics")

    async def _handle_incoming_message(self, message):
        def is_subscription_message():
            if message.get("id") == 0 and message.get("code") == 0 and message.get("msg"):
                return True
            else:
                return False

        if isinstance(message, dict) and is_subscription_message():
            self._process_subscription_message(message)
        else:
            await self._process_normal_message(message)

    async def custom_topic_stream(self, topic, callback):
        return await self.subscribe(topic=topic, callback=callback)


class _SpotWebSocket(_SpotWebSocketManager):
    listenKey: str
    http: "HTTP"

    def __init__(
        self,
        endpoint: str = SPOT,
        api_key: str = None,
        api_secret: str = None,
        loop: asyncio.AbstractEventLoop = None,
        **kwargs,
    ):
        self.ws_name = "SpotV3"
        self.endpoint = endpoint
        loop = loop or asyncio.get_event_loop()

        super().__init__(self.ws_name, api_key=api_key, api_secret=api_secret, loop=loop, **kwargs)

    async def _ws_subscribe(self, topic, callback, params: list = [], interval: str = None):
        # For private topics, ensure we have a listenKey before connecting
        if topic.startswith("private.") and self.api_key and self.api_secret:
            # Wait for listenKey to be generated if needed
            if not hasattr(self, 'listenKey') or not self.listenKey:
                # Wait a bit for _keep_alive_loop to generate the listenKey
                import asyncio
                for _ in range(10):  # Try for up to 5 seconds (10 * 0.5s)
                    await asyncio.sleep(0.5)
                    if hasattr(self, 'listenKey') and self.listenKey:
                        break
                else:
                    # If still no listenKey, we have a problem
                    raise Exception("Failed to generate listenKey for private streams. Check API credentials.")

        if not self.is_connected():
            await self._connect(self.endpoint)

        await self.subscribe(topic, callback, params, interval)

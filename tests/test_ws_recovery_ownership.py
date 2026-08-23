"""TASK-29 (basis_fork) fork-side regression tests: WS recovery ownership.

D3: ping/renewal task identity split + protocol-valid spot PING
D4: honest single-attempt _connect + setup-failure retirement
D5: listenKey HTTP session closure
D6: subscribe/ledger invariants under socket swaps

Style follows tests/test_ws_teardown_race.py (fake ws/session, no network).
All managers are built with restart_on_error=False so _on_close/_on_error
tails never dial a real endpoint.
"""

import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymexc._async import base_websocket as bw
from pymexc._async.base_websocket import (
    SPOT,
    _AsyncWebSocketManager,
    _FuturesWebSocketManager,
    _SpotWebSocketManager,
)
from pymexc._async.spot import WebSocket as SpotWebSocket


class FakeWS:
    """Async-iterable stand-in for aiohttp.ClientWebSocketResponse.

    The read loop stays alive until close() is called, like a real socket
    (same shape as tests/test_ws_teardown_race.py)."""

    def __init__(self):
        self.closed = False
        self._closed_event = asyncio.Event()
        self.sent = []
        self.sent_strings = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self._closed_event.wait()
        raise StopAsyncIteration

    async def send_json(self, message):
        self.sent.append(message)

    async def send_str(self, message):
        self.sent_strings.append(message)

    async def close(self):
        self.closed = True
        self._closed_event.set()


def make_manager(cls=_AsyncWebSocketManager, ping_interval=0, **kwargs):
    kwargs.setdefault("restart_on_error", False)

    async def callback(_message):
        return None

    if cls is _AsyncWebSocketManager:
        manager = cls(callback, "test", ping_interval=ping_interval, **kwargs)
    else:
        manager = cls("test", ping_interval=ping_interval, **kwargs)
    manager.endpoint = SPOT
    return manager


async def make_running_task():
    async def run():
        await asyncio.sleep(3600)

    task = asyncio.get_running_loop().create_task(run())
    await asyncio.sleep(0)
    return task


async def drain(manager):
    """Cancel the recv task a fake _connect left behind."""
    task = getattr(manager, "_recv_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------- D3


@pytest.mark.asyncio
async def test_ping_loop_starts_alongside_renewal_task():
    """Today _start_ping_loop refuses while the renewal task occupies the shared
    slot, so the spot private ping loop never runs. Split slots fix that."""
    manager = make_manager(ping_interval=20)
    manager._listen_key_renewal_task = await make_running_task()

    manager._start_ping_loop()

    assert isinstance(manager._ping_task, asyncio.Task)
    assert manager._ping_task is not manager._listen_key_renewal_task
    manager._ping_task.cancel()
    manager._listen_key_renewal_task.cancel()


@pytest.mark.asyncio
async def test_renewal_survives_on_close_ping_task_does_not():
    """listenKey lifetime is account-scoped, not socket-scoped: the first socket
    close must not kill renewal. The ping loop IS socket-scoped."""
    manager = make_manager(ping_interval=20)
    manager._listen_key_renewal_task = await make_running_task()
    manager._start_ping_loop()
    ping_task = manager._ping_task

    await manager._on_close()

    assert ping_task.done()
    assert not manager._listen_key_renewal_task.done()
    manager._listen_key_renewal_task.cancel()


@pytest.mark.asyncio
async def test_renewal_survives_exit_transport_path():
    """Async _on_error calls exit() on ordinary transport errors while the bot
    keeps reusing the client object - renewal must survive exit()."""
    manager = make_manager(ping_interval=20)
    manager.ws = None  # exit() inspects self.ws; never published in this test
    manager._listen_key_renewal_task = await make_running_task()
    manager._start_ping_loop()
    ping_task = manager._ping_task

    manager.exit()
    await asyncio.sleep(0)

    assert ping_task.cancelled() or ping_task.done()
    assert not manager._listen_key_renewal_task.done()
    manager._listen_key_renewal_task.cancel()


@pytest.mark.asyncio
async def test_spot_ping_payload_is_uppercase_ping():
    manager = make_manager(cls=_SpotWebSocketManager, proto=False)
    assert json.loads(manager.custom_ping_message) == {"method": "PING"}


@pytest.mark.asyncio
async def test_futures_ping_payload_stays_lowercase_ping():
    manager = make_manager(cls=_FuturesWebSocketManager)
    assert json.loads(manager.custom_ping_message) == {"method": "ping"}


@pytest.mark.asyncio
async def test_spot_send_ping_sends_protocol_payload():
    manager = make_manager(cls=_SpotWebSocketManager, ping_interval=20, proto=False)
    manager.ws = FakeWS()
    manager.connected = True

    await manager._send_ping()

    assert manager.ws.sent_strings == ['{"method": "PING"}']


@pytest.mark.asyncio
async def test_exit_from_within_ping_task_does_not_self_cancel():
    """_send_ping failure runs _on_error -> exit() INSIDE the ping task. exit()
    must not cancel the task it is running in, or the restart_on_error=True
    reconnect that _on_error drives next would be aborted mid-flight for
    library consumers."""
    manager = make_manager(ping_interval=1)
    manager.ws = FakeWS()
    manager.connected = True

    calls = []

    async def ping_calls_exit():
        calls.append(True)
        manager.exit()  # what _send_ping does via _on_error on transport failure

    manager._send_ping = ping_calls_exit
    manager._start_ping_loop()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if calls:
            break

    assert calls
    assert manager._ping_task.cancelling() == 0
    manager._ping_task.cancel()


# ---------------------------------------------------------------- D4


@pytest.mark.asyncio
async def test_connect_makes_exactly_one_attempt_on_handshake_failure(monkeypatch):
    """Characterization: the old `while` never retried (no retries decrement,
    ws_connect failure re-raised). This pins the single-attempt contract so the
    loop deletion cannot regress it."""
    sessions = []

    class RefusingSession:
        def __init__(self, *args, **kwargs):
            self.closed = False
            sessions.append(self)

        async def ws_connect(self, *args, **kwargs):
            raise aiohttp.ClientError("handshake refused")

        async def close(self):
            self.closed = True

    manager = make_manager()
    monkeypatch.setattr(bw, "ClientSession", RefusingSession)

    with pytest.raises(aiohttp.ClientError):
        await manager._connect(SPOT)

    assert len(sessions) == 1
    assert sessions[0].closed is True
    assert manager.is_connected() is False


@pytest.mark.asyncio
async def test_setup_tail_failure_retires_partial_socket(monkeypatch):
    """connected=True is published at socket open, before auth/replay/ping. If
    that tail fails, the partial socket must be retired - no more
    'connected=True but unusable' residue."""
    sessions = []

    class HappySession:
        def __init__(self, *args, **kwargs):
            self.closed = False
            self.ws = FakeWS()
            sessions.append(self)

        async def ws_connect(self, *args, **kwargs):
            return self.ws

        async def close(self):
            self.closed = True

    manager = make_manager(api_key="k", api_secret="s")
    monkeypatch.setattr(bw, "ClientSession", HappySession)

    async def failing_auth():
        raise aiohttp.ClientError("auth failed")

    manager._auth = failing_auth

    with pytest.raises(aiohttp.ClientError):
        await manager._connect(SPOT)

    assert manager.is_connected() is False
    assert manager.ws is None
    assert manager._connected_url is None
    assert manager._setup_complete is False
    assert sessions[0].ws.closed is True
    assert sessions[0].closed is True
    await drain(manager)


@pytest.mark.asyncio
async def test_setup_ping_transport_failure_retires_partial_socket(monkeypatch):
    """_send_ping swallows transport errors into _on_error() (which exits)
    instead of raising, so the retirement must be reached via the
    is_connected() re-check after the ping - otherwise _setup_complete is left
    True on a dead published socket."""
    sessions = []

    class PingFailWS(FakeWS):
        async def send_str(self, message):
            raise aiohttp.ClientError("ping send failed")

    class HappySession:
        def __init__(self, *args, **kwargs):
            self.closed = False
            self.ws = PingFailWS()
            sessions.append(self)

        async def ws_connect(self, *args, **kwargs):
            return self.ws

        async def close(self):
            self.closed = True

    manager = make_manager(ping_interval=20)
    monkeypatch.setattr(bw, "ClientSession", HappySession)

    with pytest.raises(RuntimeError):
        await manager._connect(SPOT)

    assert manager.is_connected() is False
    assert manager.ws is None
    assert manager._connected_url is None
    assert manager._setup_complete is False
    assert sessions[0].ws.closed is True
    assert sessions[0].closed is True
    await drain(manager)


# ---------------------------------------------------------------- D5


@pytest.mark.asyncio
async def test_ensure_listen_key_closes_http_session():
    with patch("pymexc._async.spot.HTTP") as mock_http:
        mock_http.return_value.create_listen_key = AsyncMock(return_value={"listenKey": "test_key"})
        mock_http.return_value.session.close = AsyncMock()
        ws_client = SpotWebSocket()  # keyless: no renewal task started
        ws_client.api_key = "k"
        ws_client.api_secret = "s"
        ws_client.listenKey = None

        await ws_client._ensure_listen_key()

        assert ws_client.listenKey == "test_key"
        mock_http.return_value.session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_keep_alive_loop_closes_http_sessions():
    with patch("pymexc._async.spot.HTTP") as mock_http:
        instances = []

        def http_factory(*_args, **_kwargs):
            http = AsyncMock()
            http.create_listen_key = AsyncMock(return_value={"listenKey": "test_key"})
            http.keep_alive_listen_key = AsyncMock(return_value={"msg": "ok"})
            http.session.close = AsyncMock()
            instances.append(http)
            return http

        mock_http.side_effect = http_factory
        ws_client = SpotWebSocket()
        ws_client.api_key = "k"
        ws_client.api_secret = "s"
        ws_client.listenKey = None
        ws_client._KEEP_ALIVE_INTERVAL_SECONDS = 0.01  # shadow the 55-min class attr

        task = asyncio.create_task(ws_client._keep_alive_loop())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(instances) >= 3
        instances[0].create_listen_key.assert_awaited_once()
        instances[1].keep_alive_listen_key.assert_awaited_once()
        # The last request may be cancelled after construction but before its finally.
        instances[0].session.close.assert_awaited_once()
        instances[1].session.close.assert_awaited_once()


# ---------------------------------------------------------------- D6


class SwappingWS(FakeWS):
    """send_json succeeds, but the manager's socket is swapped mid-send - the
    shape of a bot-driven rebuild racing an in-flight subscribe."""

    def __init__(self, manager, replacement):
        super().__init__()
        self._manager = manager
        self._replacement = replacement

    async def send_json(self, message):
        await super().send_json(message)
        self._manager.ws = self._replacement


async def noop_callback(_message):
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("manager_class", "subscribe_args"),
    [
        (_FuturesWebSocketManager, ("sub.ticker", noop_callback, {"symbol": "BTC_USDT"})),
        (_SpotWebSocketManager, ("public.deals", noop_callback, [{"symbol": "BTCUSDT"}])),
    ],
    ids=("futures", "spot"),
)
async def test_subscribe_raises_after_setup_tail_failure_leaves_no_connect(
    monkeypatch, manager_class, subscribe_args
):
    """A second subscriber must not wait forever after D4 retires A's socket."""
    auth_started = asyncio.Event()
    fail_auth = asyncio.Event()

    class HappySession:
        def __init__(self, *args, **kwargs):
            self.closed = False
            self.ws = FakeWS()

        async def ws_connect(self, *args, **kwargs):
            return self.ws

        async def close(self):
            self.closed = True

    manager = make_manager(manager_class, api_key="k", api_secret="s", proto=False)
    monkeypatch.setattr(bw, "ClientSession", HappySession)

    async def blocking_auth():
        auth_started.set()
        await fail_auth.wait()
        raise aiohttp.ClientError("auth failed")

    manager._auth = blocking_auth
    connect_task = subscribe_task = None
    try:
        connect_task = asyncio.create_task(manager._connect(SPOT))
        await asyncio.wait_for(auth_started.wait(), timeout=1)
        assert manager.is_connected() is True
        assert manager._setup_complete is False
        assert manager.attempting_connection is True

        subscribe_task = asyncio.create_task(manager.subscribe(*subscribe_args))
        await asyncio.sleep(0)
        fail_auth.set()

        with pytest.raises(aiohttp.ClientError, match="auth failed"):
            await connect_task
        assert manager.is_connected() is False
        assert manager._setup_complete is False
        assert manager.attempting_connection is False
        with pytest.raises(RuntimeError) as excinfo:
            await asyncio.wait_for(subscribe_task, timeout=1)
        assert manager.ws_name in str(excinfo.value)
    finally:
        fail_auth.set()
        for task in (connect_task, subscribe_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (connect_task, subscribe_task) if task), return_exceptions=True)
        await drain(manager)


@pytest.mark.asyncio
async def test_futures_subscribe_waits_for_setup_complete():
    manager = make_manager(cls=_FuturesWebSocketManager)
    manager.ws = FakeWS()
    manager.connected = True
    manager._setup_complete = False

    subscribe_task = asyncio.create_task(
        manager.subscribe("sub.ticker", noop_callback, {"symbol": "BTC_USDT"})
    )
    await asyncio.sleep(0.3)
    assert manager.ws.sent == []  # connected alone must not admit the send

    manager._setup_complete = True
    await asyncio.wait_for(subscribe_task, timeout=2)
    assert manager.ws.sent == [{"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}]

    manager.connected = False
    # A connect is in flight, so waiting is correct.
    manager.attempting_connection = True
    subscribe_task = asyncio.create_task(
        manager.subscribe("sub.ticker", noop_callback, {"symbol": "ETH_USDT"})
    )
    await asyncio.sleep(0.3)
    assert manager.ws.sent == [{"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}]

    manager.connected = True
    await asyncio.wait_for(subscribe_task, timeout=2)
    assert manager.ws.sent == [
        {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}},
        {"method": "sub.ticker", "param": {"symbol": "ETH_USDT"}},
    ]


@pytest.mark.asyncio
async def test_spot_subscribe_waits_for_setup_complete():
    manager = make_manager(cls=_SpotWebSocketManager, proto=False)
    manager.ws = FakeWS()
    manager.connected = True
    manager._setup_complete = False

    subscribe_task = asyncio.create_task(
        manager.subscribe("public.deals", noop_callback, [{"symbol": "BTCUSDT"}])
    )
    await asyncio.sleep(0.3)
    assert manager.ws.sent == []

    manager._setup_complete = True
    await asyncio.wait_for(subscribe_task, timeout=2)
    assert manager.ws.sent == [
        {"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@BTCUSDT"]}
    ]

    manager.connected = False
    # A connect is in flight, so waiting is correct.
    manager.attempting_connection = True
    subscribe_task = asyncio.create_task(
        manager.subscribe("public.deals", noop_callback, [{"symbol": "ETHUSDT"}])
    )
    await asyncio.sleep(0.3)
    assert manager.ws.sent == [
        {"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@BTCUSDT"]}
    ]

    manager.connected = True
    await asyncio.wait_for(subscribe_task, timeout=2)
    assert manager.ws.sent == [
        {"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@BTCUSDT"]},
        {"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@ETHUSDT"]},
    ]


@pytest.mark.asyncio
async def test_futures_raced_subscribe_records_desired_but_not_sent():
    manager = make_manager(cls=_FuturesWebSocketManager)
    replacement = FakeWS()
    manager.ws = SwappingWS(manager, replacement)
    manager.connected = True
    manager._setup_complete = True

    await manager.subscribe("sub.ticker", noop_callback, {"symbol": "BTC_USDT"})

    args = {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}
    assert args in manager.subscriptions  # desired state survives the swap
    assert args not in manager._sent_subscriptions  # sent state is per-socket
    # callback registered despite the swap: replay can never deliver to a
    # topic without a callback
    assert manager._topic("sub.ticker") in manager.callback_directory


@pytest.mark.asyncio
async def test_spot_raced_subscribe_records_desired_but_not_sent():
    manager = make_manager(cls=_SpotWebSocketManager, proto=False)
    replacement = FakeWS()
    manager.ws = SwappingWS(manager, replacement)
    manager.connected = True
    manager._setup_complete = True

    await manager.subscribe("public.deals", noop_callback, [{"symbol": "BTCUSDT"}])

    args = {"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@BTCUSDT"]}
    assert args in manager.subscriptions
    assert args not in manager._sent_subscriptions
    assert "public.deals" in manager.callback_directory


@pytest.mark.asyncio
async def test_replay_during_send_delivers_presend_intent(monkeypatch):
    """The hard interleaving: a full bot-style rebuild (retire + _connect +
    replay) completes WHILE the original send is still awaited. Replay must
    already see the desired subscription and deliver it on the replacement
    socket - this is exactly why intent is recorded before the send."""
    manager = make_manager(cls=_FuturesWebSocketManager)
    new_ws = FakeWS()

    class ReplaySession:
        def __init__(self, *a, **k):
            self.closed = False

        async def ws_connect(self, *a, **k):
            return new_ws

        async def close(self):
            self.closed = True

    monkeypatch.setattr(bw, "ClientSession", ReplaySession)

    class RebuildDuringSendWS(FakeWS):
        async def send_json(self, message):
            await super().send_json(message)
            # bot recovery retires this socket and completes a reconnect
            # before the send returns
            manager.connected = False
            manager.ws = None
            manager._connected_url = None
            manager._setup_complete = False
            await manager._connect(SPOT)

    manager.ws = RebuildDuringSendWS()
    manager.connected = True
    manager._setup_complete = True

    await manager.subscribe("sub.ticker", noop_callback, {"symbol": "BTC_USDT"})

    args = {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}
    assert args in new_ws.sent  # replay delivered it
    assert manager.subscriptions.count(args) == 1
    assert manager._sent_subscriptions.count(args) == 1  # replay's entry, no duplicate
    assert manager._topic("sub.ticker") in manager.callback_directory
    await drain(manager)


@pytest.mark.asyncio
async def test_replacement_socket_replay_delivers_raced_subscription(monkeypatch):
    """The lazier interleaving: the swap happens during the send but the
    rebuild's _connect runs only LATER - the desired-but-unsent entry must be
    delivered by that later connect's replay."""
    manager = make_manager(cls=_FuturesWebSocketManager)
    replacement = FakeWS()
    manager.ws = SwappingWS(manager, replacement)
    manager.connected = True
    manager._setup_complete = True

    await manager.subscribe("sub.ticker", noop_callback, {"symbol": "BTC_USDT"})
    args = {"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}
    assert args not in manager._sent_subscriptions

    new_ws = FakeWS()

    class ReplaySession:
        def __init__(self, *a, **k):
            self.closed = False

        async def ws_connect(self, *a, **k):
            return new_ws

        async def close(self):
            self.closed = True

    monkeypatch.setattr(bw, "ClientSession", ReplaySession)
    # the bot-style rebuild retires the old socket state before reconnecting
    manager.connected = False
    manager.ws = None
    manager._connected_url = None
    manager._setup_complete = False

    await manager._connect(SPOT)

    assert args in new_ws.sent
    assert args in manager._sent_subscriptions
    await drain(manager)

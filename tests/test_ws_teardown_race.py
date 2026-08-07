"""Teardown-race regression tests for the async websocket manager.

Covers the case where a socket's read loop outlives the socket: it must not drive
reconnects, must not touch the replacement socket, must always close its own session,
and must never leave an unretrieved task exception behind. Also covers the connect
serialization added alongside it (wrong-endpoint coalescing, shutdown races).
"""

import asyncio
import os
import sys
from types import SimpleNamespace

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymexc._async import base_websocket as bw
from pymexc._async.base_websocket import _AsyncWebSocketManager, SPOT


class FakeWS:
    """Async-iterable stand-in for aiohttp.ClientWebSocketResponse.

    `hang=True` keeps the read loop alive until close() is called, like a real socket.
    """

    def __init__(self, error: Exception | None = None, hang: bool = False, messages=()):
        self.closed = False
        self._error = error
        self._hang = hang
        self._messages = list(messages)
        self._closed_event = asyncio.Event()
        self.sent = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._messages:
            return self._messages.pop(0)
        if self._error is not None:
            raise self._error
        if self._hang:
            await self._closed_event.wait()
        raise StopAsyncIteration

    async def close(self):
        self.closed = True
        self._closed_event.set()

    async def send_json(self, message):
        self.sent.append(message)


class FakeSession:
    def __init__(self, *args, **kwargs):
        self.closed = False

    async def close(self):
        self.closed = True


def make_manager() -> _AsyncWebSocketManager:
    async def callback(_message):
        return None

    manager = _AsyncWebSocketManager(callback, "test", ping_interval=0)
    manager.endpoint = SPOT
    return manager


async def shutdown(manager: _AsyncWebSocketManager) -> None:
    """Stop the live read loop without letting it reconnect during teardown."""
    manager._closing = True
    manager.exited = True
    if manager._recv_task is not None:
        manager._recv_task.cancel()
        try:
            await manager._recv_task
        except asyncio.CancelledError:
            pass


def patch_connect(manager: _AsyncWebSocketManager, replace_socket: bool) -> list[str]:
    """Record _connect calls; optionally emulate a real reconnect swapping the socket."""
    calls: list[str] = []

    async def fake_connect(url):
        calls.append(url)
        if replace_socket:
            manager.ws = FakeWS()
            manager.session = FakeSession()

    manager._connect = fake_connect
    return calls


@pytest.mark.asyncio
async def test_stale_loop_does_not_reconnect_or_touch_new_socket():
    manager = make_manager()
    new_ws, new_session = FakeWS(), FakeSession()
    manager.ws, manager.session = new_ws, new_session
    calls = patch_connect(manager, replace_socket=False)

    old_ws = FakeWS(error=aiohttp.ClientConnectionError("Connector is closed"))
    old_session = FakeSession()
    await manager._loop_recv(old_ws, old_session)

    assert calls == []  # stale loop must not reconnect
    assert manager.ws is new_ws and not new_ws.closed and not new_session.closed
    assert old_session.closed  # but it must clean up after itself


@pytest.mark.asyncio
async def test_stale_loop_ignores_error_frame():
    manager = make_manager()
    manager.ws, manager.session = FakeWS(), FakeSession()
    calls = patch_connect(manager, replace_socket=False)
    errors = []
    manager._on_error = lambda err: errors.append(err)  # would blow up if awaited

    old_ws = FakeWS(messages=[SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=None)])
    old_session = FakeSession()
    await manager._loop_recv(old_ws, old_session)

    assert errors == [] and calls == []
    assert old_session.closed


@pytest.mark.asyncio
async def test_recv_task_exception_is_retrieved(caplog, monkeypatch):
    """The done-callback must retrieve (and log) the exception of a task nobody awaits,
    otherwise it resurfaces as 'Task exception was never retrieved'."""
    manager = make_manager()

    class OkSession(FakeSession):
        async def ws_connect(self, **kwargs):
            return FakeWS(hang=True)

    monkeypatch.setattr(bw, "ClientSession", OkSession)
    await manager._connect(SPOT)  # registers the done-callback in production code

    async def boom():
        raise RuntimeError("reconnect exploded")

    manager._on_close = boom
    session = manager.session

    with caplog.at_level("WARNING", logger=bw.__name__):
        await manager.ws.close()  # nobody awaits the recv task it kills
        await asyncio.sleep(0.05)
        assert manager._recv_task.done()

    assert "reconnect exploded" in caplog.text  # i.e. task.exception() was consumed
    assert session.closed


@pytest.mark.asyncio
async def test_current_socket_error_still_reconnects_once():
    manager = make_manager()
    ws = FakeWS(error=aiohttp.ClientConnectionError("connection reset"))
    session = FakeSession()
    manager.ws, manager.session = ws, session
    calls = patch_connect(manager, replace_socket=True)
    errors = []
    original_on_error = manager._on_error

    async def spy(err):
        errors.append(err)
        await original_on_error(err)

    manager._on_error = spy

    await manager._loop_recv(ws, session)

    assert len(errors) == 1  # the live socket's failure IS reported
    assert calls == [SPOT]  # legit disconnect still triggers exactly one reconnect
    assert session.closed


@pytest.mark.asyncio
async def test_session_closed_even_when_on_close_raises():
    manager = make_manager()
    ws, session = FakeWS(), FakeSession()
    manager.ws, manager.session = ws, session

    async def boom():
        raise RuntimeError("reconnect exploded")

    manager._on_close = boom

    with pytest.raises(RuntimeError):
        await manager._loop_recv(ws, session)

    assert session.closed


@pytest.mark.asyncio
async def test_cancel_during_handshake_leaks_nothing(monkeypatch):
    manager = make_manager()
    created: list[FakeSession] = []

    class HangingSession(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            created.append(self)

        async def ws_connect(self, **kwargs):
            await asyncio.Event().wait()

    monkeypatch.setattr(bw, "ClientSession", HangingSession)

    task = asyncio.create_task(manager._connect(SPOT))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(created) == 1 and created[0].closed
    assert manager.attempting_connection is False


@pytest.mark.asyncio
async def test_concurrent_connect_is_serialized(monkeypatch):
    manager = make_manager()
    handshakes = []
    sessions = []

    class SlowSession(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            sessions.append(self)

        async def ws_connect(self, **kwargs):
            handshakes.append(kwargs["url"])
            await asyncio.sleep(0.02)
            return FakeWS(hang=True)  # stays open: no spontaneous close/reconnect

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(SPOT), manager._connect(SPOT))

    # Same url: the second caller rides the first socket instead of opening its own.
    assert handshakes == [SPOT] and len(sessions) == 1
    assert manager.attempting_connection is False
    await shutdown(manager)


@pytest.mark.asyncio
async def test_connect_to_other_url_retires_the_old_socket(monkeypatch):
    """Spot carries the listenKey in the URL: a queued connect for a NEW endpoint must
    not be satisfied by the socket a previous connect just opened on the old one, and
    retiring that socket must not let its read loop run current-connection policy."""
    manager = make_manager()
    old_url, new_url = SPOT + "?listenKey=old", SPOT + "?listenKey=new"
    handshakes, sockets, sessions = [], [], []
    closes, errors = [], []

    async def spy_on_close():
        closes.append(True)

    async def spy_on_error(err):
        errors.append(err)

    manager._on_close = spy_on_close
    manager._on_error = spy_on_error

    retired_while_current = []

    class ProbedWS(FakeWS):
        async def close(self):
            retired_while_current.append(manager.ws is self)
            await super().close()

    class SlowSession(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            sessions.append(self)

        async def ws_connect(self, **kwargs):
            handshakes.append(kwargs["url"])
            await asyncio.sleep(0.02)
            sockets.append(ProbedWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(old_url), manager._connect(new_url))
    await asyncio.sleep(0.05)  # let the retired socket's read loop unwind

    assert handshakes == [old_url, new_url]
    assert manager._connected_url == new_url
    assert manager.ws is sockets[-1] and not manager.ws.closed
    assert sockets[0].closed and sessions[0].closed  # retired, and cleaned up by its loop
    assert closes == [] and errors == []  # stale loop ran no current-socket policy
    assert retired_while_current == [False]  # retired BEFORE the close, not after
    assert manager.is_connected()
    await shutdown(manager)


@pytest.mark.asyncio
async def test_close_all_during_handshake_is_not_resurrected(monkeypatch):
    """close_all() can only close what is published; a handshake still in flight - and
    any connect queued behind it - must not go live afterwards."""
    manager = make_manager()
    handshake_started, release = asyncio.Event(), asyncio.Event()
    sockets, sessions = [], []

    class SlowSession(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            sessions.append(self)

        async def ws_connect(self, **kwargs):
            handshake_started.set()
            await release.wait()
            sockets.append(FakeWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    first = asyncio.create_task(manager._connect(SPOT))
    await handshake_started.wait()
    queued = asyncio.create_task(manager._connect(SPOT))
    await asyncio.sleep(0)

    await manager.close_all()
    release.set()

    with pytest.raises(RuntimeError):
        await first
    with pytest.raises(RuntimeError):
        await queued

    assert sockets[0].closed and all(session.closed for session in sessions)
    assert manager.ws is None and manager.attempting_connection is False


@pytest.mark.asyncio
async def test_aexit_latches_closing():
    manager = make_manager()
    await manager.__aexit__(None, None, None)
    assert manager._closing is True


@pytest.mark.asyncio
async def test_error_flag_does_not_block_a_later_connect(monkeypatch):
    """`exited` is set by every _on_error via the sync base; only close_all/__aexit__
    may keep a connect out."""
    manager = make_manager()
    manager.exited = True

    class OkSession(FakeSession):
        async def ws_connect(self, **kwargs):
            return FakeWS(hang=True)

    monkeypatch.setattr(bw, "ClientSession", OkSession)

    await manager._connect(SPOT)

    assert manager.is_connected() and manager.ws is not None
    await shutdown(manager)


@pytest.mark.asyncio
async def test_live_error_frame_reports_the_exception():
    """aiohttp carries the exception in msg.data; the WSMessage itself is not raisable."""
    manager = make_manager()
    failure = aiohttp.ClientConnectionError("transport gone")
    ws = FakeWS(messages=[SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=failure)])
    session = FakeSession()
    manager.ws, manager.session = ws, session
    calls = patch_connect(manager, replace_socket=True)
    errors = []
    original_on_error = manager._on_error

    async def spy(err):
        errors.append(err)
        await original_on_error(err)

    manager._on_error = spy

    await manager._loop_recv(ws, session)

    assert errors == [failure]
    assert calls == [SPOT]  # and it still reconnects
    assert session.closed


@pytest.mark.asyncio
async def test_error_during_shutdown_does_not_reconnect(monkeypatch):
    """The ping loop can fail *because* teardown closed the socket; that must not turn
    into a reconnect attempt whose rejection escapes into close_all()/__aexit__."""
    manager = make_manager()
    sessions = []

    class OkSession(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            sessions.append(self)

        async def ws_connect(self, **kwargs):
            return FakeWS(hang=True)

    monkeypatch.setattr(bw, "ClientSession", OkSession)
    await manager._connect(SPOT)
    await manager.close_all()

    await manager._on_error(aiohttp.ClientConnectionError("Cannot write to closing transport"))

    assert len(sessions) == 1  # no reconnect handshake was attempted


@pytest.mark.asyncio
async def test_reentry_clears_both_latches(monkeypatch):
    manager = make_manager()

    class OkSession(FakeSession):
        async def ws_connect(self, **kwargs):
            return FakeWS(hang=True)

    monkeypatch.setattr(bw, "ClientSession", OkSession)

    await manager.__aexit__(None, None, None)
    await manager.__aenter__()

    assert manager._closing is False
    assert manager.exited is False  # else _on_close would never reconnect again
    await shutdown(manager)


@pytest.mark.asyncio
async def test_protocol_error_frame_reconnects():
    """aiohttp reports a protocol violation as an ERROR frame carrying WebSocketError."""
    manager = make_manager()
    failure = aiohttp.WebSocketError(aiohttp.WSCloseCode.PROTOCOL_ERROR, "bad frame")
    ws = FakeWS(messages=[SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=failure)])
    session = FakeSession()
    manager.ws, manager.session = ws, session
    calls = patch_connect(manager, replace_socket=True)

    await manager._loop_recv(ws, session)

    assert calls == [SPOT]
    assert session.closed


@pytest.mark.asyncio
async def test_queued_same_url_connect_does_not_redo_setup(monkeypatch):
    """A caller queued behind the lock finds the socket already live: re-running the
    login, the subscription replay and the ping-loop startup would duplicate all three
    on that socket (PR #10 review)."""
    manager = make_manager()
    manager.api_key, manager.api_secret = "k", "s"
    manager.subscriptions = [{"method": "SUBSCRIPTION", "params": ["a"]}]
    auths, pings, ping_loops = [], [], []
    sockets = []

    async def fake_auth():
        auths.append(True)

    async def fake_send_ping():
        pings.append(True)

    manager._auth = fake_auth
    manager._send_ping = fake_send_ping
    manager._start_ping_loop = lambda: ping_loops.append(True)

    class SlowSession(FakeSession):
        async def ws_connect(self, **kwargs):
            await asyncio.sleep(0.02)
            sockets.append(FakeWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(SPOT), manager._connect(SPOT))

    assert len(sockets) == 1
    assert auths == [True] and pings == [True] and ping_loops == [True]
    assert sockets[0].sent == manager.subscriptions  # each subscription sent once
    await shutdown(manager)


@pytest.mark.asyncio
async def test_queued_same_url_connect_delivers_a_late_subscription(monkeypatch):
    """The queued caller may have appended its subscription after the first caller's
    replay already read the list; early-returning must not drop it."""
    manager = make_manager()
    first = {"method": "SUBSCRIPTION", "params": ["a"]}
    late = {"method": "SUBSCRIPTION", "params": ["b"]}
    manager.subscriptions = [first]
    sockets = []

    class ProbedWS(FakeWS):
        async def send_json(self, message):
            await super().send_json(message)
            if message == first and late not in manager.subscriptions:
                # the queued caller appends while the first replay is in flight
                manager.subscriptions.append(late)

    class SlowSession(FakeSession):
        async def ws_connect(self, **kwargs):
            await asyncio.sleep(0.02)
            sockets.append(ProbedWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(SPOT), manager._connect(SPOT))

    assert len(sockets) == 1
    assert sockets[0].sent == [first, late]  # delivered, exactly once each
    await shutdown(manager)


@pytest.mark.asyncio
async def test_reconnect_replays_every_subscription(monkeypatch):
    """The per-socket 'already sent' record must not survive into the next socket."""
    manager = make_manager()
    manager.subscriptions = [{"method": "SUBSCRIPTION", "params": ["a"]}]
    sockets = []

    class OkSession(FakeSession):
        async def ws_connect(self, **kwargs):
            sockets.append(FakeWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", OkSession)

    await manager._connect(SPOT)
    manager.connected = False  # what a disconnect leaves behind
    await manager._connect(SPOT)

    assert len(sockets) == 2
    assert sockets[1].sent == manager.subscriptions  # full replay on the fresh socket
    await shutdown(manager)

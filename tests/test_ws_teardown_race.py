"""Teardown-race regression tests for the async websocket manager.

Covers the case where a socket's read loop outlives the socket: it must not drive
reconnects, must not touch the replacement socket, must always close its own session,
and must never leave an unretrieved task exception behind.
"""

import asyncio
import gc
import os
import sys
from types import SimpleNamespace

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymexc._async import base_websocket as bw
from pymexc._async.base_websocket import _AsyncWebSocketManager, SPOT


class FakeWS:
    """Async-iterable stand-in for aiohttp.ClientWebSocketResponse."""

    def __init__(self, error: Exception | None = None, hang: bool = False, messages=()):
        self.closed = False
        self._error = error
        self._hang = hang
        self._messages = list(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._messages:
            return self._messages.pop(0)
        if self._error is not None:
            raise self._error
        if self._hang:
            await asyncio.Event().wait()
        raise StopAsyncIteration

    async def close(self):
        self.closed = True


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

    unhandled = []
    asyncio.get_running_loop().set_exception_handler(lambda loop, ctx: unhandled.append(ctx))

    task = asyncio.create_task(manager._loop_recv(old_ws, old_session))
    task.add_done_callback(manager._log_recv_task_exception)
    await task
    del task
    gc.collect()
    await asyncio.sleep(0)

    assert calls == []  # stale loop must not reconnect
    assert manager.ws is new_ws and not new_ws.closed and not new_session.closed
    assert old_session.closed  # but it must clean up after itself
    assert unhandled == []


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
async def test_recv_task_exception_is_retrieved(caplog):
    """The done-callback must retrieve (and log) the exception of a task nobody awaits,
    otherwise it resurfaces as 'Task exception was never retrieved'."""
    manager = make_manager()
    ws, session = FakeWS(), FakeSession()
    manager.ws, manager.session = ws, session

    async def boom():
        raise RuntimeError("reconnect exploded")

    manager._on_close = boom

    with caplog.at_level("WARNING", logger=bw.__name__):
        task = asyncio.create_task(manager._loop_recv(ws, session))
        task.add_done_callback(manager._log_recv_task_exception)
        await asyncio.sleep(0.05)  # fire-and-forget: never awaited
        assert task.done()

    assert "reconnect exploded" in caplog.text  # i.e. task.exception() was consumed
    assert session.closed


@pytest.mark.asyncio
async def test_current_socket_error_still_reconnects_once():
    manager = make_manager()
    ws = FakeWS(error=aiohttp.ClientConnectionError("connection reset"))
    session = FakeSession()
    manager.ws, manager.session = ws, session
    calls = patch_connect(manager, replace_socket=True)

    await manager._loop_recv(ws, session)

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
    in_flight = 0
    peak = 0

    class SlowSession(FakeSession):
        async def ws_connect(self, **kwargs):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            in_flight -= 1
            return FakeWS(hang=True)  # stays open: no spontaneous close/reconnect

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(SPOT), manager._connect(SPOT))

    assert peak == 1
    assert manager.attempting_connection is False
    await shutdown(manager)


@pytest.mark.asyncio
async def test_shutdown_during_handshake_is_not_resurrected(monkeypatch):
    """close_all()/__aexit__ can only close what is published; a handshake that finishes
    afterwards must throw its socket away instead of going live behind their back."""
    manager = make_manager()
    sockets, sessions = [], []

    class ShutdownMidHandshake(FakeSession):
        def __init__(self, *args, **kwargs):
            super().__init__()
            sessions.append(self)

        async def ws_connect(self, **kwargs):
            manager.exited = True  # emulate close_all() landing mid-handshake
            sockets.append(FakeWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", ShutdownMidHandshake)

    with pytest.raises(RuntimeError):
        await manager._connect(SPOT)

    assert sockets[0].closed and sessions[0].closed
    assert getattr(manager, "ws", None) is None  # never published
    assert manager.attempting_connection is False


@pytest.mark.asyncio
async def test_connect_clears_a_stale_exit_flag(monkeypatch):
    """`exited` is set by every _on_error via the sync base; an explicit connect must
    not be permanently rejected because of a leftover flag."""
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
async def test_connect_to_other_url_does_not_ride_the_wrong_socket(monkeypatch):
    """Spot carries the listenKey in the URL: a queued connect for a NEW endpoint must
    not be satisfied by the socket a previous connect just opened on the old one."""
    manager = make_manager()
    old_url, new_url = SPOT + "?listenKey=old", SPOT + "?listenKey=new"
    handshakes = []
    sockets = []

    class SlowSession(FakeSession):
        async def ws_connect(self, **kwargs):
            handshakes.append(kwargs["url"])
            await asyncio.sleep(0.02)
            sockets.append(FakeWS(hang=True))
            return sockets[-1]

    monkeypatch.setattr(bw, "ClientSession", SlowSession)

    await asyncio.gather(manager._connect(old_url), manager._connect(new_url))

    assert handshakes == [old_url, new_url]
    assert manager._connected_url == new_url
    assert manager.ws is sockets[-1] and not manager.ws.closed
    assert sockets[0].closed  # the stale-endpoint socket was dropped
    await shutdown(manager)

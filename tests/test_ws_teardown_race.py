"""Teardown-race regression tests for the async websocket manager.

Covers the case where a socket's read loop outlives the socket: it must not drive
reconnects, must not touch the replacement socket, must always close its own session,
and must never leave an unretrieved task exception behind.
"""

import asyncio
import gc
import os
import sys

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymexc._async import base_websocket as bw
from pymexc._async.base_websocket import _AsyncWebSocketManager, SPOT


class FakeWS:
    """Async-iterable stand-in for aiohttp.ClientWebSocketResponse."""

    def __init__(self, error: Exception | None = None, hang: bool = False):
        self.closed = False
        self._error = error
        self._hang = hang

    def __aiter__(self):
        return self

    async def __anext__(self):
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
    manager._recv_task.cancel()

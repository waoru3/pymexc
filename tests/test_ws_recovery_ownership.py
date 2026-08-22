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
    assert not manager._ping_task.cancelled()
    manager._ping_task.cancel()

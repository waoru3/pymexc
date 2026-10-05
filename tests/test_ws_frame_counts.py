"""basis_fork TASK-438: per-channel count of the frames a socket delivers."""

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymexc._async import base_websocket as bw
from pymexc._async.base_websocket import _AsyncWebSocketManager
from pymexc.proto import PushDataV3ApiWrapper


@pytest.mark.asyncio
async def test_frames_are_counted_by_channel_and_still_delivered(caplog):
    delivered = []

    async def callback(message):
        delivered.append(message)

    manager = _AsyncWebSocketManager(callback, "test", ping_interval=0, restart_on_error=False)

    for channel in (
        "spot@private.deals.v3.api.pb",
        "spot@public.limit.depth.v3.api.pb@BTCUSDT@20",
        "spot@public.limit.depth.v3.api.pb@ETHUSDT@20",
    ):
        await manager._on_message(PushDataV3ApiWrapper(channel=channel).SerializeToString())
    await manager._on_message('{"id": 0, "code": 0, "msg": "PONG"}')
    await manager._on_message('{"channel": "push.personal.order", "data": {}}')

    assert len(delivered) == 5
    assert manager.frame_counts == {
        "spot@private.deals.v3.api.pb": 1,
        "spot@public.limit.depth.v3.api": 2,
        "json": 1,
        "push.personal.order": 1,
    }

    manager._frame_counts_logged_at -= bw.FRAME_COUNT_LOG_INTERVAL
    with caplog.at_level(logging.INFO, logger=bw.__name__):
        await manager._on_message('{"id": 0, "code": 0, "msg": "PONG"}')
        await manager._on_message('{"id": 0, "code": 0, "msg": "PONG"}')

    lines = [record.getMessage() for record in caplog.records if "frames received" in record.getMessage()]
    assert len(lines) == 1
    assert "'spot@private.deals.v3.api.pb': 1" in lines[0]
    assert "'json': 2" in lines[0]

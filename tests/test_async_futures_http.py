"""Unit tests for futures signing and transport (async client).

# uv run pytest tests/test_async_futures_http.py -v
"""

import hashlib
import hmac
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pymexc._async.base import MexcAPIError
from pymexc._async.futures import HTTP

API_KEY = "test_key"
API_SECRET = "test_secret"
TS = "1700000000000"
FROZEN_TIME = 1700000000.0


def hmac_hex(target: str) -> str:
    return hmac.new(API_SECRET.encode(), (API_KEY + TS + target).encode(), hashlib.sha256).hexdigest()


def make_response(ok=True, status=200, payload=None, text=""):
    response = MagicMock()
    response.ok = ok
    response.status_code = status
    response.text = text
    if payload is None:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = payload
    return response


@pytest.mark.asyncio
async def test_order_posts_json_body_to_api_mexc_com():
    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)
    response = make_response(payload={"success": True, "code": 0, "data": 1})
    with (
        patch.object(client.session, "request", new=AsyncMock(return_value=response)) as request_mock,
        patch("pymexc._async.base.time.time", return_value=FROZEN_TIME),
    ):
        result = await client.order(symbol="BTC_USDT", price=30000, vol=1, side=1, type=1, open_type=1, leverage=1)
    method, url = request_mock.call_args.args
    kwargs = request_mock.call_args.kwargs
    assert method == "POST"
    assert url == "https://api.mexc.com/api/v1/private/order/submit"
    assert kwargs["data"] == (
        '{"symbol":"BTC_USDT","price":30000,"vol":1,"side":1,"type":1,'
        '"openType":1,"leverage":1,"reduceOnly":false}'
    )
    assert kwargs["headers"]["Signature"] == hmac_hex(kwargs["data"])
    assert result == {"success": True, "code": 0, "data": 1}


@pytest.mark.asyncio
async def test_cancel_order_posts_list_body():
    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)
    response = make_response(payload={"success": True, "code": 0})
    with (
        patch.object(client.session, "request", new=AsyncMock(return_value=response)) as request_mock,
        patch("pymexc._async.base.time.time", return_value=FROZEN_TIME),
    ):
        await client.cancel_order(123456789)
    kwargs = request_mock.call_args.kwargs
    assert request_mock.call_args.args[1] == "https://api.mexc.com/api/v1/private/order/cancel"
    assert kwargs["data"] == "[123456789]"
    assert kwargs["headers"]["Signature"] == hmac_hex("[123456789]")


@pytest.mark.asyncio
async def test_get_keeps_query_params_and_signs_sorted():
    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)
    response = make_response(payload={"success": True, "code": 0, "data": []})
    with (
        patch.object(client.session, "request", new=AsyncMock(return_value=response)) as request_mock,
        patch("pymexc._async.base.time.time", return_value=FROZEN_TIME),
    ):
        await client.open_orders(symbol="BTC_USDT")
    kwargs = request_mock.call_args.kwargs
    assert kwargs["params"] == {"symbol": "BTC_USDT", "page_num": 1, "page_size": 20}
    assert kwargs["headers"]["Signature"] == hmac_hex("page_num=1&page_size=20&symbol=BTC_USDT")


@pytest.mark.asyncio
async def test_http_error_raises():
    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)
    response = make_response(ok=False, status=403, payload=None, text="<HTML>Access Denied</HTML>")
    with (
        patch.object(client.session, "request", new=AsyncMock(return_value=response)),
        patch("pymexc._async.base.time.time", return_value=FROZEN_TIME),
    ):
        with pytest.raises(MexcAPIError, match="HTTP 403"):
            await client.cancel_order(1)

"""Unit tests for futures signing and transport (sync client).

# uv run pytest tests/test_futures_http.py -v
"""

import hashlib
import hmac
from unittest.mock import MagicMock, patch

import pytest

from pymexc.base import MexcAPIError, futures_sign_request
from pymexc.futures import HTTP

API_KEY = "test_key"
API_SECRET = "test_secret"
TS = "1700000000000"
FROZEN_TIME = 1700000000.0  # int(FROZEN_TIME * 1000) == TS


def hmac_hex(target: str) -> str:
    return hmac.new(API_SECRET.encode(), (API_KEY + TS + target).encode(), hashlib.sha256).hexdigest()


class TestFuturesSignRequest:
    def test_post_dict_payload_signs_compact_json_body(self):
        payload = {"symbol": "BTC_USDT", "price": 30000, "vol": 1, "side": 1, "type": 1, "openType": 1, "leverage": 1}
        signature, body, params = futures_sign_request(API_KEY, API_SECRET, TS, "POST", payload)
        assert body == '{"symbol":"BTC_USDT","price":30000,"vol":1,"side":1,"type":1,"openType":1,"leverage":1}'
        assert params is None
        assert signature == "98fa84fee14e3f3654335676706fb6606740bf4f36e06a806c29893e6e8685b2"

    def test_post_list_payload(self):
        # cancel_order sends a bare list of order ids
        signature, body, params = futures_sign_request(API_KEY, API_SECRET, TS, "POST", [123456789])
        assert body == "[123456789]"
        assert params is None
        assert signature == "80d8e51224377143717d45628c9fee414051abdaa11c1baa0e377d91bf0901a3"

    @pytest.mark.parametrize("payload", [None, {}, []])
    def test_post_empty_payload_signs_empty_body(self, payload):
        # {} / [] / None all mean "no parameters"; integration guide: "if
        # there are no parameters, use an empty string"
        signature, body, params = futures_sign_request(API_KEY, API_SECRET, TS, "POST", payload)
        assert body == ""
        assert params is None
        assert signature == "88000ed348e8e57dfb4ae8ad93386a021b6f2f41ea839188723b7762d8c01e12"

    def test_get_signs_sorted_kv_join(self):
        params = {"symbol": "BTC_USDT", "page_num": 1, "page_size": 20}
        signature, body, out_params = futures_sign_request(API_KEY, API_SECRET, TS, "GET", params)
        assert body is None
        assert out_params == params
        # target: "page_num=1&page_size=20&symbol=BTC_USDT"
        assert signature == "ead06de1cc6ee1a0311f65949fd89904035a20e7b0e13df74422c8104c48f055"

    def test_get_none_payload_signs_empty_target(self):
        signature, body, out_params = futures_sign_request(API_KEY, API_SECRET, TS, "GET", None)
        assert body is None
        assert out_params == {}
        assert signature == hmac_hex("")


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


class TestFuturesCall:
    def setup_method(self):
        self.client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)

    def _call(self, response, fn, *args, **kwargs):
        with (
            patch.object(self.client.session, "request", return_value=response) as request_mock,
            patch("pymexc.base.time.time", return_value=FROZEN_TIME),
        ):
            result = fn(*args, **kwargs)
        return result, request_mock

    def test_order_posts_json_body_to_api_mexc_com(self):
        response = make_response(payload={"success": True, "code": 0, "data": 1})
        result, request_mock = self._call(
            response, self.client.order,
            symbol="BTC_USDT", price=30000, vol=1, side=1, type=1, open_type=1, leverage=1,
        )
        method, url = request_mock.call_args.args
        kwargs = request_mock.call_args.kwargs
        assert method == "POST"
        assert url == "https://api.mexc.com/api/v1/private/order/submit"
        # None-valued optionals stripped; reduce_only=False survives; insertion order kept
        assert kwargs["data"] == (
            '{"symbol":"BTC_USDT","price":30000,"vol":1,"side":1,"type":1,'
            '"openType":1,"leverage":1,"reduceOnly":false}'
        )
        assert "params" not in kwargs
        # byte-identity: signature recomputed over the exact sent body
        assert kwargs["headers"]["Signature"] == hmac_hex(kwargs["data"])
        assert kwargs["headers"]["Request-Time"] == TS
        # ApiKey / Content-Type ride on the session (set in __init__)
        assert self.client.session.headers["ApiKey"] == API_KEY
        assert self.client.session.headers["Content-Type"] == "application/json"
        assert result == {"success": True, "code": 0, "data": 1}

    def test_cancel_order_posts_list_body(self):
        response = make_response(payload={"success": True, "code": 0})
        _, request_mock = self._call(response, self.client.cancel_order, 123456789)
        method, url = request_mock.call_args.args
        kwargs = request_mock.call_args.kwargs
        assert method == "POST"
        assert url == "https://api.mexc.com/api/v1/private/order/cancel"
        assert kwargs["data"] == "[123456789]"
        assert kwargs["headers"]["Signature"] == hmac_hex("[123456789]")

    def test_change_leverage_posts_json_body(self):
        response = make_response(payload={"success": True, "code": 0})
        _, request_mock = self._call(response, self.client.change_leverage, position_id=1, leverage=20)
        kwargs = request_mock.call_args.kwargs
        assert request_mock.call_args.args[1] == "https://api.mexc.com/api/v1/private/position/change_leverage"
        assert kwargs["data"] == '{"positionId":1,"leverage":20}'
        assert kwargs["headers"]["Signature"] == hmac_hex(kwargs["data"])

    def test_get_keeps_query_params_and_signs_sorted(self):
        response = make_response(payload={"success": True, "code": 0, "data": []})
        _, request_mock = self._call(response, self.client.open_orders, symbol="BTC_USDT")
        method, url = request_mock.call_args.args
        kwargs = request_mock.call_args.kwargs
        assert method == "GET"
        assert url == "https://api.mexc.com/api/v1/private/order/list/open_orders/BTC_USDT"
        assert kwargs["params"] == {"symbol": "BTC_USDT", "page_num": 1, "page_size": 20}
        assert "data" not in kwargs
        assert kwargs["headers"]["Signature"] == hmac_hex("page_num=1&page_size=20&symbol=BTC_USDT")

    def test_http_error_with_json_body_raises(self):
        response = make_response(ok=False, status=400, payload={"success": False, "code": 602, "message": "Confirming signature failed"})
        with pytest.raises(MexcAPIError, match="code=602"):
            self._call(response, self.client.cancel_order, 1)

    def test_http_error_with_html_body_raises(self):
        # contract.mexc.com WAF block shape: 403 + Akamai HTML page
        response = make_response(ok=False, status=403, payload=None, text="<HTML><HEAD>Access Denied</HEAD></HTML>")
        with pytest.raises(MexcAPIError, match="HTTP 403"):
            self._call(response, self.client.order, symbol="BTC_USDT", price=1, vol=1, side=1, type=1, open_type=1)

    def test_post_with_all_none_params_sends_no_body_signs_empty(self):
        # cancel_all() with default symbol=None strips to {} - must behave
        # like a no-parameter POST: no body, signature over empty string
        response = make_response(payload={"success": True, "code": 0})
        _, request_mock = self._call(response, self.client.cancel_all)
        kwargs = request_mock.call_args.kwargs
        assert "data" not in kwargs
        assert "params" not in kwargs
        assert kwargs["headers"]["Signature"] == hmac_hex("")

    def test_unauthenticated_public_get_sends_no_signature(self):
        client = HTTP(ignore_ad=True)
        response = make_response(payload={"success": True, "code": 0, "data": {}})
        with (
            patch.object(client.session, "request", return_value=response) as request_mock,
            patch("pymexc.base.time.time", return_value=FROZEN_TIME),
        ):
            client.ticker(symbol="BTC_USDT")
        assert request_mock.call_args.kwargs.get("headers") is None
        assert request_mock.call_args.kwargs["params"] == {"symbol": "BTC_USDT"}

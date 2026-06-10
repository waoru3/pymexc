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

    def test_post_none_payload_signs_empty_body(self):
        # change_risk_level posts without payload; integration guide: "if there
        # are no parameters, use an empty string" (method unused by the bot)
        signature, body, params = futures_sign_request(API_KEY, API_SECRET, TS, "POST", None)
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

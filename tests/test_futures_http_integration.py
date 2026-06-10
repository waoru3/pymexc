"""Live validation of futures trading endpoints (basis_fork backlog TASK-19).

Places a far-from-market limit order (~$3 margin, cannot fill) on BTC_USDT,
verifies it is visible via private GET, then cancels it. Also proves private
GET endpoints work on api.mexc.com after the base URL flip.

Opt-in only:  uv run pytest tests/test_futures_http_integration.py -m integration -v -s
Credentials:  .env file (API_KEY / API_SECRET) or environment variables.
"""

import os
import time

import pytest
from dotenv import dotenv_values

from pymexc.futures import HTTP

env = {**dotenv_values(".env"), **os.environ}
API_KEY = env.get("API_KEY")
API_SECRET = env.get("API_SECRET")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not API_KEY or not API_SECRET, reason="API_KEY / API_SECRET not configured"),
]


def test_futures_order_lifecycle():
    client = HTTP(api_key=API_KEY, api_secret=API_SECRET, ignore_ad=True)

    # 1. Public read on api.mexc.com
    detail = client.detail(symbol="BTC_USDT")
    assert detail["success"] is True
    assert detail["data"]["apiAllowed"] is True

    # 2. Private GETs on api.mexc.com (spec decision 2 risk retirement)
    assets = client.assets()
    assert assets["success"] is True, f"private GET assets failed on api.mexc.com: {assets}"
    open_before = client.open_orders(symbol="BTC_USDT")
    assert open_before["success"] is True, f"private GET open_orders failed on api.mexc.com: {open_before}"

    # 3. Far-from-market limit buy: 50% of market price cannot fill
    ticker = client.ticker(symbol="BTC_USDT")
    assert ticker["success"] is True
    price = round(ticker["data"]["lastPrice"] * 0.5)
    order = client.order(symbol="BTC_USDT", price=price, vol=1, side=1, type=1, open_type=1, leverage=1)
    assert order["success"] is True, f"order failed: {order}"
    # Live-verified /order/submit returned data as the bare order id
    # (docs/MEXC/futures_api_trading.md, 2026-06-10); the newer official
    # /order/create documents {"orderId": ..., "ts": ...}. Extract robustly so
    # a schema change cannot strand a live order.
    data = order["data"]
    order_id = data["orderId"] if isinstance(data, dict) else data
    print(f"\nplaced order {order_id} at {price}")

    try:
        # 4. Order visible via private GET (TASK-19 AC#2)
        time.sleep(2)
        open_after = client.open_orders(symbol="BTC_USDT")
        assert open_after["success"] is True
        # str-normalize: docs type orderId as string, live capture returned int
        assert str(order_id) in [str(o["orderId"]) for o in open_after["data"]], (
            f"order {order_id} not in open orders: {open_after['data']}"
        )
    finally:
        # 5. Cancel ALWAYS runs - never leave a live order behind
        cancel = client.cancel_order(order_id)
        print(f"cancel response: {cancel}")
        assert cancel["success"] is True, f"CANCEL FAILED - manually cancel order {order_id}: {cancel}"
        assert cancel["data"][0]["errorCode"] == 0

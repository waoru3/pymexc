"""Regression tests for the spot HTTP error branch: non-JSON (WAF HTML) bodies
must surface the HTTP status and a truncated body, never a JSON parser error.
Shape observed live 2026-09-01 (TASK-285): 403 + Akamai "Access Denied" HTML."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pymexc.base import MexcAPIError as SyncMexcAPIError
from pymexc.base import _SpotHTTP as SyncSpotHTTP
from pymexc._async.base import MexcAPIError as AsyncMexcAPIError
from pymexc._async.base import _SpotHTTP as AsyncSpotHTTP

WAF_HTML = "<HTML><HEAD>\n<TITLE>Access Denied</TITLE>\n</HEAD></HTML>"


def make_response(ok=True, status=200, payload=None, text=""):
    response = MagicMock()
    response.ok = ok
    response.status_code = status
    response.text = text
    if payload is None:
        response.json.side_effect = json.JSONDecodeError("unexpected character", text or "x", 0)
    else:
        response.json.return_value = payload
    return response


class TestSyncSpotErrorBranch:
    def setup_method(self):
        self.client = SyncSpotHTTP(api_key="k", api_secret="s")

    def _call(self, response):
        with patch.object(self.client.session, "request", return_value=response):
            return self.client.call("GET", "/api/v3/exchangeInfo", auth=False)

    def test_html_body_surfaces_status_and_truncated_body(self):
        response = make_response(ok=False, status=403, payload=None, text=WAF_HTML)
        with pytest.raises(SyncMexcAPIError, match="HTTP 403") as excinfo:
            self._call(response)
        assert "Access Denied" in str(excinfo.value)

    def test_html_body_is_truncated_to_200_chars(self):
        long_text = "<HTML>" + "x" * 300
        response = make_response(ok=False, status=403, payload=None, text=long_text)
        with pytest.raises(SyncMexcAPIError) as excinfo:
            self._call(response)
        assert long_text[:200] in str(excinfo.value)
        assert long_text[:201] not in str(excinfo.value)

    def test_json_error_body_keeps_code_format(self):
        response = make_response(ok=False, status=400, payload={"code": 700002, "msg": "Signature invalid"})
        with pytest.raises(SyncMexcAPIError, match="code=700002"):
            self._call(response)

    def test_json_error_with_message_key_is_surfaced(self):
        response = make_response(ok=False, status=400, payload={"code": 602, "message": "Confirming signature failed"})
        with pytest.raises(SyncMexcAPIError, match="Confirming signature failed"):
            self._call(response)


class TestAsyncSpotErrorBranch:
    def setup_method(self):
        self.client = AsyncSpotHTTP(api_key="k", api_secret="s")

    async def _call(self, response):
        with patch.object(self.client.session, "request", new=AsyncMock(return_value=response)):
            return await self.client.call("GET", "/api/v3/exchangeInfo", auth=False)

    @pytest.mark.asyncio
    async def test_html_body_surfaces_status_not_json_error(self):
        response = make_response(ok=False, status=403, payload=None, text=WAF_HTML)
        with pytest.raises(AsyncMexcAPIError, match="HTTP 403") as excinfo:
            await self._call(response)
        assert "Access Denied" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_json_error_body_keeps_code_format(self):
        response = make_response(ok=False, status=400, payload={"code": 700002, "msg": "Signature invalid"})
        with pytest.raises(AsyncMexcAPIError, match="code=700002"):
            await self._call(response)

    @pytest.mark.asyncio
    async def test_html_body_is_truncated_to_200_chars(self):
        long_text = "<HTML>" + "x" * 300
        response = make_response(ok=False, status=403, payload=None, text=long_text)
        with pytest.raises(AsyncMexcAPIError) as excinfo:
            await self._call(response)
        assert long_text[:200] in str(excinfo.value)
        assert long_text[:201] not in str(excinfo.value)

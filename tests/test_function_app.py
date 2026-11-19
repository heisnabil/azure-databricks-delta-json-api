"""Unit tests for the Azure Function HTTP endpoints.

Tests cover request routing, authentication flow, pagination parameter
validation, error response formatting, and correlation IDs.
"""
from __future__ import annotations

import base64
import json
from unittest.mock import patch

import azure.functions as func
import pytest

from function_app import _parse_page_size

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    key = base64.urlsafe_b64encode(b"t" * 32).decode("ascii").rstrip("=")
    monkeypatch.setenv("API_KEY_ACTIVE", "test-key-123")
    monkeypatch.setenv("API_KEY_PREVIOUS", "")
    monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", key)
    monkeypatch.setenv("CURSOR_TTL_SECONDS", "86400")
    monkeypatch.setenv("MAX_PAGE_SIZE", "1000")
    monkeypatch.setenv("MAX_RESPONSE_BYTES", "4194304")


def _build_request(
    url: str = "/api/v1/records",
    method: str = "GET",
    headers: dict | None = None,
    params: dict | None = None,
) -> func.HttpRequest:
    """Build a mock Azure Functions HTTP request."""
    return func.HttpRequest(
        method=method,
        url=url,
        headers=headers or {},
        params=params or {},
        body=b"",
    )


# ---------------------------------------------------------------------------
# Page size validation tests
# ---------------------------------------------------------------------------

class TestParsePageSize:
    def test_default(self):
        assert _parse_page_size(None) == 250
        assert _parse_page_size("") == 250

    def test_valid_value(self):
        assert _parse_page_size("100") == 100
        assert _parse_page_size("1") == 1
        assert _parse_page_size("1000") == 1000

    def test_invalid_non_numeric(self):
        with pytest.raises(ValueError, match="positive integer"):
            _parse_page_size("abc")

    def test_invalid_zero(self):
        with pytest.raises(ValueError):
            _parse_page_size("0")

    def test_invalid_too_large(self):
        with pytest.raises(ValueError):
            _parse_page_size("1001")

    def test_invalid_negative(self):
        with pytest.raises(ValueError):
            _parse_page_size("-1")


# ---------------------------------------------------------------------------
# Health endpoint tests
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_health_returns_ok(self):
        from function_app import health
        req = _build_request(url="/api/v1/health")
        resp = health(req)
        assert resp.status_code == 200
        body = json.loads(resp.get_body())
        assert body["success"] is True
        assert body["status"] == "ok"


# ---------------------------------------------------------------------------
# Records endpoint authentication tests
# ---------------------------------------------------------------------------

class TestRecordsAuth:
    def test_missing_api_key_returns_401(self):
        from function_app import records
        req = _build_request(headers={})
        resp = records(req)
        assert resp.status_code == 401
        body = json.loads(resp.get_body())
        assert body["success"] is False
        assert body["error"]["code"] == "UNAUTHORIZED"

    def test_invalid_api_key_returns_401(self):
        from function_app import records
        req = _build_request(headers={"x-api-key": "wrong-key"})
        resp = records(req)
        assert resp.status_code == 401

    @patch("function_app.fetch_page")
    def test_valid_api_key_proceeds(self, mock_fetch):
        mock_fetch.return_value = {
            "success": True, "page": 1, "page_size": 10,
            "has_more": False, "data_as_of": "2025-01-01T00:00:00Z",
            "source_table_version": 1, "data": [], "next_cursor": None,
        }
        from function_app import records
        req = _build_request(headers={"x-api-key": "test-key-123"})
        resp = records(req)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Records endpoint error handling tests
# ---------------------------------------------------------------------------

class TestRecordsErrors:
    def test_invalid_page_size_returns_400(self):
        from function_app import records
        req = _build_request(
            headers={"x-api-key": "test-key-123"},
            params={"page_size": "abc"},
        )
        resp = records(req)
        assert resp.status_code == 400
        body = json.loads(resp.get_body())
        assert body["error"]["code"] == "INVALID_PAGE_SIZE"

    @patch("function_app.fetch_page")
    def test_backend_error_returns_503(self, mock_fetch):
        mock_fetch.side_effect = Exception("DB error")
        from function_app import records
        req = _build_request(headers={"x-api-key": "test-key-123"})
        resp = records(req)
        assert resp.status_code == 503
        body = json.loads(resp.get_body())
        assert body["error"]["code"] == "DATA_BACKEND_UNAVAILABLE"
        # Must NOT contain the actual error message
        assert "DB error" not in resp.get_body().decode()

    @patch("function_app.fetch_page")
    def test_source_stale_returns_503(self, mock_fetch):
        from databricks_store import SourceTooOld
        mock_fetch.side_effect = SourceTooOld("stale")
        from function_app import records
        req = _build_request(headers={"x-api-key": "test-key-123"})
        resp = records(req)
        assert resp.status_code == 503
        body = json.loads(resp.get_body())
        assert body["error"]["code"] == "SOURCE_STALE"

    @patch("function_app.fetch_page")
    def test_config_error_returns_500(self, mock_fetch):
        from databricks_store import BackendConfigurationError
        mock_fetch.side_effect = BackendConfigurationError("bad config")
        from function_app import records
        req = _build_request(headers={"x-api-key": "test-key-123"})
        resp = records(req)
        assert resp.status_code == 500
        body = json.loads(resp.get_body())
        assert body["error"]["code"] == "SERVER_CONFIGURATION_ERROR"
        # Must NOT contain the actual error details
        assert "bad config" not in resp.get_body().decode()


# ---------------------------------------------------------------------------
# Correlation ID tests
# ---------------------------------------------------------------------------

class TestCorrelationId:
    @patch("function_app.fetch_page")
    def test_response_includes_correlation_id(self, mock_fetch):
        mock_fetch.return_value = {
            "success": True, "page": 1, "page_size": 10,
            "has_more": False, "data_as_of": "x",
            "source_table_version": 1, "data": [], "next_cursor": None,
        }
        from function_app import records
        req = _build_request(headers={"x-api-key": "test-key-123"})
        resp = records(req)
        assert "X-Correlation-ID" in resp.headers

    def test_error_response_includes_correlation_id(self):
        from function_app import records
        req = _build_request(headers={})
        resp = records(req)
        body = json.loads(resp.get_body())
        assert "correlation_id" in body["error"]


# ---------------------------------------------------------------------------
# Security headers tests
# ---------------------------------------------------------------------------

class TestSecurityHeaders:
    def test_no_store_header(self):
        from function_app import health
        resp = health(_build_request(url="/api/v1/health"))
        assert resp.headers.get("Cache-Control") == "no-store"
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"

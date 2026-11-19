"""Azure Functions Python v2 — HTTP entry point.

GET /api/v1/health  — liveness check (no auth)
GET /api/v1/records — paginated Delta table data (API key required)
"""
from __future__ import annotations

import json
import logging
import os
import uuid

import azure.functions as func

from databricks_store import (
    BackendConfigurationError,
    PageTooLarge,
    SnapshotExpired,
    SourceTooOld,
    fetch_page,
)
from security import ExpiredCursor, InvalidCursor, api_key_is_valid, decode_cursor

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
LOGGER = logging.getLogger("databricks_delta_api")

DEFAULT_PAGE_SIZE = 250
_DEFAULT_MAX_PAGE_SIZE = 1000
_DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024  # 4 MiB


def _get_max_page_size() -> int:
    """Read MAX_PAGE_SIZE from environment, defaulting to 1000."""
    raw = os.getenv("MAX_PAGE_SIZE", "")
    if raw and raw.isdigit():
        return max(1, int(raw))
    return _DEFAULT_MAX_PAGE_SIZE


def _response(
    body: dict | str,
    status: int = 200,
    *,
    correlation_id: str | None = None,
) -> func.HttpResponse:
    """Build a JSON HTTP response with security headers."""
    if isinstance(body, dict):
        serialized = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    else:
        serialized = body  # already serialized

    headers = {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
    }
    if correlation_id:
        headers["X-Correlation-ID"] = correlation_id

    return func.HttpResponse(
        body=serialized,
        status_code=status,
        mimetype="application/json",
        headers=headers,
    )


def _error(
    code: str,
    message: str,
    status: int,
    *,
    correlation_id: str | None = None,
) -> func.HttpResponse:
    """Build a standardized error response."""
    body: dict = {"success": False, "error": {"code": code, "message": message}}
    if correlation_id:
        body["error"]["correlation_id"] = correlation_id
    return _response(body, status, correlation_id=correlation_id)


def _parse_page_size(raw: str | None) -> int:
    """Parse and validate the page_size query parameter."""
    max_page_size = _get_max_page_size()
    if raw is None or raw == "":
        return DEFAULT_PAGE_SIZE
    if not raw.isdigit():
        raise ValueError("page_size must be a positive integer")
    value = int(raw)
    if value < 1 or value > max_page_size:
        raise ValueError(f"page_size must be between 1 and {max_page_size}")
    return value


@app.route(route="v1/health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Liveness check. Does NOT probe Databricks or wake the warehouse."""
    return _response({"success": True, "status": "ok"})


@app.route(route="v1/records", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def records(req: func.HttpRequest) -> func.HttpResponse:
    """Return one page of records from the configured Delta table."""
    correlation_id = str(uuid.uuid4())

    try:
        if not api_key_is_valid(req.headers.get("x-api-key")):
            return _error(
                "UNAUTHORIZED",
                "A valid x-api-key header is required.",
                401,
                correlation_id=correlation_id,
            )
    except RuntimeError:
        LOGGER.exception("API authentication configuration error [%s]", correlation_id)
        return _error(
            "SERVER_CONFIGURATION_ERROR",
            "The API is not correctly configured. Contact the API owner.",
            500,
            correlation_id=correlation_id,
        )

    cursor_token = req.params.get("cursor")
    try:
        page_size = _parse_page_size(req.params.get("page_size"))
    except ValueError as exc:
        return _error("INVALID_PAGE_SIZE", str(exc), 400, correlation_id=correlation_id)

    state = None
    if cursor_token:
        try:
            state = decode_cursor(cursor_token)
        except ExpiredCursor:
            return _error(
                "CURSOR_EXPIRED",
                "The pagination cursor expired. Start a new pagination session.",
                410,
                correlation_id=correlation_id,
            )
        except InvalidCursor:
            return _error(
                "INVALID_CURSOR",
                "The pagination cursor is invalid.",
                400,
                correlation_id=correlation_id,
            )

    try:
        body = fetch_page(state, page_size)

        max_bytes = int(os.getenv("MAX_RESPONSE_BYTES", str(_DEFAULT_MAX_RESPONSE_BYTES)))
        serialized = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        encoded_size = len(serialized.encode("utf-8"))

        if encoded_size > max_bytes:
            return _error(
                "PAGE_TOO_LARGE",
                "This page exceeds the configured response-size limit. "
                "Retry with a smaller page_size; when continuing, reuse the same cursor.",
                413,
                correlation_id=correlation_id,
            )

        return _response(serialized, correlation_id=correlation_id)

    except SourceTooOld:
        LOGGER.warning("API refused to serve stale source data [%s]", correlation_id)
        return _error(
            "SOURCE_STALE",
            "The latest committed source data is older than the configured freshness limit.",
            503,
            correlation_id=correlation_id,
        )
    except SnapshotExpired:
        LOGGER.warning("Delta snapshot expired during pagination [%s]", correlation_id)
        return _error(
            "SNAPSHOT_EXPIRED",
            "The data snapshot for this pagination session is no longer available. "
            "Start a new pagination session without a cursor.",
            410,
            correlation_id=correlation_id,
        )
    except PageTooLarge:
        return _error(
            "PAGE_TOO_LARGE",
            "Reduce page_size and retry with the same cursor.",
            413,
            correlation_id=correlation_id,
        )
    except BackendConfigurationError:
        LOGGER.exception("API configuration error [%s]", correlation_id)
        return _error(
            "SERVER_CONFIGURATION_ERROR",
            "The API is not correctly configured. Contact the API owner.",
            500,
            correlation_id=correlation_id,
        )
    except Exception:
        # Never surface workspace hostnames, SQL, table names, or secrets in the response.
        LOGGER.exception("Databricks backend request failed [%s]", correlation_id)
        return _error(
            "DATA_BACKEND_UNAVAILABLE",
            "The data service is temporarily unavailable. Retry later.",
            503,
            correlation_id=correlation_id,
        )

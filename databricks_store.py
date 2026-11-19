"""Read-only, snapshot-consistent paging from one configured Delta table.

Connects to a Databricks SQL Warehouse via OAuth M2M, reads a Delta table
using time-travel (VERSION AS OF) for snapshot consistency, and returns
keyset-paginated pages with correct JSON serialization for all Databricks types.
"""
from __future__ import annotations

import base64
import logging
import os
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

from security import encode_cursor, new_cursor_state

LOGGER = logging.getLogger(__name__)

# Only alphanumeric identifiers are accepted for table names and key columns,
# preventing SQL injection via environment variable misconfiguration.
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# FLOAT/DOUBLE are excluded: their ordering is non-deterministic across pages.
# BINARY and complex types (ARRAY, STRUCT, MAP) are excluded: not scalar.
SAFE_KEY_TYPE_RE = re.compile(
    r"^(STRING|DATE|TIMESTAMP|TIMESTAMP_NTZ|TINYINT|SMALLINT|INT|INTEGER|BIGINT|"
    r"DECIMAL\(\d{1,2},\s*\d{1,2}\)|VARCHAR\(\d+\)|CHAR\(\d+\))$",
    re.IGNORECASE,
)

DEFAULT_QUERY_TIMEOUT_SECONDS = 90


class SourceTooOld(RuntimeError):
    """The latest Delta commit is older than the configured freshness limit."""


class PageTooLarge(RuntimeError):
    """The result page exceeds the configured maximum response size."""


class BackendConfigurationError(RuntimeError):
    """A required backend setting is missing or invalid."""


class SnapshotExpired(RuntimeError):
    """The pinned Delta version is no longer available (vacuumed)."""


def _required_env(name: str) -> str:
    """Read a required environment variable; raise on missing/empty."""
    value = os.getenv(name, "").strip()
    if not value:
        raise BackendConfigurationError(f"Missing required setting: {name}")
    return value


def _ident(value: str) -> str:
    """Validate and backtick-quote a SQL identifier."""
    if not IDENTIFIER_RE.fullmatch(value):
        raise BackendConfigurationError(
            "Table and key identifiers must use only letters, digits, and underscores"
        )
    return f"`{value}`"


def get_table_config() -> tuple[str, str, list[str]]:
    """Parse and validate the TABLE_FQN and ORDER_KEY_COLUMNS settings."""
    parts = [p.strip() for p in _required_env("TABLE_FQN").split(".")]
    if len(parts) != 3:
        raise BackendConfigurationError("TABLE_FQN must be catalog.schema.table")
    table_sql = ".".join(_ident(p) for p in parts)
    key_columns = [
        x.strip() for x in _required_env("ORDER_KEY_COLUMNS").split(",") if x.strip()
    ]
    if not key_columns:
        raise BackendConfigurationError("ORDER_KEY_COLUMNS is required")
    for key in key_columns:
        _ident(key)
    return table_sql, ".".join(parts), key_columns


def _connect():
    """Create a Databricks SQL connection using OAuth M2M (service principal)."""
    host = _required_env("DATABRICKS_SERVER_HOSTNAME").removeprefix("https://").rstrip("/")
    http_path = _required_env("DATABRICKS_HTTP_PATH")
    client_id = _required_env("DATABRICKS_CLIENT_ID")
    client_secret = _required_env("DATABRICKS_CLIENT_SECRET")

    config = Config(
        host=f"https://{host}",
        client_id=client_id,
        client_secret=client_secret,
    )
    credential_provider = oauth_service_principal(config)

    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        credentials_provider=credential_provider,
        use_cloud_fetch=False,
    )


def _utc_iso(value: Any) -> str:
    """Normalize a datetime or ISO string to a UTC ISO 8601 string."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        else:
            value = value.astimezone(UTC)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))  # noqa: FURB162
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            else:
                parsed = parsed.astimezone(UTC)
            return parsed.isoformat().replace("+00:00", "Z")
        except ValueError:
            return value
    raise RuntimeError("Unexpected timestamp type returned by Databricks")


def _inspect_key_schema(
    cursor: Any, table_sql: str, configured_keys: list[str]
) -> list[str]:
    """Confirm configured ordering keys exist in the table and have supported types."""
    cursor.execute(f"DESCRIBE TABLE {table_sql}")
    schema: dict[str, str] = {}
    for row in cursor.fetchall():
        if not row or len(row) < 2:
            continue
        col_name = str(row[0]).strip()
        data_type = str(row[1]).strip().upper()
        if not col_name or col_name.startswith("#") or not data_type or data_type == "NULL":
            continue
        schema[col_name.lower()] = data_type

    key_types: list[str] = []
    for requested in configured_keys:
        actual_type = schema.get(requested.lower())
        if actual_type is None:
            raise BackendConfigurationError(
                f"Configured ordering column was not found in table schema: {requested}"
            )
        if not SAFE_KEY_TYPE_RE.fullmatch(actual_type):
            raise BackendConfigurationError(
                f"Unsupported ordering-key type for {requested}: {actual_type}"
            )
        key_types.append(actual_type)
    return key_types


def _latest_snapshot(cursor: Any, table_sql: str) -> tuple[int, str]:
    """Return the latest Delta version number and its commit timestamp.

    Raises SourceTooOld when MAX_SOURCE_AGE_HOURS is set and the latest
    commit is older than that threshold.
    """
    cursor.execute(f"DESCRIBE HISTORY {table_sql} LIMIT 1")
    description = cursor.description or []
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("No Delta history row was returned")
    names = [item[0] for item in description]
    history = dict(zip(names, row))
    if "version" not in history or "timestamp" not in history:
        raise RuntimeError("Delta history did not contain version/timestamp")
    version = int(history["version"])
    data_as_of = _utc_iso(history["timestamp"])

    max_age_hours = float(os.getenv("MAX_SOURCE_AGE_HOURS", "0"))
    if max_age_hours > 0:
        source_time = datetime.fromisoformat(data_as_of.replace("Z", "+00:00"))  # noqa: FURB162
        age_seconds = (datetime.now(UTC) - source_time).total_seconds()
        if age_seconds > max_age_hours * 3600:
            raise SourceTooOld(
                "The latest committed table version is older than the configured freshness limit"
            )

    return version, data_as_of


def _cursor_scalar(value: Any) -> str:
    """Serialize a key column value to a string for storage in the cursor.

    NULL values are rejected — they would break keyset ordering.
    """
    if value is None:
        raise BackendConfigurationError("Ordering-key values cannot be NULL")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, Decimal)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise BackendConfigurationError(
        f"Unsupported Python value in ordering key: {type(value).__name__}"
    )


def _keyset_predicate(
    key_columns: list[str], key_types: list[str], last_keys: list[str]
) -> tuple[str, list[Any]]:
    """Build a WHERE clause for keyset pagination over a composite key.

    For keys (k1, k2) produces:
        WHERE (k1 > ?) OR (k1 = ? AND k2 > ?)

    CAST is applied so the Databricks engine compares the correct types.
    """
    if len(key_columns) != len(key_types) or len(key_columns) != len(last_keys):
        raise BackendConfigurationError("Cursor key schema does not match the table")

    disjunction: list[str] = []
    parameters: list[Any] = []
    for i, current_key in enumerate(key_columns):
        conjunction: list[str] = []
        for j in range(i):
            conjunction.append(f"{_ident(key_columns[j])} = CAST(? AS {key_types[j]})")
            parameters.append(last_keys[j])
        conjunction.append(f"{_ident(current_key)} > CAST(? AS {key_types[i]})")
        parameters.append(last_keys[i])
        disjunction.append("(" + " AND ".join(conjunction) + ")")

    return " WHERE " + " OR ".join(disjunction), parameters


def _json_value(value: Any) -> Any:
    """Convert a Databricks cell value to a JSON-serializable Python type.

    Decimals become strings to preserve exact precision.
    Aware datetimes are normalized to UTC; naive datetimes (TIMESTAMP_NTZ)
    are left without a timezone suffix.
    Binary data is Base64-encoded.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
            return value.isoformat().replace("+00:00", "Z")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if hasattr(value, "tolist"):
        return _json_value(value.tolist())
    if hasattr(value, "asDict"):
        return _json_value(value.asDict(recursive=True))
    return str(value)


def fetch_page(state: dict[str, Any] | None, page_size: int) -> dict[str, Any]:
    """Return one page of records from a pinned Delta snapshot.

    First call (state=None): discovers the latest Delta version, validates key
    columns, and returns page 1. Subsequent calls restore state from the cursor
    and read the same VERSION AS OF, guaranteeing snapshot consistency across pages.
    """
    table_sql, table_fqn_plain, configured_keys = get_table_config()

    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute("SET TIME ZONE 'UTC'")

        if state is None:
            key_columns = configured_keys
            key_types = _inspect_key_schema(cursor, table_sql, key_columns)
            version, data_as_of = _latest_snapshot(cursor, table_sql)
            last_keys = None
            page_number = 1
        else:
            if state.get("table_fqn") != table_fqn_plain:
                raise BackendConfigurationError(
                    "Cursor belongs to a different table configuration"
                )
            key_columns = state.get("key_columns", [])
            key_types = state.get("key_types", [])
            version = int(state["version"])
            data_as_of = str(state["data_as_of"])
            last_keys = state.get("last_keys")
            page_number = int(state.get("page", 1)) + 1
            if key_columns != configured_keys:
                raise BackendConfigurationError(
                    "Ordering-key configuration changed; restart pagination"
                )

        if not isinstance(version, int) or version < 0:
            raise BackendConfigurationError("Invalid Delta version")

        where_sql = ""
        parameters: list[Any] = []
        if last_keys is not None:
            where_sql, parameters = _keyset_predicate(
                key_columns, key_types, last_keys
            )

        order_sql = ", ".join(f"{_ident(k)} ASC" for k in key_columns)

        # Fetch page_size+1 rows so we can detect whether a next page exists
        # without an extra COUNT query. LIMIT must be a literal — Databricks SQL
        # does not accept a parameter marker in the LIMIT clause.
        fetch_limit = page_size + 1
        query = (
            f"SELECT * FROM {table_sql} VERSION AS OF {version}"  # nosec B608
            f"{where_sql} ORDER BY {order_sql} LIMIT {fetch_limit}"
        )

        try:
            cursor.arraysize = fetch_limit
            cursor.execute(query, parameters if parameters else None)
            # Arrow fetch retains Decimal precision and nested types (STRUCT/MAP/ARRAY)
            # more faithfully than the connector's default Python scalar conversion.
            arrow_page = cursor.fetchmany_arrow(fetch_limit)
            page_rows = arrow_page.to_pylist()
        except Exception as exc:
            msg = str(exc).lower()
            if "version" in msg and ("not exist" in msg or "cannot be found" in msg):
                raise SnapshotExpired(
                    "The pinned Delta version is no longer available. "
                    "Start a new pagination session."
                ) from exc
            raise

    has_more = len(page_rows) > page_size
    page_rows = page_rows[:page_size]

    records = [
        {name: _json_value(value) for name, value in row.items()}
        for row in page_rows
    ]

    next_cursor = None
    if has_more and page_rows:
        last_row = page_rows[-1]
        next_keys = [_cursor_scalar(last_row[key]) for key in key_columns]
        state_out = new_cursor_state(
            table_fqn=table_fqn_plain,
            version=version,
            data_as_of=data_as_of,
            key_columns=key_columns,
            key_types=key_types,
            last_keys=next_keys,
            page=page_number,
        )
        next_cursor = encode_cursor(state_out)

    return {
        "success": True,
        "page": page_number,
        "page_size": page_size,
        "has_more": has_more,
        "data_as_of": data_as_of,
        "source_table_version": version,
        "data": records,
        "next_cursor": next_cursor,
    }

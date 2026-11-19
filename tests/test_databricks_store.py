"""Mock integration tests for the Databricks data store.

These tests mock the Databricks SQL connection to test the pagination
logic, error handling, and JSON serialization without requiring an actual
Databricks workspace.
"""
from __future__ import annotations

import base64
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def env_config(monkeypatch):
    """Set up all required environment variables for databricks_store."""
    key = base64.urlsafe_b64encode(b"k" * 32).decode("ascii").rstrip("=")
    monkeypatch.setenv("TABLE_FQN", "test_catalog.test_schema.test_table")
    monkeypatch.setenv("ORDER_KEY_COLUMNS", "id")
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test123")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("CURSOR_ENCRYPTION_KEY_ACTIVE", key)
    monkeypatch.setenv("API_KEY_ACTIVE", "test-api-key")
    monkeypatch.setenv("CURSOR_TTL_SECONDS", "86400")
    monkeypatch.setenv("MAX_SOURCE_AGE_HOURS", "0")
    monkeypatch.setenv("QUERY_TIMEOUT_SECONDS", "30")


def _mock_arrow_table(rows: list[dict]) -> MagicMock:
    """Create a mock Arrow table that returns the given rows."""
    mock = MagicMock()
    mock.to_pylist.return_value = rows
    return mock


def _make_describe_rows(columns: dict[str, str]) -> list[tuple]:
    """Build DESCRIBE TABLE result rows from a column_name -> type mapping."""
    return [(name, dtype, "") for name, dtype in columns.items()]


def _make_history_row(version: int, timestamp: datetime) -> tuple:
    return (version, timestamp, "user", "WRITE", None, None, None, None)


HISTORY_DESCRIPTION = [
    ("version",), ("timestamp",), ("userId",), ("operation",),
    ("operationParameters",), ("notebook",), ("clusterId",), ("operationMetrics",),
]


# ---------------------------------------------------------------------------
# Successful page fetch tests
# ---------------------------------------------------------------------------

class TestFetchPageSuccess:
    @patch("databricks_store._connect")
    def test_first_page_returns_data(self, mock_connect, env_config):
        from databricks_store import fetch_page

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        # Set up cursor behavior for sequential execute calls
        describe_rows = _make_describe_rows({"id": "BIGINT", "name": "STRING"})
        history_ts = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
        history_row = _make_history_row(42, history_ts)

        call_count = [0]
        def side_effect_execute(sql, params=None):
            call_count[0] += 1

        mock_cursor.execute = MagicMock(side_effect=side_effect_execute)

        # fetchall returns DESCRIBE TABLE results
        mock_cursor.fetchall.return_value = describe_rows
        # fetchone returns DESCRIBE HISTORY result
        mock_cursor.description = HISTORY_DESCRIPTION
        mock_cursor.fetchone.return_value = history_row
        # fetchmany_arrow returns data rows
        mock_cursor.fetchmany_arrow.return_value = _mock_arrow_table([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])

        result = fetch_page(None, 10)

        assert result["success"] is True
        assert result["page"] == 1
        assert result["has_more"] is False
        assert result["source_table_version"] == 42
        assert len(result["data"]) == 2
        assert result["data"][0]["name"] == "Alice"
        assert result["next_cursor"] is None

    @patch("databricks_store._connect")
    def test_has_more_when_extra_row_returned(self, mock_connect, env_config):
        from databricks_store import fetch_page

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        describe_rows = _make_describe_rows({"id": "BIGINT", "name": "STRING"})
        history_ts = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)

        mock_cursor.fetchall.return_value = describe_rows
        mock_cursor.description = HISTORY_DESCRIPTION
        mock_cursor.fetchone.return_value = _make_history_row(10, history_ts)
        # Return 3 rows for page_size=2 → has_more=True
        mock_cursor.fetchmany_arrow.return_value = _mock_arrow_table([
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
            {"id": 3, "name": "C"},
        ])

        result = fetch_page(None, 2)

        assert result["has_more"] is True
        assert len(result["data"]) == 2
        assert result["next_cursor"] is not None


# ---------------------------------------------------------------------------
# Empty table tests
# ---------------------------------------------------------------------------

class TestEmptyTable:
    @patch("databricks_store._connect")
    def test_empty_table_returns_empty_data(self, mock_connect, env_config):
        from databricks_store import fetch_page

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_cursor.fetchall.return_value = _make_describe_rows({"id": "BIGINT"})
        mock_cursor.description = HISTORY_DESCRIPTION
        mock_cursor.fetchone.return_value = _make_history_row(1, datetime.now(UTC))
        mock_cursor.fetchmany_arrow.return_value = _mock_arrow_table([])

        result = fetch_page(None, 10)

        assert result["success"] is True
        assert result["data"] == []
        assert result["has_more"] is False
        assert result["next_cursor"] is None


# ---------------------------------------------------------------------------
# JSON serialization tests
# ---------------------------------------------------------------------------

class TestJsonSerialization:
    def test_decimal_preserved_as_string(self):
        from databricks_store import _json_value
        assert _json_value(Decimal("123.456")) == "123.456"
        assert _json_value(Decimal("0.00")) == "0.00"

    def test_null_preserved(self):
        from databricks_store import _json_value
        assert _json_value(None) is None

    def test_boolean_preserved(self):
        from databricks_store import _json_value
        assert _json_value(True) is True
        assert _json_value(False) is False

    def test_datetime_aware_to_utc(self):
        from databricks_store import _json_value
        dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
        assert _json_value(dt) == "2025-01-15T12:00:00Z"

    def test_datetime_naive_no_tz(self):
        from databricks_store import _json_value
        dt = datetime(2025, 1, 15, 12, 0, 0)
        assert _json_value(dt) == "2025-01-15T12:00:00"

    def test_date_serialized(self):
        from databricks_store import _json_value
        d = date(2025, 6, 15)
        assert _json_value(d) == "2025-06-15"

    def test_bytes_to_base64(self):
        from databricks_store import _json_value
        result = _json_value(b"\x00\x01\x02")
        assert result == base64.b64encode(b"\x00\x01\x02").decode("ascii")

    def test_nested_dict(self):
        from databricks_store import _json_value
        result = _json_value({"key": Decimal("1.5"), "nested": {"inner": None}})
        assert result == {"key": "1.5", "nested": {"inner": None}}

    def test_nested_list(self):
        from databricks_store import _json_value
        result = _json_value([1, Decimal("2.5"), None, "text"])
        assert result == [1, "2.5", None, "text"]


# ---------------------------------------------------------------------------
# Configuration validation tests
# ---------------------------------------------------------------------------

class TestConfigValidation:
    def test_missing_table_fqn_raises(self, monkeypatch):
        monkeypatch.delenv("TABLE_FQN", raising=False)
        from databricks_store import BackendConfigurationError, get_table_config
        with pytest.raises(BackendConfigurationError, match="TABLE_FQN"):
            get_table_config()

    def test_invalid_table_fqn_format(self, monkeypatch):
        monkeypatch.setenv("TABLE_FQN", "only_two.parts")
        from databricks_store import BackendConfigurationError, get_table_config
        with pytest.raises(BackendConfigurationError, match="catalog.schema.table"):
            get_table_config()

    def test_sql_injection_in_table_name_rejected(self, monkeypatch):
        monkeypatch.setenv("TABLE_FQN", "cat.sch.tbl; DROP TABLE x")
        from databricks_store import BackendConfigurationError, get_table_config
        with pytest.raises(BackendConfigurationError):
            get_table_config()

    def test_valid_table_fqn_parsed(self, monkeypatch):
        monkeypatch.setenv("TABLE_FQN", "my_catalog.my_schema.my_table")
        monkeypatch.setenv("ORDER_KEY_COLUMNS", "id,created_at")
        from databricks_store import get_table_config
        table_sql, plain, keys = get_table_config()
        assert table_sql == "`my_catalog`.`my_schema`.`my_table`"
        assert plain == "my_catalog.my_schema.my_table"
        assert keys == ["id", "created_at"]


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------

class TestErrorHandling:
    @patch("databricks_store._connect")
    def test_connection_failure_propagates(self, mock_connect, env_config):
        mock_connect.side_effect = Exception("Connection refused")
        from databricks_store import fetch_page
        with pytest.raises(Exception, match="Connection refused"):
            fetch_page(None, 10)

    @patch("databricks_store._connect")
    def test_source_too_old_raises(self, mock_connect, monkeypatch, env_config):
        monkeypatch.setenv("MAX_SOURCE_AGE_HOURS", "1")
        from databricks_store import SourceTooOld, fetch_page

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        old_ts = datetime(2020, 1, 1, tzinfo=UTC)
        mock_cursor.fetchall.return_value = _make_describe_rows({"id": "BIGINT"})
        mock_cursor.description = HISTORY_DESCRIPTION
        mock_cursor.fetchone.return_value = _make_history_row(1, old_ts)

        with pytest.raises(SourceTooOld):
            fetch_page(None, 10)


# ---------------------------------------------------------------------------
# Keyset predicate tests
# ---------------------------------------------------------------------------

class TestKeysetPredicate:
    def test_single_key_predicate(self):
        from databricks_store import _keyset_predicate
        where, params = _keyset_predicate(["id"], ["BIGINT"], ["100"])
        assert ">" in where
        assert params == ["100"]

    def test_composite_key_predicate(self):
        from databricks_store import _keyset_predicate
        where, params = _keyset_predicate(
            ["id", "seq"], ["BIGINT", "INT"], ["100", "5"]
        )
        # Should have disjunction for composite keys
        assert "OR" in where
        assert len(params) == 3  # id > ?, (id = ? AND seq > ?)

    def test_mismatched_lengths_raises(self):
        from databricks_store import BackendConfigurationError, _keyset_predicate
        with pytest.raises(BackendConfigurationError):
            _keyset_predicate(["id"], ["BIGINT"], ["100", "extra"])

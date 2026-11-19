"""Optional smoke test against a real Databricks workspace.

Run ONLY against an approved non-production warehouse and table.
Requires all Databricks environment variables to be configured.

SKIPPED automatically if credentials are not set.
"""
import os

import pytest

REQUIRED = [
    "TABLE_FQN",
    "ORDER_KEY_COLUMNS",
    "DATABRICKS_SERVER_HOSTNAME",
    "DATABRICKS_HTTP_PATH",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "CURSOR_ENCRYPTION_KEY_ACTIVE",
    "API_KEY_ACTIVE",
]

_all_configured = all(os.getenv(k) for k in REQUIRED)


@pytest.mark.skipif(not _all_configured, reason="Databricks integration settings are not configured")
class TestRealDatabricksIntegration:
    """Tests that run against a real Databricks workspace."""

    def test_fetch_first_page(self):
        pytest.importorskip("databricks.sdk")
        pytest.importorskip("databricks.sql")
        from databricks_store import fetch_page

        result = fetch_page(None, 2)
        assert result["success"] is True
        assert result["page"] == 1
        assert len(result["data"]) <= 2
        assert "data_as_of" in result
        assert "source_table_version" in result

    def test_pagination_round_trip(self):
        """Fetch two pages and verify no duplicates."""
        from databricks_store import fetch_page
        from security import decode_cursor

        page1 = fetch_page(None, 2)
        assert page1["success"] is True

        if not page1["has_more"]:
            pytest.skip("Table has fewer than 3 rows — cannot test pagination")

        cursor_state = decode_cursor(page1["next_cursor"])
        page2 = fetch_page(cursor_state, 2)
        assert page2["success"] is True
        assert page2["page"] == 2
        assert page2["source_table_version"] == page1["source_table_version"]

        # Verify no overlap between page 1 and page 2 data
        ids_p1 = {row.get("id") for row in page1["data"] if "id" in row}
        ids_p2 = {row.get("id") for row in page2["data"] if "id" in row}
        if ids_p1 and ids_p2:
            assert ids_p1.isdisjoint(ids_p2), "Duplicate records detected across pages"

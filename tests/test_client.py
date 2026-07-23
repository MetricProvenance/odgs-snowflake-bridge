"""
Tests for SnowflakeClient's SQL-identifier validation — the guard against
unsafe interpolation of database/schema/table names into query strings.
"""

import pytest
from odgs_snowflake.client import _validate_identifier


class TestValidateIdentifier:
    def test_accepts_simple_identifier(self):
        assert _validate_identifier("PRODUCTION") == "PRODUCTION"

    def test_accepts_dotted_identifier(self):
        assert _validate_identifier("PRODUCTION.FINANCE.TRANSACTIONS") == "PRODUCTION.FINANCE.TRANSACTIONS"

    def test_accepts_underscores_and_digits(self):
        assert _validate_identifier("MY_DB_2026") == "MY_DB_2026"

    def test_rejects_sql_injection_attempt(self):
        """The write-back path reads table names from a log file, not an
        operator-typed CLI flag — this must reject anything that could break
        out of the ALTER TABLE ... SET COMMENT statement."""
        with pytest.raises(ValueError):
            _validate_identifier("PRODUCTION; DROP TABLE USERS; --")

    def test_rejects_embedded_quote(self):
        with pytest.raises(ValueError):
            _validate_identifier("PRODUCTION' OR '1'='1")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError):
            _validate_identifier("")

    def test_rejects_leading_digit(self):
        with pytest.raises(ValueError):
            _validate_identifier("1PRODUCTION")


class TestSyncFilenameIncludesSchema:
    """Regression test: sync()'s output filename didn't include schema_filter,
    so syncing two schemas of the same database silently overwrote the first
    output with the second."""

    def test_filename_differs_by_schema(self, monkeypatch, tmp_path):
        from odgs_snowflake.bridge import SnowflakeBridge
        from odgs_snowflake.client import SnowflakeTable

        bridge = SnowflakeBridge(account="x", user="u", organization="acme", password="p")

        def fake_get_all_tables(database, schema_filter=None):
            return [
                SnowflakeTable(
                    full_name=f"{database}.{schema_filter or 'ANY'}.T",
                    database_name=database,
                    schema_name=schema_filter or "ANY",
                    table_name="T",
                    table_type="BASE TABLE",
                    row_count=1,
                    bytes=1,
                    comment="",
                    owner="",
                    columns=[],
                )
            ]

        monkeypatch.setattr(bridge.client, "get_all_tables", fake_get_all_tables)

        path_finance = bridge.sync(database="PROD", schema_filter="FINANCE", output_dir=str(tmp_path))
        path_sales = bridge.sync(database="PROD", schema_filter="SALES", output_dir=str(tmp_path))

        assert path_finance != path_sales
        assert "finance" in path_finance.lower()
        assert "sales" in path_sales.lower()

"""
Tests for the Snowflake → ODGS transformer.

Uses mock data to validate schema transformation without a live Snowflake instance.
"""

import pytest
from odgs_snowflake.client import SnowflakeTable, SnowflakeColumn
from odgs_snowflake.transformer import SnowflakeTransformer, _sanitize_urn


@pytest.fixture
def transformer():
    return SnowflakeTransformer(organization="acme_corp")


@pytest.fixture
def sample_table():
    return SnowflakeTable(
        full_name="PRODUCTION.FINANCE.TRANSACTIONS",
        database_name="PRODUCTION",
        schema_name="FINANCE",
        table_name="TRANSACTIONS",
        table_type="BASE TABLE",
        row_count=1500000,
        bytes=524288000,
        comment="Core transaction ledger",
        owner="FINANCE_ADMIN",
        columns=[
            SnowflakeColumn(name="TXN_ID", data_type="NUMBER", nullable=False, ordinal_position=1, numeric_precision=38, numeric_scale=0),
            SnowflakeColumn(name="AMOUNT", data_type="NUMBER", nullable=False, ordinal_position=2, numeric_precision=18, numeric_scale=2),
            SnowflakeColumn(name="CURRENCY", data_type="VARCHAR", nullable=True, ordinal_position=3, character_maximum_length=3),
            SnowflakeColumn(name="TXN_DATE", data_type="TIMESTAMP_NTZ", nullable=False, ordinal_position=4),
            SnowflakeColumn(name="METADATA", data_type="VARIANT", nullable=True, ordinal_position=5),
            SnowflakeColumn(name="REGION", data_type="VARCHAR", nullable=True, ordinal_position=6, character_maximum_length=50),
        ],
    )


@pytest.fixture
def view_table():
    return SnowflakeTable(
        full_name="ANALYTICS.REPORTING.REVENUE_SUMMARY",
        database_name="ANALYTICS",
        schema_name="REPORTING",
        table_name="REVENUE_SUMMARY",
        table_type="VIEW",
        comment="Monthly revenue by region",
        columns=[
            SnowflakeColumn(name="MONTH", data_type="DATE", nullable=False, ordinal_position=1),
            SnowflakeColumn(name="TOTAL_REVENUE", data_type="FLOAT", nullable=True, ordinal_position=2),
        ],
    )


class TestMetricTransformation:
    def test_basic_metric_structure(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)

        assert metric["metric_urn"] == "urn:odgs:custom:acme_corp:production_finance_transactions"
        assert metric["name"] == "TRANSACTIONS"
        assert metric["domain"] == "PRODUCTION.FINANCE"
        assert metric["source_authority"] == "snowflake:PRODUCTION"
        assert metric["schema"]["column_count"] == 6
        assert "content_hash" in metric
        assert len(metric["content_hash"]) == 64

    def test_statistics_captured(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        assert metric["statistics"]["row_count"] == 1500000
        assert metric["statistics"]["size_bytes"] == 524288000

    def test_statistics_omitted_when_missing(self, transformer, view_table):
        metric = transformer.table_to_metric(view_table)
        assert "row_count" not in metric["statistics"]

    def test_column_precision_captured(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        columns = metric["schema"]["columns"]
        amount_col = next(c for c in columns if c["name"] == "AMOUNT")
        assert amount_col["precision"] == 18
        assert amount_col["scale"] == 2

    def test_varchar_max_length(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        columns = metric["schema"]["columns"]
        currency_col = next(c for c in columns if c["name"] == "CURRENCY")
        assert currency_col["max_length"] == 3

    def test_provenance_tracking(self, transformer, sample_table):
        metric = transformer.table_to_metric(sample_table)
        prov = metric["provenance"]
        assert prov["bridge"] == "odgs-snowflake-bridge"
        assert prov["source_url"] == "snowflake://PRODUCTION.FINANCE.TRANSACTIONS"


class TestRuleGeneration:
    def test_not_null_rules(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        not_null_rules = [r for r in rules if r["constraint_type"] == "NOT_NULL"]
        # TXN_ID, AMOUNT, TXN_DATE are NOT NULL
        assert len(not_null_rules) == 3
        names = [r["target_column"] for r in not_null_rules]
        assert "TXN_ID" in names
        assert "AMOUNT" in names
        assert "TXN_DATE" in names
        assert "CURRENCY" not in names
        assert "METADATA" not in names

    def test_type_check_rules(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        type_rules = [r for r in rules if r["constraint_type"] == "TYPE_CHECK"]
        # All 6 columns should have type checks
        assert len(type_rules) == 6

    def test_variant_type_mapping(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        type_rules = [r for r in rules if r["constraint_type"] == "TYPE_CHECK"]
        variant_rule = next(r for r in type_rules if r["target_column"] == "METADATA")
        assert variant_rule["expected_type"] == "semi_structured"

    def test_max_length_rules(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        max_len_rules = [r for r in rules if r["constraint_type"] == "MAX_LENGTH"]
        # CURRENCY (VARCHAR(3)) and REGION (VARCHAR(50))
        assert len(max_len_rules) == 2
        currency_rule = next(r for r in max_len_rules if r["target_column"] == "CURRENCY")
        assert currency_rule["max_length"] == 3

    def test_severity_propagation(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table, severity="HARD_STOP")
        not_null_rules = [r for r in rules if r["constraint_type"] == "NOT_NULL"]
        assert all(r["severity"] == "HARD_STOP" for r in not_null_rules)

    def test_content_hashes(self, transformer, sample_table):
        rules = transformer.table_to_rules(sample_table)
        for rule in rules:
            assert "content_hash" in rule
            assert len(rule["content_hash"]) == 64


class TestSchemaPackOutput:
    def test_metrics_schema(self, transformer, sample_table, view_table):
        schema = transformer.transform_tables(
            [sample_table, view_table], output_type="metrics"
        )
        assert schema["$schema"] == "https://metricprovenance.com/schemas/odgs/v4"
        assert schema["metadata"]["source"] == "snowflake"
        assert schema["metadata"]["tables_processed"] == 2
        assert schema["metadata"]["items_generated"] == 2

    def test_rules_schema(self, transformer, sample_table):
        schema = transformer.transform_tables(
            [sample_table], output_type="rules"
        )
        assert schema["metadata"]["tables_processed"] == 1
        # 3 NOT_NULL + 6 TYPE_CHECK + 2 MAX_LENGTH = 11
        assert schema["metadata"]["items_generated"] == 11


class TestUrnSanitization:
    def test_dots_replaced(self):
        assert _sanitize_urn("PRODUCTION.FINANCE.TXN") == "production_finance_txn"

    def test_consecutive_underscores(self):
        assert "__" not in _sanitize_urn("hello  /  world")

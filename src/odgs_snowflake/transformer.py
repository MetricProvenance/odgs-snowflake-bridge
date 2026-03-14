"""
Snowflake → ODGS Schema Transformer
=====================================

Transforms Snowflake INFORMATION_SCHEMA table/column metadata into
ODGS-compliant JSON schemas for runtime enforcement.

Key difference from Databricks bridge:
- Uses Snowflake-native data types (NUMBER, VARCHAR, VARIANT, etc.)
- Captures Snowflake-specific metadata (row_count, bytes, clustering)
- Supports VARIANT/OBJECT/ARRAY semi-structured type detection
"""

import hashlib
import json
import re
import datetime
import logging
from typing import Any, Dict, List, Optional

from odgs_snowflake.client import SnowflakeTable, SnowflakeColumn

logger = logging.getLogger(__name__)


def _content_hash(data: Dict) -> str:
    """Generate SHA-256 content hash for immutability verification."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sanitize_urn(name: str) -> str:
    """Convert a name into a URN-safe identifier."""
    result = (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("/", "_")
        .replace("&", "and")
    )
    result = re.sub(r"_+", "_", result).strip("_")
    return result


# Snowflake type → ODGS constraint mapping
TYPE_CONSTRAINTS = {
    "NUMBER": {"type": "numeric"},
    "DECIMAL": {"type": "numeric"},
    "NUMERIC": {"type": "numeric"},
    "INT": {"type": "integer"},
    "INTEGER": {"type": "integer"},
    "BIGINT": {"type": "integer"},
    "SMALLINT": {"type": "integer"},
    "TINYINT": {"type": "integer"},
    "BYTEINT": {"type": "integer"},
    "FLOAT": {"type": "numeric"},
    "FLOAT4": {"type": "numeric"},
    "FLOAT8": {"type": "numeric"},
    "DOUBLE": {"type": "numeric"},
    "DOUBLE PRECISION": {"type": "numeric"},
    "REAL": {"type": "numeric"},
    "VARCHAR": {"type": "string"},
    "CHAR": {"type": "string"},
    "CHARACTER": {"type": "string"},
    "STRING": {"type": "string"},
    "TEXT": {"type": "string"},
    "BINARY": {"type": "binary"},
    "VARBINARY": {"type": "binary"},
    "BOOLEAN": {"type": "boolean"},
    "DATE": {"type": "date"},
    "DATETIME": {"type": "timestamp"},
    "TIME": {"type": "time"},
    "TIMESTAMP": {"type": "timestamp"},
    "TIMESTAMP_LTZ": {"type": "timestamp"},
    "TIMESTAMP_NTZ": {"type": "timestamp"},
    "TIMESTAMP_TZ": {"type": "timestamp"},
    "VARIANT": {"type": "semi_structured"},
    "OBJECT": {"type": "semi_structured"},
    "ARRAY": {"type": "semi_structured"},
}


class SnowflakeTransformer:
    """
    Transforms Snowflake INFORMATION_SCHEMA metadata into ODGS JSON schemas.

    Generates:
    - **Metric definitions** from table/column metadata
    - **Enforcement rules** from NOT NULL constraints and type checks

    Args:
        organization: Organization identifier for URN namespace
    """

    def __init__(self, organization: str):
        self.organization = _sanitize_urn(organization)

    def table_to_metric(self, table: SnowflakeTable) -> Dict[str, Any]:
        """Transform a Snowflake table into an ODGS metric definition."""
        table_urn = _sanitize_urn(table.full_name)
        urn = f"urn:odgs:custom:{self.organization}:{table_urn}"

        columns_spec = []
        for col in table.columns:
            col_spec = {
                "name": col.name,
                "data_type": col.data_type,
                "nullable": col.nullable,
                "ordinal_position": col.ordinal_position,
            }
            if col.comment:
                col_spec["description"] = col.comment
            if col.character_maximum_length:
                col_spec["max_length"] = col.character_maximum_length
            if col.numeric_precision is not None:
                col_spec["precision"] = col.numeric_precision
            if col.numeric_scale is not None:
                col_spec["scale"] = col.numeric_scale
            columns_spec.append(col_spec)

        metric = {
            "metric_id": _sanitize_urn(table.table_name),
            "metric_urn": urn,
            "name": table.table_name,
            "description": table.comment or f"Table {table.full_name}",
            "domain": f"{table.database_name}.{table.schema_name}",
            "source_authority": f"snowflake:{table.database_name}",
            "schema": {
                "full_name": table.full_name,
                "table_type": table.table_type,
                "column_count": len(table.columns),
                "columns": columns_spec,
            },
            "statistics": {},
            "compliance": {
                "owner": table.owner,
                "governance_database": table.database_name,
            },
            "provenance": {
                "bridge": "odgs-snowflake-bridge",
                "bridge_version": "0.1.0",
                "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source_url": f"snowflake://{table.full_name}",
            },
        }

        if table.row_count is not None:
            metric["statistics"]["row_count"] = table.row_count
        if table.bytes is not None:
            metric["statistics"]["size_bytes"] = table.bytes

        metric["content_hash"] = _content_hash(metric)
        return metric

    def table_to_rules(
        self,
        table: SnowflakeTable,
        severity: str = "WARNING",
    ) -> List[Dict[str, Any]]:
        """
        Generate ODGS enforcement rules from a Snowflake table's column schema.

        Creates rules for:
        - NOT NULL constraints
        - Type validation
        - VARCHAR max length constraints (when defined)
        """
        rules = []
        table_id = _sanitize_urn(table.table_name)

        for col in table.columns:
            col_id = _sanitize_urn(col.name)

            # Rule 1: NOT NULL enforcement
            if not col.nullable:
                rule_urn = f"urn:odgs:custom:{self.organization}:rule:{table_id}_{col_id}_not_null"
                rules.append({
                    "rule_id": f"{table_id}_{col_id}_not_null",
                    "rule_urn": rule_urn,
                    "name": f"{table.table_name}.{col.name} NOT NULL",
                    "description": f"Column {col.name} must not be null",
                    "domain": f"{table.database_name}.{table.schema_name}",
                    "severity": severity,
                    "logic_expression": f"{col.name} != None",
                    "constraint_type": "NOT_NULL",
                    "target_column": col.name,
                    "target_table": table.full_name,
                    "source_authority": f"snowflake:{table.database_name}",
                    "provenance": {
                        "bridge": "odgs-snowflake-bridge",
                        "bridge_version": "0.1.0",
                        "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "source_url": f"snowflake://{table.full_name}/{col.name}",
                    },
                })

            # Rule 2: Type constraint
            type_upper = col.data_type.upper().split("(")[0].strip()
            if type_upper in TYPE_CONSTRAINTS:
                rule_urn = f"urn:odgs:custom:{self.organization}:rule:{table_id}_{col_id}_type"
                rules.append({
                    "rule_id": f"{table_id}_{col_id}_type",
                    "rule_urn": rule_urn,
                    "name": f"{table.table_name}.{col.name} TYPE({col.data_type})",
                    "description": f"Column {col.name} must be {TYPE_CONSTRAINTS[type_upper]['type']}",
                    "domain": f"{table.database_name}.{table.schema_name}",
                    "severity": "INFO",
                    "logic_expression": f"type({col.name}) == '{TYPE_CONSTRAINTS[type_upper]['type']}'",
                    "constraint_type": "TYPE_CHECK",
                    "expected_type": TYPE_CONSTRAINTS[type_upper]["type"],
                    "target_column": col.name,
                    "target_table": table.full_name,
                    "source_authority": f"snowflake:{table.database_name}",
                    "provenance": {
                        "bridge": "odgs-snowflake-bridge",
                        "bridge_version": "0.1.0",
                        "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "source_url": f"snowflake://{table.full_name}/{col.name}",
                    },
                })

            # Rule 3: VARCHAR max length constraint
            if type_upper in ("VARCHAR", "CHAR", "STRING", "TEXT") and col.character_maximum_length:
                rule_urn = f"urn:odgs:custom:{self.organization}:rule:{table_id}_{col_id}_max_length"
                rules.append({
                    "rule_id": f"{table_id}_{col_id}_max_length",
                    "rule_urn": rule_urn,
                    "name": f"{table.table_name}.{col.name} MAX_LENGTH({col.character_maximum_length})",
                    "description": f"Column {col.name} must not exceed {col.character_maximum_length} characters",
                    "domain": f"{table.database_name}.{table.schema_name}",
                    "severity": "WARNING",
                    "logic_expression": f"len({col.name}) <= {col.character_maximum_length}",
                    "constraint_type": "MAX_LENGTH",
                    "max_length": col.character_maximum_length,
                    "target_column": col.name,
                    "target_table": table.full_name,
                    "source_authority": f"snowflake:{table.database_name}",
                    "provenance": {
                        "bridge": "odgs-snowflake-bridge",
                        "bridge_version": "0.1.0",
                        "synced_at": datetime.datetime.utcnow().isoformat() + "Z",
                        "source_url": f"snowflake://{table.full_name}/{col.name}",
                    },
                })

        # Add content hashes
        for rule in rules:
            rule["content_hash"] = _content_hash(rule)

        return rules

    def transform_tables(
        self,
        tables: List[SnowflakeTable],
        output_type: str = "metrics",
        severity: str = "WARNING",
    ) -> Dict[str, Any]:
        """Transform a list of Snowflake tables into an ODGS schema pack."""
        logger.warning("[ODGS Bridge] ⚠️ Compiling unsigned rules for ODGS Community Edition. Get Certified Sovereign Packs at https://platform.metricprovenance.com")
        items = []
        for table in tables:
            if output_type == "metrics":
                items.append(self.table_to_metric(table))
            elif output_type == "rules":
                items.extend(self.table_to_rules(table, severity=severity))

        schema = {
            "$schema": "https://metricprovenance.com/schemas/odgs/v4",
            "metadata": {
                "source": "snowflake",
                "organization": self.organization,
                "bridge": "odgs-snowflake-bridge",
                "bridge_version": "0.1.0",
                "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                "tables_processed": len(tables),
                "items_generated": len(items),
            },
            "items": items,
        }

        return schema

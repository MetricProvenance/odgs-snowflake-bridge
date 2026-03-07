"""
ODGS Snowflake Bridge — Orchestrator
======================================

High-level interface for syncing Snowflake table metadata
into ODGS schemas on the local filesystem.
"""

import json
import os
import logging
from typing import Optional

from odgs_snowflake.client import SnowflakeClient
from odgs_snowflake.transformer import SnowflakeTransformer

logger = logging.getLogger("odgs_snowflake.bridge")


class SnowflakeBridge:
    """
    High-level orchestrator that syncs Snowflake INFORMATION_SCHEMA
    metadata into ODGS-compliant JSON schema files.

    Usage:
        bridge = SnowflakeBridge(
            account="xy12345.eu-west-1",
            user="odgs_service",
            password="...",
            organization="acme_corp",
        )
        bridge.sync(database="PRODUCTION", output_dir="./schemas/custom/")
    """

    def __init__(
        self,
        account: str,
        user: str,
        organization: str,
        password: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        authenticator: str = "snowflake",
    ):
        self.client = SnowflakeClient(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            authenticator=authenticator,
        )
        self.transformer = SnowflakeTransformer(organization=organization)

    def sync(
        self,
        database: str,
        schema_filter: Optional[str] = None,
        output_dir: str = "./schemas/custom/",
        output_type: str = "metrics",
        severity: str = "WARNING",
    ) -> str:
        """
        Sync Snowflake tables to ODGS JSON schemas on disk.

        Args:
            database: Snowflake database name
            schema_filter: Optional schema name to limit scope
            output_dir: Directory to write ODGS JSON files
            output_type: "metrics" or "rules"
            severity: Rule severity level

        Returns:
            Absolute path to the generated schema file.
        """
        try:
            tables = self.client.get_all_tables(
                database=database,
                schema_filter=schema_filter,
            )

            if not tables:
                logger.warning(
                    f"No tables found in {database}"
                    + (f".{schema_filter}" if schema_filter else "")
                )
                return ""

            logger.info(
                f"Transforming {len(tables)} Snowflake tables → ODGS {output_type}"
            )

            schema = self.transformer.transform_tables(
                tables=tables,
                output_type=output_type,
                severity=severity,
            )

            os.makedirs(output_dir, exist_ok=True)
            db_id = database.replace(".", "_").lower()
            filename = f"snowflake_{self.transformer.organization}_{db_id}_{output_type}.json"
            filepath = os.path.join(output_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=2, ensure_ascii=False)

            abs_path = os.path.abspath(filepath)
            logger.info(
                f"✅ Written ODGS schema: {abs_path} "
                f"({len(tables)} tables → {schema['metadata']['items_generated']} {output_type})"
            )
            return abs_path

        finally:
            self.client.close()

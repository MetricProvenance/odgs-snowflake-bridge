"""
Snowflake INFORMATION_SCHEMA Client
====================================

Reads table and column metadata from Snowflake using the native connector
and INFORMATION_SCHEMA queries. No external REST API needed.

Reference: https://docs.snowflake.com/en/sql-reference/info-schema
"""

import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger("odgs_snowflake.client")


@dataclass
class SnowflakeColumn:
    """Represents a column in a Snowflake table."""
    name: str
    data_type: str
    nullable: bool = True
    comment: str = ""
    ordinal_position: int = 0
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None


@dataclass
class SnowflakeTable:
    """Represents a table or view in Snowflake."""
    full_name: str  # DATABASE.SCHEMA.TABLE
    database_name: str
    schema_name: str
    table_name: str
    table_type: str  # BASE TABLE, VIEW, EXTERNAL TABLE
    row_count: Optional[int] = None
    bytes: Optional[int] = None
    comment: str = ""
    owner: str = ""
    columns: List[SnowflakeColumn] = field(default_factory=list)


class SnowflakeClient:
    """
    Client for reading Snowflake table/column metadata via INFORMATION_SCHEMA.

    Uses the snowflake-connector-python for native Snowflake connectivity.

    Args:
        account: Snowflake account identifier (e.g., xy12345.eu-west-1)
        user: Snowflake username
        password: Snowflake password
        role: Role to use (optional, defaults to user's default role)
        warehouse: Warehouse for queries (optional)
        authenticator: Auth method (default: snowflake, or externalbrowser)
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        authenticator: str = "snowflake",
    ):
        self.account = account
        self.user = user
        self.password = password
        self.role = role
        self.warehouse = warehouse
        self.authenticator = authenticator
        self._connection = None

    def _connect(self):
        """Establish connection to Snowflake."""
        import snowflake.connector

        connect_params = {
            "account": self.account,
            "user": self.user,
            "authenticator": self.authenticator,
        }

        if self.password:
            connect_params["password"] = self.password
        if self.role:
            connect_params["role"] = self.role
        if self.warehouse:
            connect_params["warehouse"] = self.warehouse

        self._connection = snowflake.connector.connect(**connect_params)
        logger.info(f"Connected to Snowflake account: {self.account}")

    def _execute(self, query: str, database: Optional[str] = None) -> List[Dict]:
        """Execute a SQL query and return results as list of dicts."""
        if not self._connection:
            self._connect()

        cursor = self._connection.cursor()
        try:
            if database:
                cursor.execute(f"USE DATABASE {database}")

            cursor.execute(query)
            columns = [desc[0].lower() for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def list_databases(self) -> List[str]:
        """List all accessible databases."""
        rows = self._execute("SHOW DATABASES")
        return [r.get("name", "") for r in rows]

    def list_schemas(self, database: str) -> List[str]:
        """List all schemas in a database (excluding INFORMATION_SCHEMA)."""
        rows = self._execute(
            f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA' "
            f"ORDER BY SCHEMA_NAME"
        )
        return [r.get("schema_name", "") for r in rows]

    def list_tables(
        self,
        database: str,
        schema_name: str,
    ) -> List[SnowflakeTable]:
        """
        List all tables in a schema with column metadata.

        Uses INFORMATION_SCHEMA.TABLES and INFORMATION_SCHEMA.COLUMNS.
        """
        # Get tables
        table_rows = self._execute(
            f"SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, COMMENT, TABLE_OWNER "
            f"FROM {database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"ORDER BY TABLE_NAME"
        )

        # Get columns for all tables in this schema at once
        column_rows = self._execute(
            f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
            f"ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH, "
            f"NUMERIC_PRECISION, NUMERIC_SCALE, COMMENT "
            f"FROM {database}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema_name}' "
            f"ORDER BY TABLE_NAME, ORDINAL_POSITION"
        )

        # Group columns by table name
        columns_by_table: Dict[str, List[SnowflakeColumn]] = {}
        for col_row in column_rows:
            tname = col_row.get("table_name", "")
            col = SnowflakeColumn(
                name=col_row.get("column_name", ""),
                data_type=col_row.get("data_type", "VARCHAR"),
                nullable=col_row.get("is_nullable", "YES") == "YES",
                comment=col_row.get("comment", "") or "",
                ordinal_position=col_row.get("ordinal_position", 0),
                character_maximum_length=col_row.get("character_maximum_length"),
                numeric_precision=col_row.get("numeric_precision"),
                numeric_scale=col_row.get("numeric_scale"),
            )
            columns_by_table.setdefault(tname, []).append(col)

        tables = []
        for row in table_rows:
            tname = row.get("table_name", "")
            table = SnowflakeTable(
                full_name=f"{database}.{schema_name}.{tname}",
                database_name=database,
                schema_name=schema_name,
                table_name=tname,
                table_type=row.get("table_type", "BASE TABLE"),
                row_count=row.get("row_count"),
                bytes=row.get("bytes"),
                comment=row.get("comment", "") or "",
                owner=row.get("table_owner", ""),
                columns=columns_by_table.get(tname, []),
            )
            tables.append(table)

        logger.info(f"Fetched {len(tables)} tables from {database}.{schema_name}")
        return tables

    def get_all_tables(
        self,
        database: str,
        schema_filter: Optional[str] = None,
    ) -> List[SnowflakeTable]:
        """
        Fetch all tables across all schemas in a database.

        Args:
            database: Snowflake database name
            schema_filter: Optional schema name to limit scope
        """
        schemas = self.list_schemas(database)
        all_tables = []

        for schema_name in schemas:
            if schema_filter and schema_name != schema_filter:
                continue
            tables = self.list_tables(database, schema_name)
            all_tables.extend(tables)

        logger.info(f"Total: {len(all_tables)} tables from database '{database}'")
        return all_tables

    def close(self):
        """Close the Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

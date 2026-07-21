#!/usr/bin/env python3
"""
Mock Snowflake data source  (GLUE — clearly labeled)
====================================================

The real `odgs-snowflake-bridge` reads metadata through
`snowflake-connector-python` (a native DBAPI connection to a live Snowflake
account). There is no REST endpoint to point at offline, so — exactly as the
task's honesty rule requires — we mock at the DBAPI boundary, NOT the bridge.

This module provides a fake DBAPI connection/cursor that:
  * accepts the SAME INFORMATION_SCHEMA SQL the real SnowflakeClient emits,
  * returns canned rows from an in-memory SAMPLE catalog.

Everything above the connector runs for real:
  SnowflakeClient.list_databases / list_schemas / list_tables /
  get_all_tables (their real SQL + result-shaping) and the real
  SnowflakeTransformer. Only the network transport is replaced.

install_fake_connector() monkeypatches snowflake.connector.connect.

-----------------------------------------------------------------------
NOTE: ALL DATA BELOW IS SAMPLE DATA, invented for demo purposes only.
-----------------------------------------------------------------------
"""
import re

# ---- SAMPLE Snowflake catalog (INFORMATION_SCHEMA contents) ----------------
DATABASE = "PRODUCTION"
SCHEMATA = ["FINANCE", "INFORMATION_SCHEMA"]  # client filters out INFORMATION_SCHEMA

# TABLES rows: (TABLE_NAME, TABLE_TYPE, ROW_COUNT, BYTES, COMMENT, TABLE_OWNER)
TABLES = {
    "FINANCE": [
        ("TRANSACTIONS", "BASE TABLE", 1284000, 41943040,
         "Ledger of customer financial transactions (SAMPLE).", "FINANCE_ETL_ROLE"),
        ("CUSTOMERS", "BASE TABLE", 52000, 8388608,
         "Master customer records (SAMPLE).", "FINANCE_ETL_ROLE"),
    ],
}

# COLUMNS rows: (TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION,
#                CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COMMENT)
COLUMNS = {
    "FINANCE": [
        ("TRANSACTIONS", "TRANSACTION_ID", "VARCHAR",      "NO",  1, 32,   None, None, "Primary key"),
        ("TRANSACTIONS", "CUSTOMER_ID",    "VARCHAR",      "NO",  2, 16,   None, None, "FK to CUSTOMERS"),
        ("TRANSACTIONS", "AMOUNT",         "NUMBER",       "NO",  3, None, 18,   2,    "Transaction amount"),
        ("TRANSACTIONS", "CURRENCY",       "VARCHAR",      "NO",  4, 3,    None, None, "ISO-4217 code"),
        ("TRANSACTIONS", "CREATED_AT",     "TIMESTAMP_NTZ","NO",  5, None, None, None, "Event time"),
        ("TRANSACTIONS", "MEMO",           "VARCHAR",      "YES", 6, 256,  None, None, "Optional note"),
        ("CUSTOMERS",    "CUSTOMER_ID",    "VARCHAR",      "NO",  1, 16,   None, None, "Primary key"),
        ("CUSTOMERS",    "LEGAL_NAME",     "VARCHAR",      "NO",  2, 128,  None, None, "Registered name"),
        ("CUSTOMERS",    "RISK_TIER",      "NUMBER",       "NO",  3, None, 2,    0,    "1-5 risk rating"),
        ("CUSTOMERS",    "ONBOARDED_AT",   "DATE",         "YES", 4, None, None, None, "KYC completion date"),
    ],
}


class _FakeCursor:
    """Minimal DBAPI cursor that answers the SnowflakeClient's exact queries."""

    def __init__(self):
        self.description = None
        self._rows = []

    def execute(self, query):
        q = " ".join(query.split())  # normalise whitespace
        up = q.upper()

        if up.startswith("USE DATABASE"):
            self.description, self._rows = [("status",)], [("Statement executed successfully.",)]
            return self

        if up == "SHOW DATABASES":
            self.description = [("name",)]
            self._rows = [(DATABASE,)]
            return self

        if "INFORMATION_SCHEMA.SCHEMATA" in up:
            self.description = [("SCHEMA_NAME",)]
            self._rows = [(s,) for s in SCHEMATA if s != "INFORMATION_SCHEMA"]
            return self

        m = re.search(r"TABLE_SCHEMA\s*=\s*'([^']+)'", q)
        schema = m.group(1) if m else None

        if "INFORMATION_SCHEMA.TABLES" in up:
            self.description = [("TABLE_NAME",), ("TABLE_TYPE",), ("ROW_COUNT",),
                                ("BYTES",), ("COMMENT",), ("TABLE_OWNER",)]
            self._rows = list(TABLES.get(schema, []))
            return self

        if "INFORMATION_SCHEMA.COLUMNS" in up:
            self.description = [("TABLE_NAME",), ("COLUMN_NAME",), ("DATA_TYPE",),
                                ("IS_NULLABLE",), ("ORDINAL_POSITION",),
                                ("CHARACTER_MAXIMUM_LENGTH",), ("NUMERIC_PRECISION",),
                                ("NUMERIC_SCALE",), ("COMMENT",)]
            self._rows = list(COLUMNS.get(schema, []))
            return self

        raise NotImplementedError(f"Fake connector received unexpected SQL: {q}")

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, **params):
        self.params = params

    def cursor(self):
        return _FakeCursor()

    def close(self):
        pass


def install_fake_connector():
    """Monkeypatch snowflake.connector.connect to hand back the in-memory fake."""
    import snowflake.connector

    def _fake_connect(**params):
        return _FakeConnection(**params)

    snowflake.connector.connect = _fake_connect
    return _fake_connect

#!/usr/bin/env python3
"""
SEGMENT 3 (Snowflake) — Sync INFORMATION_SCHEMA metadata → ODGS schemas.

Uses the REAL `odgs-snowflake-bridge` package (SnowflakeBridge /
SnowflakeClient / SnowflakeTransformer, v0.4.0). Because Snowflake has no
offline REST endpoint, we mock ONLY the DBAPI transport (see
mock_snowflake_source.py — clearly labeled GLUE). The client's real
INFORMATION_SCHEMA SQL and the real transformer run unchanged.

Real API entrypoints exercised:
    SnowflakeBridge(account=..., user=..., password=..., organization=...)
    bridge.client.list_databases()
    bridge.client.list_schemas(database)
    bridge.client.get_all_tables(database)   # -> real INFORMATION_SCHEMA SQL
    bridge.sync(database=..., output_type="metrics"|"rules", severity="HARD_STOP")
"""
import logging

# 1) Replace ONLY the native connector transport with the in-memory SAMPLE source.
from mock_snowflake_source import install_fake_connector
install_fake_connector()

# 2) Everything below is the real bridge.
from odgs_snowflake import SnowflakeBridge

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s | %(message)s")

ACCOUNT = "acme-demo.eu-west-1"
DATABASE = "PRODUCTION"

bridge = SnowflakeBridge(
    account=ACCOUNT,
    user="odgs_service",
    password="demo",          # ignored by the fake connector
    organization="acme_finance",
)

print("=" * 74)
print("Connecting to Snowflake account:", ACCOUNT, "(DBAPI transport mocked)")
print("Databases:", bridge.client.list_databases())
print("Schemas in PRODUCTION:", bridge.client.list_schemas(DATABASE))

tables = bridge.client.get_all_tables(database=DATABASE)
print(f"\nSnowflake tables in '{DATABASE}':")
for t in tables:
    nn = sum(1 for col in t.columns if not col.nullable)
    print(f"  [{t.table_type:>10}] {t.full_name:<34} "
          f"{len(t.columns)} cols ({nn} NOT NULL)  rows~{t.row_count}")
print("=" * 74)

print("\n>>> bridge.sync(database='PRODUCTION', output_type='metrics')")
metrics_path = bridge.sync(
    database=DATABASE, output_dir="./schemas/custom/", output_type="metrics")

# NOTE: the sync() call above closes the connection in its finally-block; the
# fake connector is re-usable, so we can sync again for the rules output.
print("\n>>> bridge.sync(database='PRODUCTION', output_type='rules', severity='HARD_STOP')")
rules_path = bridge.sync(
    database=DATABASE, output_dir="./schemas/custom/", output_type="rules",
    severity="HARD_STOP")

print("\nGenerated ODGS schemas:")
print("  metrics:", metrics_path)
print("  rules:  ", rules_path)

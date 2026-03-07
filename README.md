# ODGS Snowflake Bridge

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![ODGS](https://img.shields.io/badge/ODGS-v4.0.1-0055AA)](https://github.com/MetricProvenance/odgs-protocol)

**Transform your Snowflake Data Dictionary into active ODGS runtime enforcement schemas.**

> Snowflake stores your data. ODGS enforces the rules.

---

## What It Does

The ODGS Snowflake Bridge connects to your Snowflake account, reads INFORMATION_SCHEMA metadata, and transforms table/column definitions into ODGS-compliant JSON schemas for the [Universal Interceptor](https://github.com/MetricProvenance/odgs-protocol).

```
Snowflake INFORMATION_SCHEMA      ODGS
┌──────────────┐     Bridge      ┌──────────────┐
│ Databases    │ ──────────────→ │ JSON Schema  │
│ Schemas      │   reads tables, │ + Interceptor│
│ Tables       │   outputs ODGS  │ = Enforcement│
│ Columns      │                 └──────────────┘
└──────────────┘
```

### Three Rule Types Generated

| Column Property | Rule Type | Example |
|---|---|---|
| `NOT NULL` constraint | `NOT_NULL` | `TXN_ID != None` |
| Data type | `TYPE_CHECK` | `type(AMOUNT) == 'numeric'` |
| `VARCHAR(N)` length | `MAX_LENGTH` | `len(CURRENCY) <= 3` |

Supports 35+ Snowflake data types including `VARIANT`, `OBJECT`, and `ARRAY` semi-structured types.

## Install

```bash
pip install odgs-snowflake-bridge
```

## Quick Start

### Python API

```python
from odgs_snowflake import SnowflakeBridge

bridge = SnowflakeBridge(
    account="xy12345.eu-west-1",
    user="odgs_service",
    password="...",
    organization="acme_corp",
)

# Sync all tables → ODGS metric definitions
bridge.sync(
    database="PRODUCTION",
    output_dir="./schemas/custom/",
    output_type="metrics",
)

# Sync column constraints → enforcement rules
bridge.sync(
    database="PRODUCTION",
    schema_filter="FINANCE",
    output_dir="./schemas/custom/",
    output_type="rules",
    severity="HARD_STOP",
)
```

### CLI

```bash
# Using environment variables
export SNOWFLAKE_ACCOUNT=xy12345.eu-west-1
export SNOWFLAKE_USER=odgs_service
export SNOWFLAKE_PASSWORD=...

odgs-snowflake sync \
    --org acme_corp \
    --database PRODUCTION \
    --schema FINANCE \
    --type rules \
    --severity HARD_STOP

# SSO / Browser auth
odgs-snowflake sync \
    --account xy12345.eu-west-1 \
    --user user@company.com \
    --authenticator externalbrowser \
    --org acme_corp \
    --database PRODUCTION
```

### Output

```json
{
  "$schema": "https://metricprovenance.com/schemas/odgs/v4",
  "metadata": {
    "source": "snowflake",
    "tables_processed": 8,
    "items_generated": 47
  },
  "items": [
    {
      "rule_urn": "urn:odgs:custom:acme_corp:rule:transactions_amount_not_null",
      "severity": "HARD_STOP",
      "constraint_type": "NOT_NULL",
      "target_table": "PRODUCTION.FINANCE.TRANSACTIONS",
      "content_hash": "a1b2c3..."
    }
  ]
}
```

## Authentication

| Method | CLI Flags | Environment Variables |
|---|---|---|
| Password | `--user` + `--password` | `SNOWFLAKE_USER` + `SNOWFLAKE_PASSWORD` |
| SSO / Browser | `--authenticator externalbrowser` | — |
| Account | `--account` | `SNOWFLAKE_ACCOUNT` |

## Requirements

- Python ≥ 3.9
- `odgs` ≥ 4.0.0 (core protocol)
- `snowflake-connector-python` ≥ 3.0.0
- Snowflake account with INFORMATION_SCHEMA access

## Related

- [ODGS Protocol](https://github.com/MetricProvenance/odgs-protocol) — The core enforcement engine
- [ODGS Collibra Bridge](https://github.com/MetricProvenance/odgs-collibra-bridge) — Collibra integration
- [ODGS Databricks Bridge](https://github.com/MetricProvenance/odgs-databricks-bridge) — Unity Catalog integration

---

## License

Apache 2.0 — [Metric Provenance](https://metricprovenance.com) | The Hague, NL 🇳🇱

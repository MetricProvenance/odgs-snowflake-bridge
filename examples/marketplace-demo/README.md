# Marketplace Demo — end-to-end without a Snowflake account

Self-contained demonstration of the ODGS Snowflake Bridge: a local mock
(`mock_snowflake_source.py`) patches the `snowflake.connector` DBAPI boundary
in-process and serves `INFORMATION_SCHEMA` results for a **sample**
`PRODUCTION.FINANCE` database — 2 tables (`TRANSACTIONS`, `CUSTOMERS`),
clearly labeled sample data. The real, unmodified `odgs-snowflake-bridge`
package syncs against it; the real `odgs` engine then enforces the generated
rules.

## Run

```bash
pip install odgs-snowflake-bridge odgs flask
bash run_seg3_snowflake.sh
```

Starts a JWKS server (`:8602`) — there is no separate mock REST server, since
Snowflake's own client transport is a DBAPI connection, not a REST API — then:

1. `sync_from_snowflake.py` — bridge fetches the `PRODUCTION.FINANCE` schema
   through the mocked `snowflake.connector` DBAPI boundary and writes two
   ODGS schema files to `schemas/custom/`
2. `build_and_enforce.py rows/transactions_ok.json` — VERDICT: APPROVED
3. `build_and_enforce.py rows/transactions_bad.json` — VERDICT: BLOCKED
   (HARD_STOP, exit 1) — a `NULL` `currency` column

`seg3_snowflake_output.txt` is a captured full run for reference.

## Honest accounting

- The bridge generates **24 rules** from the 2-table schema (column
  constraints, type assertions, nullability). Only the **5 `NOT NULL`
  HARD_STOP rules** on `TRANSACTIONS` are actually bound and enforced in this
  demo.
- The auto-generated `TYPE_CHECK` / `INFO` rules ship in the schema file but
  are **not bound** here — they use a `type()` check that falls outside the
  engine's `simpleeval` expression allowlist (a deliberate security
  restriction: the rule-evaluation sandbox only permits a constrained set of
  safe operations, not arbitrary Python builtins). Binding them would need a
  custom evaluator extension, which is out of scope for this demo.
- The mock replaces **only the `snowflake.connector` DBAPI boundary** — the
  Snowflake bridge has no REST surface to mock. The bridge's real SQL-building
  against `INFORMATION_SCHEMA`, result-shaping, and transformer code all run
  for real against the mocked connection; only the underlying network
  round-trip to an actual Snowflake account is replaced.
- The rules pack signature is **ES256/JWS**, verified by the engine's own
  `CryptoResolver` against the mock's JWKS at load time. The **Ed25519 "audit
  seal"** is demo glue around the engine-generated audit record, the same
  pattern as the Collibra and Databricks demos — not a built-in engine
  feature.
- Everything else — bridge code, schema sync, transformation, enforcement,
  blocking — is the real published packages doing real work. Only the
  catalog data is sample data.

Same rig pattern as `odgs-collibra-bridge`'s `examples/marketplace-demo/` and
`odgs-databricks-bridge`'s — mock API/DBAPI boundary + real bridge + real
engine.

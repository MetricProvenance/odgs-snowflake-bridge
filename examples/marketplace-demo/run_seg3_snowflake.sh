#!/usr/bin/env bash
# SEGMENT 3 (Snowflake) — single-command runnable.
# DBAPI-mocked INFORMATION_SCHEMA -> real odgs-snowflake-bridge sync -> ODGS
# schemas -> signed pack -> real engine enforces an OK row and a NULL row.
set -e
cd "$(dirname "$0")"

echo "[seg3-snowflake] starting JWKS on :8602 (Snowflake transport is DBAPI-mocked in-process)"
python3 jwks_server.py >/tmp/seg3_sf_jwks.log 2>&1 &
JWKS=$!
trap "kill $JWKS 2>/dev/null" EXIT
sleep 2

echo ""
echo '$ pip show odgs-snowflake-bridge | grep -E "Name|Version"'
pip show odgs-snowflake-bridge 2>/dev/null | grep -E "^(Name|Version):"

echo ""
echo '$ python3 sync_from_snowflake.py'
python3 sync_from_snowflake.py

echo ""
echo '$ python3 build_and_enforce.py rows/transactions_ok.json'
python3 build_and_enforce.py rows/transactions_ok.json

echo ""
echo '$ python3 build_and_enforce.py rows/transactions_bad.json'
python3 build_and_enforce.py rows/transactions_bad.json || echo "(exit 1 — NULL currency hard-stopped, as intended)"

echo ""
echo "[seg3-snowflake] done."

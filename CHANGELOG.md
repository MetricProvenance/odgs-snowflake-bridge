# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
adhering to [Semantic Versioning](https://semver.org/).

## [v0.4.3] - 2026-07-23

### 🐛 Fixed

- **`TYPE_CHECK` rules use `type()`, which the ODGS engine's sandboxed evaluator doesn't allow** — every `TYPE_CHECK` rule this bridge generates was silently non-functional against the real engine. Kept as always-`INFO` (documented, not silent) rather than honoring `--severity`, since doing so would falsely imply enforcement that isn't real.
- **`MAX_LENGTH` rules hardcoded severity to `WARNING`, ignoring `--severity`** — unlike `TYPE_CHECK`, `len()` **is** in the engine's safe-functions allowlist, so these rules genuinely can enforce. Now honors the requested severity.
- **`sync()`'s output filename didn't include the schema filter**, so syncing two schemas of the same database (e.g. `FINANCE` then `SALES`) silently overwrote the first output with the second. Filename now includes the schema when one is specified.
- **Unescaped SQL identifier interpolation** in `client.py` — database/schema/table names were interpolated directly into queries with no validation, most exposed on the `write-back` path where table names originate from a log file rather than an operator-typed CLI flag. Added `_validate_identifier()`, applied at every interpolation point.
- **`write-back` had no top-level error handling**, unlike `sync()` — a syntactically-valid-but-non-object JSON line (e.g. `null` or a list) raised an uncaught `AttributeError` past the parse step, aborting the whole run. Parse errors were also only logged at `DEBUG` (invisible by default) with no summary count. Now validated explicitly, logged at `INFO`, with a skipped-line count printed at the end.
- **`output_type`/`severity` were never validated** — a CLI typo silently produced a valid-looking but empty schema. Both now raise `ValueError` on an unrecognized value.
- **Hardcoded `"0.4.2"` `bridge_version` literal** in five places in `transformer.py` instead of importing `__version__` — the same drift class of bug already fixed once in the Collibra bridge this release cycle.

### 📄 Docs

- `transformer.py`'s module docstring claimed "clustering" metadata capture that doesn't exist anywhere in the code — removed.
- README overstated supported type count ("35+", actual is 33) and its architecture diagram said `LOG_ONLY` where the code says `INFO` — both corrected. Output Schema example didn't match any real code path (missing fields, a `plain_english_description` field that's never emitted, and — copied from the Collibra bridge's changelog — a stray reference to "Collibra bridge reads from asset attributes" in this repo's own v0.3.0 entry, fixed below) — corrected to a real `table_to_rules()` output.
- This changelog's `# Changelog` title had landed in the middle of the file (after the `[v0.3.0]` entry) instead of at the top — moved back.

Verified: full test suite passes (17 passed, no change from baseline).

## [v0.4.2] - 2026-07-20

### Docs

- Removed a committee/standards-body reference from the README that named specific external bodies — the standard and the software are two different things and this repo's docs should describe the latter, not standardization-process status.

## [v0.4.1] - 2026-07-18

### 🔧 Fixed — Version unification

- **Unified version metadata:** `pyproject.toml`, `__version__`, and the `bridge_version` stamped into emitted ODGS schemas now all report `0.4.1`. Previously these disagreed (package 0.4.0, `__version__` 1.0.0, emitted `bridge_version` stale), which made provenance metadata in generated schemas unreliable.

No functional changes to transformation logic.

## [v0.4.0] - 2026-04-13

### ✨ Added — ODGS v6.0 Compatibility

- **`SOFT_STOP` severity support:** CLI `--severity` flag and Python API now accept `SOFT_STOP`, the override-gated severity introduced in ODGS v6.0.0. Existing `HARD_STOP`, `WARNING`, and `INFO` remain unchanged.
- **Badge updated** to `v5.1+ | v6.0 Compatible` — signals forward compatibility with the ODGS Sovereign Validation Engine.
- **Architecture diagram** updated to include `SOFT_STOP` in the Universal Interceptor severity list.

### 🔗 Compatibility

- Requires `odgs>=5.1.0` — works with both v5.x and v6.0 engines.
- All changes are **additive, backward-compatible, and non-breaking**.

---

## [v0.3.0] - 2026-03-19

### ✨ Added

- **Legislative lineage fields (ODGS S-Cert v5.1.0):** Rules now include:
  - `legislative_source` — declares the source authority (defaults to `"BRIDGE_GENERATED_UNATTESTED"`; set explicitly to declare your legislative source)
  - `verbatim_source_text` — optional raw text from source
  - `semantic_hash: "UNATTESTED"` — placeholder for Registry-attested SHA-256 hash; upgrade to Registry at https://registry.metricprovenance.com
  - `verdict_on_pass: "PASS"` — explicit pass verdict per ODGS S-Cert specification

- **Bridge version bumped to 0.3.0** in all generated schema packs.

### ⚠️ Migration Notes

All new fields are additive. Existing schemas continue to work. Rules without `legislative_source` explicitly set will show `"BRIDGE_GENERATED_UNATTESTED"`.

## [0.2.0] - 2026-03-19

### 🔧 Fixed
- **Python 3.14 Forward Compatibility:** Replaced deprecated `datetime.datetime.utcnow()` with
  `datetime.datetime.now(datetime.timezone.utc)`. Timestamp format changes from
  `"2026-03-19T12:00:00Z"` (naive UTC with `Z`) to `"2026-03-19T12:00:00+00:00"` (timezone-aware ISO 8601).
- **Schema Reference:** Updated `$schema` URL from `schemas/odgs/v4` → `schemas/odgs/v5` to reflect
  compatibility with the ODGS v5.0.x polymorphic execution engine.
- **Provenance Metadata:** `bridge_version` field in all generated ODGS schemas now correctly reports `0.2.0`.

### 🔗 Compatibility
- Requires `odgs>=5.1.0` — targets the ODGS v5.1.0 audit engine (S-Cert, LOG_ONLY, temporal bounds).

## [0.1.0] - 2026-03-07

### 🚀 Initial Release
- Snowflake Data Dictionary → ODGS Schema transformation engine.
- Converts Snowflake INFORMATION_SCHEMA columns, statistics, and precision metadata into ODGS-compliant JSON schemas.
- CLI: `odgs-snowflake bridge` command for direct integration pipeline.
- SHA-256 content hashing for immutability verification.

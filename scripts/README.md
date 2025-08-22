# Script Inventory and Proposed Organization

This document catalogs the helper scripts in the `scripts/` directory, groups them by intent, and proposes a small reorganization to make the repository easier to navigate.

Sections:
- What these scripts are for
- Current script list (by category)
- Proposed directory layout
- Safety notes and recommended next steps

---

## What these scripts are for

The `scripts/` folder contains a mix of ingestion drivers (run data collection), example insertors (seed a raw table with a JSON example), DB admin helpers (apply SQL, list tables), and developer/debug helpers (dump sample rows, inspect URL status). Some scripts are destructive (truncate) and should be used with caution.

## Current script list (by category)

Ingestion drivers (coordinate HTTP fetch + bronze insert)
- `run_ingest.ps1` — PowerShell helper to prep venv, load `.env`, install deps, and run `python -m src.cli ingest`.
- `ingest_all_matches.py` — High-level runner for scoreboard + schedule ingestion (discovery or numeric ranges).
- `ingest_competitions.py` — Competition ingestion with pagination support and id discovery.
- `ingest_matches.py` — Helper to ingest a list of scoreboard match ids.
- `ingest_teams_range.py` — Iterate a range of team ids and ingest `raw_teams` payloads.

Examples and small idempotent insertors
- `insert_schedule_example.py` — Insert a schedule JSON example into `raw_schedule` (idempotent via `source_hash`).
- `insert_competition_example.py` — Same for `raw_competition`.
- `create_raw_competition.py` — Create `raw_competition` table if it does not exist.

DB admin & apply utilities
- `apply_sql_file.py` — Apply a `.sql` file by splitting on semicolons and executing statements. Useful for quick DDL application (note: naive splitting).
- `run_ensure_tables.py` — Wrapper that calls `ingest.ensure_tables(conn)`.
- `list_tables.py` — Introspect DB tables/columns and write `tmp_db_schema.json`.

Dev / inspection / dumps (non-destructive)
- `dump_scoreboard_payloads.py` — Export recent `raw_scoreboard` rows to `tmp_scoreboard_payloads.json`.
- `dump_raw_competition_rows.py` — Export sample rows from `raw_competition`.
- `dump_schedule_samples.py` — Export sample rows from schedule-related silver tables.
- `collect_silver_counts.py` — Write counts for several silver tables to `tmp_silver_counts.json`.
- `show_url_status.py` — Print entries from `url_status` table for review.
- `printSimpleMap.js` — Node helper that extracts the template literal from `src/tableMap.ts`.
- `check_raw_624.py` — Ad-hoc query script for a known scoreboard id (debug helper).

Maintenance / destructive operations (use with caution)
- `clear_raw_competition.py` — TRUNCATE `raw_competition` (destructive).
- `clear_competition_silver.py` — TRUNCATE competition-related silver tables (destructive).

## Proposed directory layout

Recommended grouping to reduce noise and improve discoverability.

- `scripts/` (ingestion drivers + cross-platform helpers)
  - `ingest_all_matches.py`
  - `ingest_competitions.py`
  - `ingest_matches.py`
  - `ingest_teams_range.py`
  - `run_ingest.ps1`
  - `run_ensure_tables.py`

- `scripts/examples/` (idempotent example insertors and small create helpers)
  - `insert_schedule_example.py`
  - `insert_competition_example.py`
  - `create_raw_competition.py`

- `scripts/dev/` (debugging, dumps, non-destructive dev tools)
  - `dump_scoreboard_payloads.py`
  - `dump_raw_competition_rows.py`
  - `dump_schedule_samples.py`
  - `collect_silver_counts.py`
  - `show_url_status.py`
  - `printSimpleMap.js`
  - `check_raw_624.py`

- `scripts/db/` (DB admin and potentially-destructive ops)
  - `apply_sql_file.py`
  - `list_tables.py`
  - `clear_raw_competition.py`
  - `clear_competition_silver.py`

## Safety notes and recommended next steps

- Mark destructive scripts clearly (top-of-file comment or `--yes` gate). Consider adding a confirmation prompt or require `ALLOW_DESTRUCTIVE=1` env var.
- Add short usage lines at the top of ad-hoc scripts (some already have them). This helps future maintainers.
- Consider moving `run_ingest.ps1` to a `scripts/windows/` folder if you plan to maintain platform-specific helpers.

If you want, I can now perform the reorganization (move files into the proposed subdirectories) and add this README under `scripts/`. Confirm and I'll proceed.

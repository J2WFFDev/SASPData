# SASPData

**Minimal Ingestion Pipeline** - Clean, simple data collection from SAS APIs into PostgreSQL.

> **Note**: The complete ETL pipeline with silver transforms is preserved in git tag `v1`. This version focuses on reliable bronze ingestion only.

## What this does

- Ingests raw JSON data from SAS APIs into PostgreSQL `raw_*` tables
- Idempotent ingestion (won't duplicate data)
- HTTP resilience with backoff and 404 tracking
- Supports teams, scoreboard, schedule, and competition endpoints

## Quick Start

1. **Setup environment:**
   ```powershell
   # Copy and edit environment file
   cp .env.example .env
   # Edit .env with your PostgreSQL credentials
   ```

2. **Run ingestion:**
   ```powershell
   # Setup venv, install deps, run ingest
   .\scripts\run_ingest.ps1
   
   # Or ingest specific team range
   $env:START_ID="1800"; $env:END_ID="1900"; python .\scripts\ingest_teams_range.py
   ```

3. **Check data:**
   ```sql
   SELECT COUNT(*) FROM raw_teams;
   SELECT COUNT(*) FROM raw_scoreboard;
   ```

## Raw Tables

- `raw_teams` - Team JSON payloads
- `raw_scoreboard` - Match scoreboard data  
- `raw_schedule` - Match schedule data
- `raw_competition` - Competition metadata

All tables include `source_hash` for idempotency and `ingested_at` timestamps.

# SASPData

**Steel Challenge Analytics Pipeline** - Complete data ingestion and analytics for competitive shooting sports.

## 🚀 Current Status (August 22, 2025)

**Phase 1: Analytics Foundation** - In Progress  
- ✅ Complete scripts organization (bronze/silver/admin/other)
- ✅ Performance ETL pipeline (stage/match/squad aggregation)  
- ✅ Rankings tables deployed (individual + squad)
- ⚠️ Rankings ETL needs data fixes to complete

**Next:** Fix team_key constraints → Complete rankings pipeline → Phase 2 full re-ingestion

See `HANDOFF.md` for detailed session documentation and next steps.

---

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

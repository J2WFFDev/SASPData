SASPData — Handoff Notes
=========================

Quick start (run locally)

1. Ensure Postgres env vars are set:

   - `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`

2. Install Python deps:

   ```pwsh
   pip install -r requirements.txt
   ```

3. Run a small ingest (example IDs 1..10):

   ```pwsh
   $env:START_ID='1'; $env:END_ID='10'; python .\scripts\ingest_all_matches.py
   ```

4. Collect silver counts:

   ```pwsh
   python .\scripts\collect_silver_counts.py
   ```

Useful developer commands

- Print the compact table map:

  ```pwsh
  node .\scripts\printSimpleMap.js
  ```

- Open the full ASCII map (in `doc/table-map.md`).

Next work (priority)

1. Implement `src/SilverScheduleTransform.py` to normalize `raw_schedule` into silver tables.
2. Add field-to-column mapping documentation for each silver target in `doc/table-map.md`.
3. Add lightweight unit tests for canonicalization and `extract_match_id_from_url()`.

Notes

- Bronze ingestion is idempotent: JSON is canonicalized and `source_hash` is used to avoid duplicates.
- `url_status` heuristics reduce repeated 404s; see `src/ingest.py` for details.

Commit history note

- Latest pushed commit: `ebb4504` — adds the table map and print script.

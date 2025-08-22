# Silver Transformations

This document summarizes the silver-level transformations currently implemented in the repo, the tables they create or update, and notes about a `dim_team` schema conflict encountered and how it was resolved.


I'll scan the scoreboard transform code and the DB DDL files to list every silver table the scoreboard transform writes to, then summarize each table's purpose. I'll check SilverScoreboardTransform.py and db files for matching DDL.

Plan: I'll list the silver tables the scoreboard transform writes, with a 1-line purpose and the important key/unique columns to help you validate.

dim_scoreboard: scoreboard header/identity.

Purpose: one row per source scoreboard (external id, name, generated_at).
Important cols: scoreboard_id (pk), external_id (unique), name, generated_at, raw_id (raw source reference).
dim_discipline: discipline lookup (e.g., trap, skeet).

Purpose: canonical codes/labels for disciplines.
Important cols: discipline_id (pk), code (unique), label.
dim_athlete: normalized athlete/person.

Purpose: one row per athlete identity used by roster/results.
Important cols: athlete_id (pk), ath_id (unique source id), fname, lname, (optional gender, bdate).
dim_team (upserted by scoreboard transform): team lookup/identity.

Purpose: scoreboard references to teams; transform calls an upsert helper for teams.
Important cols (varies by schema): team_id (pk), ent_id or (source, source_ent_id) for source id, team_name.
map_roster: roster mapping for a scoreboard.

Purpose: maps scoreboard_id + athlete_id to team_id and discipline_id (slot/class/status included).
Important cols / PK: composite PK (scoreboard_id, athlete_id, discipline_id), plus slot_id, class_id, eligible, status.
fact_stage_series_result: per-stage/per-series numeric results.

Purpose: raw numeric series results (e.g., spp1_1 → value).
Important cols / PK: composite PK (scoreboard_id, athlete_id, stage_num, series_num), value, payload JSONB, inserted_at.
fact_penalty: penalties tied to athlete/stage/series.

Purpose: penalties extracted from payload fields (pen, penalty keys).
Important cols / indexes: (scoreboard_id, athlete_id, stage_num, series_num nullable, penalty_code, penalty_value). A unique index ux_fact_penalty_unique exists to deduplicate by (scoreboard_id, athlete_id, stage_num, penalty_code, COALESCE(series_num,0)).
fact_stage_total: derived per-stage totals per athlete.

Purpose: aggregates of series values + penalties per stage (raw_time, penalties, final_time).
Important cols / PK: (scoreboard_id, athlete_id, stage_num), raw_time, penalties, final_time.
Quick validation queries you can run locally (PowerShell / psql)

Count rows per silver table:
Show recent scoreboard processed:
Spot-check an athlete’s series:
Note on the dim_team conflict you observed

SilverScoreboardTransform.py also upserts dim_team (it calls upsert_dim_team), so there are two places in this repo that expect/write dim_team (scoreboard transform and the team transform). We can:
Consolidate both transforms onto a single dim_team schema (recommended), or
Namespace one transform’s table to avoid conflicts while we migrate.


## Scoreboard Transform (implemented)

- **Source**: `src/SilverScoreboardTransform.py`
- **Source raw table**: `raw_scoreboard`
- **Primary silver tables created/updated**:
  - `dim_scoreboard` — scoreboard header (one row per source scoreboard). Key: `scoreboard_id`, `external_id` (unique), `raw_id`.
  - `dim_discipline` — discipline lookup. Key: `discipline_id`, `code` (unique).
  - `dim_athlete` — normalized athlete. Key: `athlete_id`, `ath_id` (unique).
  - `dim_team` — team lookup (upserted by the scoreboard transform). Note: schema historically varied; see `dim_team` conflict below.
  - `map_roster` — roster mapping for a scoreboard. PK: `(scoreboard_id, athlete_id, discipline_id)`.
  - `fact_stage_series_result` — per stage/series numeric results. PK: `(scoreboard_id, athlete_id, stage_num, series_num)`.
  - `fact_penalty` — penalty entries; unique index `ux_fact_penalty_unique` enforces de-dup.
  - `fact_stage_total` — per-stage aggregates (raw_time + penalties -> final_time). PK: `(scoreboard_id, athlete_id, stage_num)`.

## Team Transform (implemented)

- **Source**: `src/SilverTeamTransform.py`
- **Source raw table**: `raw_teams`
- **Primary silver tables created/updated**:
  - `dim_team` — canonical team entity. Columns (from `db/silver_teams.sql`): `team_id` (pk), `source`, `source_ent_id`, `team_name`, `display_name`, `short_name`, `country`, `state`, `city`, `metadata`, `first_seen`, `last_seen`, `active`. Unique key: `(source, source_ent_id)`.
  - `fact_team_snapshot` — snapshot of `raw_teams` payloads. Columns: `snapshot_id` (pk), `team_id` (fk -> dim_team), `raw_id` (fk -> raw_teams), `snapshot_at`, `payload`, `payload_hash`.
  - `dim_person` — normalized person entity for roster members. Columns: `person_id` (pk), `source`, `source_ent_id`, `given_name`, `family_name`, `full_name`, `dob`, `country`, `metadata`. Unique key: `(source, source_ent_id)`.
  - `map_team_members` — mapping persons to teams. Columns: `team_id` (fk), `person_id` (fk), `role`, `source_raw_id`, `added_at`. PK: `(team_id, person_id, role)`.

Quick validation queries (psql / PowerShell):
```sql
SELECT COUNT(*) FROM dim_team;
SELECT COUNT(*) FROM fact_team_snapshot;
SELECT COUNT(*) FROM dim_person;
SELECT COUNT(*) FROM map_team_members;

-- show recent snapshots
SELECT snapshot_id, team_id, raw_id, snapshot_at FROM fact_team_snapshot ORDER BY snapshot_at DESC LIMIT 10;

-- show teams with no snapshots
SELECT * FROM dim_team WHERE team_id NOT IN (SELECT DISTINCT team_id FROM fact_team_snapshot);
```

Notes:
- `src/SilverTeamTransform.py` attempts to upsert `dim_team` using the modern `(source, source_ent_id)` key and falls back to a legacy `ent_id` upsert if the columns are missing; this makes it tolerant to mixed schemas during migration.
- Consider adding `UNIQUE(payload_hash)` to `fact_team_snapshot` to enforce idempotent snapshots (or check existing snapshots by `payload_hash` before insert).

## Conflict: `dim_team` schema mismatch

- Problem: `dim_team` already existed in the DB with a legacy schema (`team_id`, `ent_id`, `team_name`) from earlier transforms (scoreboard transform uses `ent_id`). The new team transform expects a richer schema with `source` and `source_ent_id`.
- Actions taken:
  1. Added `db/silver_teams.sql` containing the new table definitions.
  2. Implemented a non-destructive migration script `db/migrate_silver_team_schema.py` that:
     - Adds missing columns to `dim_team` if they don't exist (`source`, `source_ent_id`, `metadata`, `first_seen`, `last_seen`, `active`, etc.).
     - Creates `fact_team_snapshot`, `dim_person`, and `map_team_members` if missing.
     - Populates `source_ent_id` from `ent_id` where possible and sets `source='legacy'` for legacy rows.
  3. Updated `src/SilverTeamTransform.py`'s `upsert_dim_team()` to try the modern `(source, source_ent_id)` upsert, and fall back to the legacy `(ent_id)` insert/upsert when necessary. This makes transforms tolerant of either schema during migration.
  4. Re-ran `src/SilverTeamTransform.py` after migration — transforms completed for `raw_teams` rows 1..50 and `fact_team_snapshot` entries were created.

## Validation queries

- Count rows per relevant table:
  - `SELECT COUNT(*) FROM dim_team;`
  - `SELECT COUNT(*) FROM fact_team_snapshot;`
  - `SELECT COUNT(*) FROM map_team_members;`

- Find legacy-only `dim_team` rows:
  ```sql
  SELECT team_id, ent_id, team_name FROM dim_team WHERE source IS NULL OR source = 'legacy';
  ```

- Find rows where both legacy and modern identifiers exist (potential duplicates):
  ```sql
  SELECT d1.team_id AS legacy_id, d1.ent_id, d1.team_name AS legacy_name,
         d2.team_id AS modern_id, d2.source, d2.source_ent_id, d2.team_name AS modern_name
  FROM dim_team d1
  JOIN dim_team d2 ON d1.ent_id::text = d2.source_ent_id
  WHERE d2.source IS NOT NULL;
  ```

## Recommendations / Next Steps

1. Consolidate `dim_team` into a single canonical schema (preferred):
   - Use `(source, source_ent_id)` as the canonical unique key.
   - Keep `ent_id` only as legacy for backward compatibility or remove after migration.
   - Update `SilverScoreboardTransform.py` to use the modern upsert pattern (try `(source,source_ent_id)` then fall back to `ent_id`).

2. Make snapshots idempotent:
   - Add `UNIQUE(payload_hash)` on `fact_team_snapshot` or guard inserts by `payload_hash` to avoid duplicate snapshots.

3. Add tests / CI checks:
   - Add a unit/integration test that runs the transforms and verifies `dim_team` uniqueness and snapshot creation.

4. Cleanup plan (optional): once consolidated, drop legacy `ent_id` column and remove legacy paths.

## Competition Transform (implemented)

- **Source**: `src/SilverCompetitionTransform.py`
- **Source raw table**: `raw_competition`
- **Primary silver tables created/updated**:
  - `dim_state` — geographic/state lookup used by ranges and teams. Key: `state_id` (pk), `name`, `abbr`.
  - `dim_team` — hosting team lookup (competition hosting_team). Key: `team_id` (pk), `external_id` (unique) in competition DDL; transform will adapt to available schema.
  - `dim_range` — range/location metadata. Key: `range_id` (pk), `name`, `phone`, `email`, `url`.
  - `dim_contact` — contact/organizer information. Key: `contact_id` (pk), `fname`, `lname`, `full_name`.
  - `dim_registration_type` — registration type mapping. Key: `registration_type_id` (pk), `external_id` (unique), `value`.
  - `dim_classification` — classification mapping. Key: `classification_id` (pk), `external_id` (unique), `value`.
  - `dim_competition` — competition header/metadata (one row per competition). Key: `competition_id` (pk), `external_id` (unique), `name`, `start_date`, `end_date`, plus fk refs to registration_type, classification, hosting_team, range, contact.
  - `competition_stage` — per-competition stage rows (stage_one..stage_four). PK: `(competition_id, stage_num)`.
  - `bridge_competition_invited_team` — bridge table for invited teams. PK: `(competition_id, team_id)`.

The DDL for these tables is in `db/silver_competition_pg.sql` and matches the fields used by `src/SilverCompetitionTransform.py`.

Quick validation queries (psql / PowerShell):
```sql
SELECT COUNT(*) FROM dim_competition;
SELECT COUNT(*) FROM competition_stage;
SELECT COUNT(*) FROM bridge_competition_invited_team;
SELECT COUNT(*) FROM dim_registration_type;
SELECT COUNT(*) FROM dim_classification;
```

Notes and recommendations:
- `src/SilverCompetitionTransform.py` is defensive about existing `dim_team` column names and will attempt to upsert teams using whichever key/columns exist (`external_id`, `ent_id`, or name fallback). This reduces risk during the `dim_team` consolidation discussed earlier.
- The `dim_competition` DDL includes useful indexes (`ix_comp_dates`, `ix_comp_org_status`) to support date-range and org/status queries.
- If you plan to consolidate `dim_team` across transforms, ensure `db/silver_competition_pg.sql` and `db/silver_teams.sql` share the canonical `dim_team` definition before running large-scale re-ingests to avoid FK/constraint conflicts.

## Visual Map — raw → scripts → silver (table + key columns)

Below is a compact grid followed by a hierarchical view that shows, for each `raw_` table, the transform script(s) that consume it, the silver tables produced/updated, and the important key columns to inspect.

| Raw Table | Transform Script(s) | Silver Tables (key columns) | DDL / Source of CREATEs |
|---|---|---|---|
| `raw_scoreboard` | `src/SilverScoreboardTransform.py` | `dim_scoreboard` (scoreboard_id, external_id, raw_id), `dim_discipline` (discipline_id, code), `dim_athlete` (athlete_id, ath_id), `dim_team` (team_id, ent_id), `map_roster` (scoreboard_id, athlete_id, discipline_id), `dim_stage` (stage_id, discipline_id, stage_num), `dim_series` (series_id, stage_id, series_num), `fact_stage_series_result` (scoreboard_id, athlete_id, stage_num, series_num), `fact_penalty` (scoreboard_id, athlete_id, stage_num, penalty_code), `fact_stage_total` (scoreboard_id, athlete_id, stage_num), `silver_property` (scoreboard_id, entity_type, entity_id, prop_key) | `db/silver_scoreboard.sql` |
| `raw_teams` | `src/SilverTeamTransform.py` | `dim_team` (team_id, source, source_ent_id), `fact_team_snapshot` (snapshot_id, team_id, raw_id, payload_hash), `dim_person` (person_id, source, source_ent_id), `map_team_members` (team_id, person_id, role) | `db/silver_teams.sql` |
| `raw_competition` | `src/SilverCompetitionTransform.py` | `dim_competition` (competition_id, external_id), `competition_stage` (competition_id, stage_num), `bridge_competition_invited_team` (competition_id, team_id), `dim_registration_type` (registration_type_id, external_id), `dim_classification` (classification_id, external_id), `dim_range` (range_id), `dim_contact` (contact_id), `dim_state` (state_id) | `db/silver_competition_pg.sql` |

---

### Hierarchical view (expanded)

- raw_scoreboard
  - Script: `src/SilverScoreboardTransform.py`
  - DDL: `db/silver_scoreboard.sql`
  - Silver tables and purpose:
    - `dim_scoreboard` — scoreboard header/identity.
      - Key columns: `scoreboard_id` (PK), `external_id` (unique), `raw_id`.
    - `dim_discipline` — discipline lookup (trap/skeet/etc.). Key: `discipline_id`, `code`.
    - `dim_athlete` — athlete/person normalization. Key: `athlete_id`, `ath_id`.
    - `dim_team` — team lookup used by scoreboard (legacy shape). Key: `team_id`, `ent_id` (legacy external id).
    - `map_roster` — maps scoreboard → athlete → team → discipline. PK: `(scoreboard_id, athlete_id, discipline_id)`.
    - `dim_stage` / `dim_series` — optional expressive tables for canonical stage/series indexing. Keys: `(discipline_id, stage_num)`, `(stage_id, series_num)`.
    - `fact_stage_series_result` — per-series numeric results. PK: `(scoreboard_id, athlete_id, stage_num, series_num)`.
    - `fact_penalty` — penalties; deduplicated by unique index `ux_fact_penalty_unique`.
    - `fact_stage_total` — per-stage aggregates (raw_time, penalties, final_time).
    - `silver_property` — small JSONB key/value store scoped by scoreboard and entity.

- raw_teams
  - Script: `src/SilverTeamTransform.py`
  - DDL: `db/silver_teams.sql`
  - Silver tables and purpose:
    - `dim_team` — canonical team entity (modern). Key: `team_id`, unique `(source, source_ent_id)`.
    - `fact_team_snapshot` — stores raw payload snapshots with `payload_hash` for deduplication. Key: `snapshot_id`, `payload_hash`.
    - `dim_person` — roster/person normalization. Key: `person_id`, unique `(source, source_ent_id)`.
    - `map_team_members` — mapping persons → teams (role/captain/player). PK: `(team_id, person_id, role)`.

- raw_competition
  - Script: `src/SilverCompetitionTransform.py`
  - DDL: `db/silver_competition_pg.sql`
  - Silver tables and purpose:
    - `dim_competition` — competition header/metadata. Key: `competition_id`, `external_id`.
    - `competition_stage` — competition stage rows (stage_one..stage_four). PK: `(competition_id, stage_num)`.
    - `bridge_competition_invited_team` — invited-teams bridge (competition_id, team_id).
    - `dim_registration_type`, `dim_classification` — lookups for registration/classification metadata.
    - `dim_range` — range/location metadata.
    - `dim_contact` — contact/person metadata for competitions.
    - `dim_state` — geographic state lookup used by range/team/contact.

## Visual Diagram (Mermaid)

```mermaid
flowchart TD
  subgraph RAW[Raw Tables]
    RS(raw_scoreboard)
    RT(raw_teams)
    RC(raw_competition)
  end

  subgraph SCRIPTS[Transform Scripts]
    SS(src/SilverScoreboardTransform.py)
    ST(src/SilverTeamTransform.py)
    SC(src/SilverCompetitionTransform.py)
  end

  subgraph SILVER[Silver Tables]
    ds(dim_scoreboard)
    dd(dim_discipline)
    da(dim_athlete)
    dt_legacy(dim_team\n(legacy ent_id))
    mr(map_roster)
    dstage(dim_stage)
    dseries(dim_series)
    fsr(fact_stage_series_result)
    fp(fact_penalty)
    fst(fact_stage_total)
    sp(silver_property)

    dt(dim_team\n(modern source/source_ent_id))
    fts(fact_team_snapshot)
    dp(dim_person)
    mtm(map_team_members)

    dc(dim_competition)
    cs(competition_stage)
    bct(bridge_competition_invited_team)
    drt(dim_registration_type)
    dcl(dim_classification)
    dr(dim_range)
    dct(dim_contact)
    ds_state(dim_state)
  end

  RS --> SS
  SS --> ds
  SS --> dd
  SS --> da
  SS --> dt_legacy
  SS --> mr
  SS --> dstage
  SS --> dseries
  SS --> fsr
  SS --> fp
  SS --> fst
  SS --> sp

  RT --> ST
  ST --> dt
  ST --> fts
  ST --> dp
  ST --> mtm

  RC --> SC
  SC --> dc
  SC --> cs
  SC --> bct
  SC --> drt
  SC --> dcl
  SC --> dr
  SC --> dct
  SC --> ds_state

  %% note about dt/dt_legacy
  dt_legacy --- dt
```

Note: not all Markdown renderers support Mermaid; if your renderer doesn't, I can generate a PNG/SVG and add it to `/doc`.

## ASCII Map — raw -> scripts -> silver

```
raw_scoreboard
  └─ src/SilverScoreboardTransform.py
     ├─ dim_scoreboard (scoreboard_id, external_id, raw_id)
     ├─ dim_discipline (discipline_id, code)
     ├─ dim_athlete (athlete_id, ath_id)
     ├─ dim_team (team_id, ent_id)  # legacy
     ├─ map_roster (scoreboard_id, athlete_id, discipline_id)
     ├─ dim_stage (stage_id, discipline_id, stage_num)
     ├─ dim_series (series_id, stage_id, series_num)
     ├─ fact_stage_series_result (scoreboard_id, athlete_id, stage_num, series_num)
     ├─ fact_penalty (scoreboard_id, athlete_id, stage_num, penalty_code)
     ├─ fact_stage_total (scoreboard_id, athlete_id, stage_num)
     └─ silver_property (scoreboard_id, entity_type, entity_id, prop_key)

raw_teams
  └─ src/SilverTeamTransform.py
     ├─ dim_team (team_id, source, source_ent_id)  # modern
     ├─ fact_team_snapshot (snapshot_id, team_id, raw_id, payload_hash)
     ├─ dim_person (person_id, source, source_ent_id)
     └─ map_team_members (team_id, person_id, role)

raw_competition
  └─ src/SilverCompetitionTransform.py
     ├─ dim_competition (competition_id, external_id)
     ├─ competition_stage (competition_id, stage_num)
     ├─ bridge_competition_invited_team (competition_id, team_id)
     ├─ dim_registration_type (registration_type_id, external_id)
     ├─ dim_classification (classification_id, external_id)
     ├─ dim_range (range_id)
     ├─ dim_contact (contact_id)
     └─ dim_state (state_id)
```

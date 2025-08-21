# Table mapping: Bronze (raw) → Silver / Dim / Fact / Bridge

This document describes how the raw (bronze) tables produced by the ingest
process are transformed into normalized silver tables, dimensions (dim_*),
facts (fact_*), maps and bridges. It is a human-readable outline used for
architecture and debugging.

## Overview (improved outline)

### 1) raw_scoreboard
- Produces:
  - `dim_team`
    - Note: upsert teams referenced in scoreboard payload
    - Example keys: `team.external_id`, `team.name`
  - `dim_athlete`
    - Note: upsert athletes found in rosters/results
    - Example keys: `athlete.external_id`, `athlete.name`
  - `dim_discipline`
    - Note: event/discipline metadata
    - Example keys: `discipline.code`
  - `dim_stage`
    - Note: stage-level metadata derived from match info
    - Example keys: `match_id`, `stage_index`
  - `athlete_scores`
    - Note: per-athlete per-stage/series scoring (normalized)
    - Example keys: `match_id`, `team_id`, `athlete_id`, `series_index`, `score`
  - `fact_stage_series_result`
    - Note: series-level results (one row per series/athlete/team)
    - Example keys: `match_id`, `series_index`, `athlete_id`, `team_id`
  - `fact_penalty`
    - Note: penalty events tied to stage/team/athlete
    - Example keys: `match_id`, `team_id`, `penalty_id`
  - `fact_stage_total`
    - Note: per-stage totals per team/athlete
    - Example keys: `match_id`, `team_id`, `total_score`
  - `silver_scoreboard`
    - Note: canonicalized raw JSON stored for audit + FK to raw_id
    - Example keys: `raw_id`, `source_hash`, `fetched_at_utc`


### 2) raw_schedule
- Produces:
  - `silver_schedule`
    - Note: canonicalized schedule JSON (one row per raw fetch)
    - Keys: `raw_id`, `source_hash`
  - `silver_schedule_slot`
    - Note: exploded slot/line entries from schedule (slot-level)
    - Example keys: `match_id`, `slot_index`, `slot_id`
  - `silver_slot_flight`
    - Note: flight-level details inside a slot (if present)
    - Example keys: `slot_id`, `flight_index`
  - `silver_slot_lineup`
    - Note: athlete lineup rows per slot (links to `dim_athlete`)
    - Example keys: `slot_id`, `athlete_id`, `lineup_position`
  - `dim_team`
    - Note: ensure teams referenced in schedule exist in team dim
    - Example keys: `team.external_id`, `team.name`
  - `dim_range`
    - Note: range/location dimension for schedule entries
    - Example keys: `range.external_id`, `range.name`
  - `dim_contact`
    - Note: organizer / contact info extracted from schedule pages
    - Example keys: `contact.email`, `contact.name`


### 3) raw_competition
- Produces:
  - `dim_competition`
    - Note: competition-level metadata (name, external id)
    - Example keys: `competition.external_id`, `competition.name`
  - `competition_stage`
    - Note: stages/phases of competition (per competition)
    - Example keys: `competition_id`, `stage_index`
  - `bridge_competition_invited_team`
    - Note: invited teams bridge table (many-to-many)
    - Example keys: `competition_id`, `team_id`
  - `dim_registration_type`
    - Note: registration type lookup
    - Example keys: `registration_type.code`
  - `dim_classification`
    - Note: classification metadata (e.g. divisions)
    - Example keys: `classification.code`
  - `dim_state`
    - Note: geographic/state dimension used by competition/team
    - Example keys: `state.code`
  - `silver_competition`
    - Note: canonical competition page JSON (audit + raw_id)
    - Keys: `raw_id`, `source_hash`


## Cross-source notes
- `dim_team` is created/upserted from both `raw_scoreboard` and `raw_schedule` (many-to-one). Use team external id to deduplicate.
- `dim_athlete` may be populated from both scoreboard and schedule payloads if schedule includes athlete info.
- `silver_*` tables store exploded, query-ready rows and include FK back to `raw_id` for traceability.

---

If you'd like I can:
- Add the transform script filename that creates each silver table (e.g., `src/SilverScoreboardTransform.py` → `dim_athlete`).
- Add exact field mappings and SQL join keys for each target table.
- Convert this doc to a CSV/JSON for external tools.


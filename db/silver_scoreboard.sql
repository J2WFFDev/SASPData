-- Silver schema for Scoreboard normalization (Postgres)
-- Run via src/silver.py which applies statements safely

-- dim_scoreboard
CREATE TABLE IF NOT EXISTS dim_scoreboard (
  scoreboard_id  BIGSERIAL PRIMARY KEY,
  external_id    TEXT UNIQUE,
  name           TEXT,
  generated_at   TIMESTAMPTZ,
  raw_id         BIGINT UNIQUE,
  ingested_at    TIMESTAMPTZ DEFAULT now()
);

-- dim_team
CREATE TABLE IF NOT EXISTS dim_team (
  team_id   BIGSERIAL PRIMARY KEY,
  ent_id    TEXT UNIQUE,
  team_name TEXT
);

-- dim_discipline
CREATE TABLE IF NOT EXISTS dim_discipline (
  discipline_id BIGSERIAL PRIMARY KEY,
  code          TEXT UNIQUE,
  label         TEXT
);

-- dim_athlete
CREATE TABLE IF NOT EXISTS dim_athlete (
  athlete_id BIGSERIAL PRIMARY KEY,
  ath_id     TEXT UNIQUE,
  fname      TEXT,
  lname      TEXT,
  gender     TEXT,
  bdate      DATE
);

-- roster mapping
CREATE TABLE IF NOT EXISTS map_roster (
  scoreboard_id BIGINT NOT NULL,
  athlete_id    BIGINT NOT NULL,
  team_id       BIGINT NOT NULL,
  discipline_id BIGINT NOT NULL,
  slot_id       TEXT NULL,
  class_id      TEXT NULL,
  eligible      BOOLEAN NULL,
  status        TEXT NULL,
  PRIMARY KEY (scoreboard_id, athlete_id, discipline_id)
);

-- dim_stage / dim_series (optional expressive tables)
CREATE TABLE IF NOT EXISTS dim_stage (
  stage_id     BIGSERIAL PRIMARY KEY,
  discipline_id BIGINT NOT NULL,
  stage_num    SMALLINT NOT NULL,
  stage_name   TEXT,
  UNIQUE (discipline_id, stage_num)
);

CREATE TABLE IF NOT EXISTS dim_series (
  series_id  BIGSERIAL PRIMARY KEY,
  stage_id   BIGINT NOT NULL,
  series_num SMALLINT NOT NULL,
  UNIQUE (stage_id, series_num)
);

-- fact_stage_series_result
CREATE TABLE IF NOT EXISTS fact_stage_series_result (
  scoreboard_id BIGINT NOT NULL,
  athlete_id    BIGINT NOT NULL,
  team_id       BIGINT NOT NULL,
  discipline_id BIGINT NOT NULL,
  stage_num     SMALLINT NOT NULL,
  series_num    SMALLINT NOT NULL,
  value         NUMERIC NULL,
  payload       JSONB NULL,
  inserted_at   TIMESTAMPTZ DEFAULT now()
  ,PRIMARY KEY (scoreboard_id, athlete_id, stage_num, series_num)
);

-- fact_penalty
CREATE TABLE IF NOT EXISTS fact_penalty (
  scoreboard_id BIGINT NOT NULL,
  athlete_id    BIGINT NOT NULL,
  stage_num     SMALLINT NOT NULL,
  series_num    SMALLINT NULL,
  penalty_code  TEXT NOT NULL,
  penalty_value NUMERIC NOT NULL,
  inserted_at   TIMESTAMPTZ DEFAULT now()
  -- primary key with expression is invalid in Postgres; create a unique index below instead
  -- keep table definition minimal here
);

-- Enforce intended uniqueness: (scoreboard_id, athlete_id, stage_num, penalty_code, COALESCE(series_num,0))
CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_penalty_unique ON fact_penalty (
  scoreboard_id, athlete_id, stage_num, penalty_code, (COALESCE(series_num,0))
);

-- fact_stage_total
CREATE TABLE IF NOT EXISTS fact_stage_total (
  scoreboard_id BIGINT NOT NULL,
  athlete_id    BIGINT NOT NULL,
  stage_num     SMALLINT NOT NULL,
  raw_time      NUMERIC NULL,
  penalties     NUMERIC NULL,
  final_time    NUMERIC NULL,
  PRIMARY KEY (scoreboard_id, athlete_id, stage_num)
);

-- silver_property
CREATE TABLE IF NOT EXISTS silver_property (
  scoreboard_id BIGINT NOT NULL,
  entity_type   TEXT NOT NULL CHECK (entity_type IN ('athlete','team','scoreboard')),
  entity_id     BIGINT NOT NULL,
  prop_key      TEXT NOT NULL,
  prop_value    JSONB NOT NULL,
  PRIMARY KEY (scoreboard_id, entity_type, entity_id, prop_key)
);

-- Indexes
CREATE INDEX IF NOT EXISTS ix_fact_result_discipline_stage ON fact_stage_series_result (discipline_id, stage_num, value);
CREATE INDEX IF NOT EXISTS ix_map_roster_scoreboard_team ON map_roster (scoreboard_id, team_id);

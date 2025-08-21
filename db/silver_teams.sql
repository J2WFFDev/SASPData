-- Silver schema for teams
-- dim_team: canonical team entity
CREATE TABLE IF NOT EXISTS dim_team (
  team_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_ent_id TEXT NOT NULL,
  team_name TEXT,
  display_name TEXT,
  short_name TEXT,
  country TEXT,
  state TEXT,
  city TEXT,
  metadata JSONB,
  first_seen TIMESTAMPTZ DEFAULT now(),
  last_seen TIMESTAMPTZ DEFAULT now(),
  active BOOLEAN DEFAULT TRUE,
  UNIQUE (source, source_ent_id)
);
CREATE INDEX IF NOT EXISTS idx_dim_team_name ON dim_team (team_name);
CREATE INDEX IF NOT EXISTS idx_dim_team_source ON dim_team (source, source_ent_id);

-- fact_team_snapshot: snapshot of raw payloads mapped to a team
CREATE TABLE IF NOT EXISTS fact_team_snapshot (
  snapshot_id BIGSERIAL PRIMARY KEY,
  team_id BIGINT REFERENCES dim_team(team_id) ON DELETE SET NULL,
  raw_id BIGINT REFERENCES raw_teams(id) ON DELETE SET NULL,
  snapshot_at TIMESTAMPTZ DEFAULT now(),
  payload JSONB,
  payload_hash TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshot_team ON fact_team_snapshot(team_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_raw ON fact_team_snapshot(raw_id);

-- dim_person: optional normalization for athletes/contacts
CREATE TABLE IF NOT EXISTS dim_person (
  person_id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  source_ent_id TEXT NOT NULL,
  given_name TEXT,
  family_name TEXT,
  full_name TEXT,
  dob DATE,
  country TEXT,
  metadata JSONB,
  UNIQUE (source, source_ent_id)
);

-- map_team_members: mapping persons to teams (role: captain/player/coach)
CREATE TABLE IF NOT EXISTS map_team_members (
  team_id BIGINT REFERENCES dim_team(team_id) ON DELETE CASCADE,
  person_id BIGINT REFERENCES dim_person(person_id) ON DELETE CASCADE,
  role TEXT,
  source_raw_id BIGINT,
  added_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (team_id, person_id, role)
);

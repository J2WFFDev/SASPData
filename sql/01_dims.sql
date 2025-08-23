-- Silver Layer Dimension Tables for SASP Data Warehouse
-- PostgreSQL DDL adapted from MySQL design
-- Creates normalized dimension tables from raw JSON data

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Dimension: Competition
CREATE TABLE IF NOT EXISTS dim_competition (
  competition_key BIGSERIAL PRIMARY KEY,
  competition_id_nat INTEGER UNIQUE NOT NULL,
  name VARCHAR(255),
  org VARCHAR(32),
  type VARCHAR(8),
  status VARCHAR(8),
  shooting_style VARCHAR(32),
  stage_one VARCHAR(64),
  stage_two VARCHAR(64),
  stage_three VARCHAR(64),
  stage_four VARCHAR(64),
  start_date DATE,
  end_date DATE,
  open_date DATE,
  close_date DATE,
  hosting_team_id_nat INTEGER,
  range_id_nat INTEGER,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_competition_range ON dim_competition(range_id_nat);
CREATE INDEX IF NOT EXISTS idx_dim_competition_host_team ON dim_competition(hosting_team_id_nat);

-- Dimension: Range
CREATE TABLE IF NOT EXISTS dim_range (
  range_key BIGSERIAL PRIMARY KEY,
  range_id_nat INTEGER UNIQUE NOT NULL,
  name VARCHAR(255),
  type_id INTEGER,
  contact VARCHAR(120),
  phone VARCHAR(40),
  email VARCHAR(120),
  url VARCHAR(255),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Dimension: Team
CREATE TABLE IF NOT EXISTS dim_team (
  team_key BIGSERIAL PRIMARY KEY,
  team_id_nat INTEGER UNIQUE NOT NULL,
  name VARCHAR(255),
  org VARCHAR(32),
  paper_name VARCHAR(255),
  paper_email VARCHAR(255),
  state_id_nat INTEGER,
  home_range_id_nat INTEGER,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_team_range ON dim_team(home_range_id_nat);
CREATE INDEX IF NOT EXISTS idx_dim_team_state ON dim_team(state_id_nat);

-- Dimension: Athlete
CREATE TABLE IF NOT EXISTS dim_athlete (
  athlete_key BIGSERIAL PRIMARY KEY,
  ath_id_nat INTEGER UNIQUE NOT NULL,
  fname VARCHAR(80),
  lname VARCHAR(80),
  gender VARCHAR(12),
  bdate DATE,
  address VARCHAR(255),
  city VARCHAR(120),
  state_id_nat INTEGER,
  zip VARCHAR(15),
  phone VARCHAR(40),
  email VARCHAR(120),
  email2 VARCHAR(120),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_athlete_state ON dim_athlete(state_id_nat);
CREATE INDEX IF NOT EXISTS idx_dim_athlete_name ON dim_athlete(lname, fname);

-- Dimension: Discipline  
CREATE TABLE IF NOT EXISTS dim_discipline (
  discipline_key BIGSERIAL PRIMARY KEY,
  discipline_id_nat INTEGER UNIQUE NOT NULL,
  name VARCHAR(64),
  competition_id_nat INTEGER,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_discipline_comp ON dim_discipline(competition_id_nat);

-- Dimension: Slot (Schedule slots)
CREATE TABLE IF NOT EXISTS dim_slot (
  slot_key BIGSERIAL PRIMARY KEY,
  slot_rid_nat BIGINT UNIQUE NOT NULL,
  number INTEGER,
  name VARCHAR(255),
  stage VARCHAR(64),
  discipline_name VARCHAR(64),
  location_name VARCHAR(255),
  range_name VARCHAR(255),
  expanded BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_slot_discipline ON dim_slot(discipline_name);
CREATE INDEX IF NOT EXISTS idx_dim_slot_stage ON dim_slot(stage);

-- Dimension: Date (for time intelligence)
CREATE TABLE IF NOT EXISTS dim_date (
  date_key INTEGER PRIMARY KEY,  -- YYYYMMDD format
  full_date DATE UNIQUE NOT NULL,
  year INTEGER,
  month INTEGER,
  day INTEGER,
  dow INTEGER,  -- Day of week (0=Sunday)
  week_of_year INTEGER,
  is_weekend BOOLEAN DEFAULT FALSE
);

-- Dimension: Time (for time intelligence) 
CREATE TABLE IF NOT EXISTS dim_time (
  time_key INTEGER PRIMARY KEY,  -- HHMM format
  hour INTEGER,
  minute INTEGER,
  am_pm VARCHAR(2)
);

-- Bridge: Team-Athlete relationships
CREATE TABLE IF NOT EXISTS bridge_team_athlete (
  bridge_id BIGSERIAL PRIMARY KEY,
  team_key BIGINT REFERENCES dim_team(team_key),
  athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
  competition_key BIGINT REFERENCES dim_competition(competition_key),
  from_date DATE,
  thru_date DATE,
  role VARCHAR(64),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bridge_team_athlete_team ON bridge_team_athlete(team_key);
CREATE INDEX IF NOT EXISTS idx_bridge_team_athlete_athlete ON bridge_team_athlete(athlete_key);
CREATE INDEX IF NOT EXISTS idx_bridge_team_athlete_comp ON bridge_team_athlete(competition_key);

-- Comments for documentation
COMMENT ON TABLE dim_competition IS 'Competition dimension - normalized from raw_competition JSON';
COMMENT ON TABLE dim_team IS 'Team dimension - normalized from raw_teams JSON';  
COMMENT ON TABLE dim_athlete IS 'Athlete dimension - extracted from scoreboard entries';
COMMENT ON TABLE dim_discipline IS 'Discipline dimension - shooting disciplines within competitions';
COMMENT ON TABLE dim_slot IS 'Slot dimension - schedule slots from raw_schedule JSON';
COMMENT ON TABLE bridge_team_athlete IS 'Many-to-many relationship between teams and athletes';
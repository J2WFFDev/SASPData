-- Silver Layer Fact Tables for SASP Data Warehouse
-- PostgreSQL DDL for fact tables that reference dimension keys

-- Fact: Entry (main scoreboard entries)
CREATE TABLE IF NOT EXISTS fact_entry (
  entry_id BIGSERIAL PRIMARY KEY,
  competition_key BIGINT REFERENCES dim_competition(competition_key),
  team_key BIGINT REFERENCES dim_team(team_key),
  discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
  athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
  slot_key BIGINT REFERENCES dim_slot(slot_key),
  
  -- Original scoreboard fields
  station INTEGER,
  number INTEGER,
  lid INTEGER,
  date_key INTEGER REFERENCES dim_date(date_key),
  time_key INTEGER REFERENCES dim_time(time_key),
  location VARCHAR(64),
  flight VARCHAR(64),
  
  -- Scoring and status flags
  manual_scoring BOOLEAN DEFAULT FALSE,
  is_valid BOOLEAN DEFAULT TRUE,
  eligible BOOLEAN DEFAULT TRUE,
  dq_tag BOOLEAN DEFAULT FALSE,
  dnf_tag BOOLEAN DEFAULT FALSE,
  
  -- Penalties and final scores
  proc_pen NUMERIC(10,3),
  spp_final NUMERIC(10,3),
  
  -- Registration tracking
  reg_date_key INTEGER REFERENCES dim_date(date_key),
  reg_who VARCHAR(120),
  
  -- Metadata
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Indexes for fact_entry performance
CREATE INDEX IF NOT EXISTS idx_fact_entry_comp_disc ON fact_entry(competition_key, discipline_key);
CREATE INDEX IF NOT EXISTS idx_fact_entry_team ON fact_entry(team_key);
CREATE INDEX IF NOT EXISTS idx_fact_entry_athlete ON fact_entry(athlete_key);
CREATE INDEX IF NOT EXISTS idx_fact_entry_slot ON fact_entry(slot_key);
CREATE INDEX IF NOT EXISTS idx_fact_entry_valid_score ON fact_entry(is_valid, dq_tag, dnf_tag, spp_final);
CREATE INDEX IF NOT EXISTS idx_fact_entry_date ON fact_entry(date_key);

-- Fact: Entry Strings (detailed stage/string scoring)
CREATE TABLE IF NOT EXISTS fact_entry_strings (
  entry_string_id BIGSERIAL PRIMARY KEY,
  entry_id BIGINT REFERENCES fact_entry(entry_id) ON DELETE CASCADE,
  stage_no SMALLINT CHECK (stage_no BETWEEN 1 AND 4),
  string_no SMALLINT CHECK (string_no BETWEEN 1 AND 5),
  
  -- Performance metrics
  time_value NUMERIC(10,3),
  penalty_value NUMERIC(10,3),
  total_value NUMERIC(10,3),
  
  -- Metadata
  created_at TIMESTAMPTZ DEFAULT now(),
  
  UNIQUE(entry_id, stage_no, string_no)
);

CREATE INDEX IF NOT EXISTS idx_fact_entry_strings_entry ON fact_entry_strings(entry_id);
CREATE INDEX IF NOT EXISTS idx_fact_entry_strings_stage ON fact_entry_strings(stage_no, string_no);

-- Fact: Schedule (lineup and flight information)
CREATE TABLE IF NOT EXISTS fact_schedule (
  schedule_id BIGSERIAL PRIMARY KEY,
  competition_key BIGINT REFERENCES dim_competition(competition_key),
  slot_key BIGINT REFERENCES dim_slot(slot_key),
  
  -- Schedule-specific identifiers
  lineup_id BIGINT,
  station INTEGER,
  
  -- Participant information (denormalized for performance)
  athlete_name VARCHAR(255),
  team_name VARCHAR(255),
  class_label VARCHAR(128),
  
  -- Status flags
  is_open BOOLEAN DEFAULT FALSE,
  exists_flag BOOLEAN DEFAULT TRUE,
  
  -- Metadata
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_schedule_comp_slot ON fact_schedule(competition_key, slot_key);
CREATE INDEX IF NOT EXISTS idx_fact_schedule_lineup ON fact_schedule(lineup_id);

-- Aggregate table: Team performance summary
CREATE TABLE IF NOT EXISTS agg_team_performance (
  agg_id BIGSERIAL PRIMARY KEY,
  competition_key BIGINT REFERENCES dim_competition(competition_key),
  team_key BIGINT REFERENCES dim_team(team_key),
  discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
  
  -- Aggregated metrics
  total_athletes INTEGER,
  valid_entries INTEGER,
  dq_count INTEGER,
  dnf_count INTEGER,
  avg_score NUMERIC(10,3),
  best_score NUMERIC(10,3),
  worst_score NUMERIC(10,3),
  total_penalties NUMERIC(10,3),
  
  -- Calculated at
  calculated_at TIMESTAMPTZ DEFAULT now(),
  
  UNIQUE(competition_key, team_key, discipline_key)
);

CREATE INDEX IF NOT EXISTS idx_agg_team_perf_comp ON agg_team_performance(competition_key);
CREATE INDEX IF NOT EXISTS idx_agg_team_perf_team ON agg_team_performance(team_key);

-- View: Complete entry details with dimension names
CREATE OR REPLACE VIEW v_entry_details AS
SELECT 
  fe.entry_id,
  dc.name AS competition_name,
  dt.name AS team_name,
  da.fname || ' ' || da.lname AS athlete_name,
  dd.name AS discipline_name,
  ds.name AS slot_name,
  fe.station,
  fe.spp_final,
  fe.is_valid,
  fe.dq_tag,
  fe.dnf_tag,
  fe.proc_pen,
  fe.location,
  fe.flight
FROM fact_entry fe
LEFT JOIN dim_competition dc ON fe.competition_key = dc.competition_key
LEFT JOIN dim_team dt ON fe.team_key = dt.team_key  
LEFT JOIN dim_athlete da ON fe.athlete_key = da.athlete_key
LEFT JOIN dim_discipline dd ON fe.discipline_key = dd.discipline_key
LEFT JOIN dim_slot ds ON fe.slot_key = ds.slot_key;

-- Comments for documentation
COMMENT ON TABLE fact_entry IS 'Main fact table for scoreboard entries with foreign keys to dimensions';
COMMENT ON TABLE fact_entry_strings IS 'Detailed stage/string performance data for each entry';
COMMENT ON TABLE fact_schedule IS 'Schedule and lineup fact table from raw_schedule data';
COMMENT ON TABLE agg_team_performance IS 'Pre-aggregated team performance metrics by competition and discipline';
COMMENT ON VIEW v_entry_details IS 'Denormalized view joining entry facts with dimension names for reporting';
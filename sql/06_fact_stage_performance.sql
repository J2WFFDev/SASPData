-- fact_stage_performance: Aggregated string performance data by stage
-- This implements the core analytics requirement: "keep 4 of the 5 fastest string times, dropping the slowest"

DROP TABLE IF EXISTS fact_stage_performance CASCADE;

CREATE TABLE fact_stage_performance (
    stage_performance_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    entry_id BIGINT NOT NULL REFERENCES fact_entry(entry_id),
    stage_key INTEGER NOT NULL REFERENCES dim_stage(stage_key),
    competition_key BIGINT NOT NULL REFERENCES dim_competition(competition_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    athlete_key BIGINT NOT NULL REFERENCES dim_athlete(athlete_key),
    discipline_key BIGINT NOT NULL REFERENCES dim_discipline(discipline_key),
    slot_key BIGINT NOT NULL REFERENCES dim_slot(slot_key),
    
    -- Stage Performance Aggregates (4 fastest strings, slowest dropped)
    total_raw_time DECIMAL(8,3),          -- Sum of 4 fastest raw times
    total_total_time DECIMAL(8,3),        -- Sum of 4 fastest total times  
    total_penalties DECIMAL(8,3),         -- Sum of 4 fastest penalty times
    strings_count SMALLINT DEFAULT 4,     -- Always 4 (after dropping slowest)
    dropped_string_number SMALLINT,       -- Which string was dropped (1-5)
    
    -- Individual String Performance (4 fastest only)
    string1_raw DECIMAL(8,3),
    string1_total DECIMAL(8,3), 
    string1_penalties DECIMAL(8,3),
    string2_raw DECIMAL(8,3),
    string2_total DECIMAL(8,3),
    string2_penalties DECIMAL(8,3), 
    string3_raw DECIMAL(8,3),
    string3_total DECIMAL(8,3),
    string3_penalties DECIMAL(8,3),
    string4_raw DECIMAL(8,3),
    string4_total DECIMAL(8,3),
    string4_penalties DECIMAL(8,3),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(entry_id, stage_key),
    CHECK (strings_count <= 4),
    CHECK (dropped_string_number IS NULL OR dropped_string_number BETWEEN 1 AND 5)
);

-- Indexes for performance
CREATE INDEX idx_stage_perf_entry ON fact_stage_performance(entry_id);
CREATE INDEX idx_stage_perf_stage ON fact_stage_performance(stage_key); 
CREATE INDEX idx_stage_perf_comp ON fact_stage_performance(competition_key);
CREATE INDEX idx_stage_perf_team ON fact_stage_performance(team_key);
CREATE INDEX idx_stage_perf_athlete ON fact_stage_performance(athlete_key);

-- View for easy querying
CREATE OR REPLACE VIEW v_stage_performance AS
SELECT 
    sp.stage_performance_key,
    sp.entry_id,
    
    -- Dimension attributes
    c.name as competition_name,
    c.start_date as competition_date,
    ds.stage_name_standard as stage_name,
    ds.stage_short_code,
    ds.stage_type,
    t.name as team_name,
    t.paper_name as team_short_name,
    a.fname as first_name,
    a.lname as last_name,
    a.fname || ' ' || a.lname as athlete_name,
    d.name as discipline_name,
    sl.number as slot_number,
    
    -- Aggregated performance
    sp.total_raw_time,
    sp.total_total_time,
    sp.total_penalties,
    sp.strings_count,
    sp.dropped_string_number,
    
    -- Performance metrics
    ROUND(sp.total_raw_time / 4, 3) as avg_raw_time,
    ROUND(sp.total_total_time / 4, 3) as avg_total_time,
    ROUND(sp.total_penalties / 4, 3) as avg_penalty_time,
    
    sp.created_at,
    sp.updated_at

FROM fact_stage_performance sp
JOIN fact_entry fe ON sp.entry_id = fe.entry_id
JOIN dim_competition c ON sp.competition_key = c.competition_key
JOIN dim_stage ds ON sp.stage_key = ds.stage_key
JOIN dim_team t ON sp.team_key = t.team_key
JOIN dim_athlete a ON sp.athlete_key = a.athlete_key
JOIN dim_discipline d ON sp.discipline_key = d.discipline_key
JOIN dim_slot sl ON sp.slot_key = sl.slot_key;

COMMENT ON TABLE fact_stage_performance IS 'Aggregated string performance by stage - keeps 4 fastest strings, drops slowest';
COMMENT ON COLUMN fact_stage_performance.dropped_string_number IS 'Which string (1-5) was dropped as the slowest';
COMMENT ON COLUMN fact_stage_performance.strings_count IS 'Always 4 - number of strings kept after dropping slowest';
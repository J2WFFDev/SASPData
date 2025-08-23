-- fact_match_performance: Aggregated stage performance data by match
-- This implements: "total string data for all 4 stages as match data"

DROP TABLE IF EXISTS fact_match_performance CASCADE;

CREATE TABLE fact_match_performance (
    match_performance_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    entry_id BIGINT NOT NULL REFERENCES fact_entry(entry_id),
    competition_key BIGINT NOT NULL REFERENCES dim_competition(competition_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    athlete_key BIGINT NOT NULL REFERENCES dim_athlete(athlete_key),
    discipline_key BIGINT NOT NULL REFERENCES dim_discipline(discipline_key),
    slot_key BIGINT NOT NULL REFERENCES dim_slot(slot_key),
    classification_key INTEGER REFERENCES dim_classification(classification_key),
    
    -- Match Performance Aggregates (sum of all 4 stages)
    total_raw_time DECIMAL(8,3),          -- Sum of all stage raw times
    total_total_time DECIMAL(8,3),        -- Sum of all stage total times
    total_penalties DECIMAL(8,3),         -- Sum of all stage penalty times
    stages_count SMALLINT DEFAULT 4,      -- Number of stages (should be 4)
    
    -- Individual Stage Performance (totals from fact_stage_performance)
    stage1_raw DECIMAL(8,3),              -- GoFast stage totals
    stage1_total DECIMAL(8,3),
    stage1_penalties DECIMAL(8,3),
    stage2_raw DECIMAL(8,3),              -- Focus stage totals
    stage2_total DECIMAL(8,3),
    stage2_penalties DECIMAL(8,3),
    stage3_raw DECIMAL(8,3),              -- SpeedTrap stage totals  
    stage3_total DECIMAL(8,3),
    stage3_penalties DECIMAL(8,3),
    stage4_raw DECIMAL(8,3),              -- InOut stage totals
    stage4_total DECIMAL(8,3),
    stage4_penalties DECIMAL(8,3),
    
    -- Classification information
    division_name VARCHAR(50),             -- From dim_classification
    class_name VARCHAR(50),                -- From dim_classification
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(entry_id),                     -- One match record per entry
    CHECK (stages_count = 4)
);

-- Indexes for performance
CREATE INDEX idx_match_perf_entry ON fact_match_performance(entry_id);
CREATE INDEX idx_match_perf_comp ON fact_match_performance(competition_key);
CREATE INDEX idx_match_perf_team ON fact_match_performance(team_key);
CREATE INDEX idx_match_perf_athlete ON fact_match_performance(athlete_key);
CREATE INDEX idx_match_perf_classification ON fact_match_performance(classification_key);
CREATE INDEX idx_match_perf_division ON fact_match_performance(division_name);

-- View for easy querying
CREATE OR REPLACE VIEW v_match_performance AS
SELECT 
    mp.match_performance_key,
    mp.entry_id,
    
    -- Dimension attributes
    c.name as competition_name,
    c.start_date as competition_date,
    c.hosting_team_id_nat as location,
    t.name as team_name,
    t.paper_name as team_short_name,
    a.fname as first_name,
    a.lname as last_name,
    a.fname || ' ' || a.lname as athlete_name,
    d.name as discipline_name,
    sl.number as slot_number,
    
    -- Classification
    mp.division_name,
    mp.class_name,
    dc.allows_ghost_athletes,
    dc.is_open_division,
    
    -- Match performance totals
    mp.total_raw_time,
    mp.total_total_time,
    mp.total_penalties,
    mp.stages_count,
    
    -- Stage breakdown
    mp.stage1_raw as gofast_raw,
    mp.stage1_total as gofast_total,
    mp.stage1_penalties as gofast_penalties,
    mp.stage2_raw as focus_raw,
    mp.stage2_total as focus_total,
    mp.stage2_penalties as focus_penalties,
    mp.stage3_raw as speedtrap_raw,
    mp.stage3_total as speedtrap_total,
    mp.stage3_penalties as speedtrap_penalties,
    mp.stage4_raw as inout_raw,
    mp.stage4_total as inout_total,
    mp.stage4_penalties as inout_penalties,
    
    -- Performance metrics
    ROUND(mp.total_raw_time / 4, 3) as avg_stage_raw_time,
    ROUND(mp.total_total_time / 4, 3) as avg_stage_total_time,
    ROUND(mp.total_penalties / 4, 3) as avg_stage_penalty_time,
    
    mp.created_at,
    mp.updated_at

FROM fact_match_performance mp
JOIN fact_entry fe ON mp.entry_id = fe.entry_id
JOIN dim_competition c ON mp.competition_key = c.competition_key
JOIN dim_team t ON mp.team_key = t.team_key
JOIN dim_athlete a ON mp.athlete_key = a.athlete_key
JOIN dim_discipline d ON mp.discipline_key = d.discipline_key
JOIN dim_slot sl ON mp.slot_key = sl.slot_key
LEFT JOIN dim_classification dc ON mp.classification_key = dc.classification_key;

COMMENT ON TABLE fact_match_performance IS 'Aggregated stage performance by match - sum of all 4 stages per athlete';
COMMENT ON COLUMN fact_match_performance.stages_count IS 'Always 4 - number of stages in a complete match';
COMMENT ON COLUMN fact_match_performance.classification_key IS 'Links to athlete classification/division';
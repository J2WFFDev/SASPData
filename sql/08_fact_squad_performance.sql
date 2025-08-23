-- fact_squad_performance: Aggregated match performance data by squad  
-- This implements: "total match data for all 4 members of the same squad for squad data"

DROP TABLE IF EXISTS fact_squad_performance CASCADE;

CREATE TABLE fact_squad_performance (
    squad_performance_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    competition_key BIGINT NOT NULL REFERENCES dim_competition(competition_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    discipline_key BIGINT NOT NULL REFERENCES dim_discipline(discipline_key),
    classification_key INTEGER REFERENCES dim_classification(classification_key),
    
    -- Squad identification
    squad_name VARCHAR(100),               -- Team name + discipline for display
    
    -- Squad Performance Aggregates (sum of all 4 team members)
    total_raw_time DECIMAL(8,3),          -- Sum of all member match raw times
    total_total_time DECIMAL(8,3),        -- Sum of all member match total times  
    total_penalties DECIMAL(8,3),         -- Sum of all member match penalty times
    members_count SMALLINT DEFAULT 4,     -- Number of team members (should be 4)
    
    -- Individual Member Performance (from fact_match_performance)
    member1_entry_id BIGINT REFERENCES fact_entry(entry_id),
    member1_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member1_slot_key BIGINT REFERENCES dim_slot(slot_key),
    member1_raw DECIMAL(8,3),
    member1_total DECIMAL(8,3),
    member1_penalties DECIMAL(8,3),
    
    member2_entry_id BIGINT REFERENCES fact_entry(entry_id),
    member2_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member2_slot_key BIGINT REFERENCES dim_slot(slot_key),
    member2_raw DECIMAL(8,3),
    member2_total DECIMAL(8,3),
    member2_penalties DECIMAL(8,3),
    
    member3_entry_id BIGINT REFERENCES fact_entry(entry_id),
    member3_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member3_slot_key BIGINT REFERENCES dim_slot(slot_key),
    member3_raw DECIMAL(8,3),
    member3_total DECIMAL(8,3),
    member3_penalties DECIMAL(8,3),
    
    member4_entry_id BIGINT REFERENCES fact_entry(entry_id),
    member4_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member4_slot_key BIGINT REFERENCES dim_slot(slot_key),
    member4_raw DECIMAL(8,3),
    member4_total DECIMAL(8,3),
    member4_penalties DECIMAL(8,3),
    
    -- Classification information
    division_name VARCHAR(50),             -- Squad's division (determined by get_squad_division)
    is_mixed_division BOOLEAN DEFAULT false, -- True if members from different divisions
    has_ghost_athletes BOOLEAN DEFAULT false, -- True if any member is a ghost athlete
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(competition_key, team_key, discipline_key), -- One squad per team/discipline/comp
    CHECK (members_count <= 4),
    CHECK (members_count > 0)
);

-- Indexes for performance
CREATE INDEX idx_squad_perf_comp ON fact_squad_performance(competition_key);
CREATE INDEX idx_squad_perf_team ON fact_squad_performance(team_key);
CREATE INDEX idx_squad_perf_discipline ON fact_squad_performance(discipline_key);
CREATE INDEX idx_squad_perf_classification ON fact_squad_performance(classification_key);
CREATE INDEX idx_squad_perf_division ON fact_squad_performance(division_name);

-- View for easy querying with member details
CREATE OR REPLACE VIEW v_squad_performance AS
SELECT 
    sp.squad_performance_key,
    sp.squad_name,
    
    -- Dimension attributes
    c.name as competition_name,
    c.start_date as competition_date,
    c.hosting_team_id_nat as location,
    t.name as team_name,
    t.paper_name as team_short_name,
    d.name as discipline_name,
    
    -- Classification
    sp.division_name,
    sp.is_mixed_division,
    sp.has_ghost_athletes,
    dc.allows_ghost_athletes as division_allows_ghosts,
    dc.is_open_division,
    
    -- Squad performance totals
    sp.total_raw_time,
    sp.total_total_time,
    sp.total_penalties,
    sp.members_count,
    
    -- Member details
    a1.fname || ' ' || a1.lname as member1_name,
    sl1.number as member1_slot,
    sp.member1_raw,
    sp.member1_total,
    sp.member1_penalties,
    
    a2.fname || ' ' || a2.lname as member2_name,
    sl2.number as member2_slot,
    sp.member2_raw,
    sp.member2_total,
    sp.member2_penalties,
    
    a3.fname || ' ' || a3.lname as member3_name,
    sl3.number as member3_slot,
    sp.member3_raw,
    sp.member3_total,
    sp.member3_penalties,
    
    a4.fname || ' ' || a4.lname as member4_name,
    sl4.number as member4_slot,
    sp.member4_raw,
    sp.member4_total,
    sp.member4_penalties,
    
    -- Performance metrics
    ROUND(sp.total_raw_time / sp.members_count, 3) as avg_member_raw_time,
    ROUND(sp.total_total_time / sp.members_count, 3) as avg_member_total_time,
    ROUND(sp.total_penalties / sp.members_count, 3) as avg_member_penalty_time,
    
    sp.created_at,
    sp.updated_at

FROM fact_squad_performance sp
JOIN dim_competition c ON sp.competition_key = c.competition_key
JOIN dim_team t ON sp.team_key = t.team_key
JOIN dim_discipline d ON sp.discipline_key = d.discipline_key
LEFT JOIN dim_classification dc ON sp.classification_key = dc.classification_key

-- Member joins
LEFT JOIN dim_athlete a1 ON sp.member1_athlete_key = a1.athlete_key
LEFT JOIN dim_slot sl1 ON sp.member1_slot_key = sl1.slot_key
LEFT JOIN dim_athlete a2 ON sp.member2_athlete_key = a2.athlete_key
LEFT JOIN dim_slot sl2 ON sp.member2_slot_key = sl2.slot_key
LEFT JOIN dim_athlete a3 ON sp.member3_athlete_key = a3.athlete_key
LEFT JOIN dim_slot sl3 ON sp.member3_slot_key = sl3.slot_key
LEFT JOIN dim_athlete a4 ON sp.member4_athlete_key = a4.athlete_key
LEFT JOIN dim_slot sl4 ON sp.member4_slot_key = sl4.slot_key;

COMMENT ON TABLE fact_squad_performance IS 'Aggregated match performance by squad - sum of all 4 team members';
COMMENT ON COLUMN fact_squad_performance.squad_name IS 'Team name + discipline for display purposes';
COMMENT ON COLUMN fact_squad_performance.division_name IS 'Squad division determined by get_squad_division function';
COMMENT ON COLUMN fact_squad_performance.is_mixed_division IS 'True if squad members are from different divisions';
-- rankings_individual: HOA (High Overall Aggregate) individual awards
-- Purpose: Rankings by class + gender + discipline for individual athletes

DROP TABLE IF EXISTS rankings_individual CASCADE;

CREATE TABLE rankings_individual (
    ranking_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    competition_key BIGINT NOT NULL REFERENCES dim_competition(competition_key),
    athlete_key BIGINT NOT NULL REFERENCES dim_athlete(athlete_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    discipline_key BIGINT NOT NULL REFERENCES dim_discipline(discipline_key),
    classification_key INTEGER REFERENCES dim_classification(classification_key),
    
    -- Ranking Category (class + gender + discipline)
    ranking_category VARCHAR(100) NOT NULL,  -- e.g., "Rookie Men Iron Rifle"
    class_name VARCHAR(50),                  -- Rookie, Intermediate, Senior, etc.
    gender VARCHAR(10),                      -- Men, Women
    discipline_name VARCHAR(50),             -- Iron Rifle, Optic Pistol, etc.
    
    -- Performance Data
    total_time DECIMAL(8,3) NOT NULL,        -- Complete match time
    raw_time DECIMAL(8,3),                   -- Raw time without penalties
    penalty_time DECIMAL(8,3),               -- Total penalties
    
    -- Ranking Position
    overall_rank INTEGER NOT NULL,           -- 1st, 2nd, 3rd, etc. within category
    total_competitors INTEGER NOT NULL,      -- Total athletes in this category
    percentile DECIMAL(5,2),                 -- Performance percentile (0-100)
    
    -- Award Information
    award_level VARCHAR(20),                 -- "1st Place", "2nd Place", "3rd Place", etc.
    is_hoa_winner BOOLEAN DEFAULT false,     -- High Overall Aggregate winner
    
    -- Validation Flags
    is_complete_match BOOLEAN DEFAULT true,  -- All 4 stages completed
    is_valid_time BOOLEAN DEFAULT true,      -- Time is valid (not DNF/DQ)
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(competition_key, athlete_key, discipline_key),  -- One ranking per athlete per discipline per competition
    CHECK (overall_rank > 0),
    CHECK (total_competitors > 0),
    CHECK (percentile >= 0 AND percentile <= 100),
    CHECK (total_time > 0)
);

-- Indexes for performance
CREATE INDEX idx_rankings_individual_competition ON rankings_individual(competition_key);
CREATE INDEX idx_rankings_individual_athlete ON rankings_individual(athlete_key);
CREATE INDEX idx_rankings_individual_category ON rankings_individual(ranking_category);
CREATE INDEX idx_rankings_individual_rank ON rankings_individual(overall_rank);
CREATE INDEX idx_rankings_individual_class_gender_disc ON rankings_individual(class_name, gender, discipline_name);

-- View for easy querying
CREATE OR REPLACE VIEW v_individual_rankings AS
SELECT 
    ri.ranking_key,
    
    -- Competition info
    c.name as competition_name,
    c.start_date as competition_date,
    
    -- Athlete info
    a.fname as first_name,
    a.lname as last_name,
    CONCAT(a.fname, ' ', a.lname) as full_name,
    
    -- Team info
    t.name as team_name,
    
    -- Ranking info
    ri.ranking_category,
    ri.class_name,
    ri.gender,
    ri.discipline_name,
    ri.overall_rank,
    ri.total_competitors,
    ri.award_level,
    ri.is_hoa_winner,
    
    -- Performance
    ri.total_time,
    ri.raw_time,
    ri.penalty_time,
    ri.percentile,
    
    -- Validation
    ri.is_complete_match,
    ri.is_valid_time,
    
    ri.created_at

FROM rankings_individual ri
JOIN dim_competition c ON ri.competition_key = c.competition_key
JOIN dim_athlete a ON ri.athlete_key = a.athlete_key
JOIN dim_team t ON ri.team_key = t.team_key
JOIN dim_discipline d ON ri.discipline_key = d.discipline_key
LEFT JOIN dim_classification dc ON ri.classification_key = dc.classification_key;

COMMENT ON TABLE rankings_individual IS 'Individual athlete rankings by class + gender + discipline for HOA awards';
COMMENT ON COLUMN rankings_individual.ranking_category IS 'Category string like "Rookie Men Iron Rifle"';
COMMENT ON COLUMN rankings_individual.overall_rank IS 'Position within category (1st, 2nd, 3rd, etc.)';
COMMENT ON COLUMN rankings_individual.is_hoa_winner IS 'High Overall Aggregate winner (1st place in category)';
COMMENT ON COLUMN rankings_individual.percentile IS 'Performance percentile within category (0-100)';
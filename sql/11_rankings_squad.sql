-- rankings_squad: Squad standings by division + discipline
-- Purpose: Squad rankings by division + discipline for team awards

DROP TABLE IF EXISTS rankings_squad CASCADE;

CREATE TABLE rankings_squad (
    squad_ranking_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys
    competition_key BIGINT NOT NULL REFERENCES dim_competition(competition_key),
    team_key BIGINT NOT NULL REFERENCES dim_team(team_key),
    discipline_key BIGINT NOT NULL REFERENCES dim_discipline(discipline_key),
    classification_key INTEGER REFERENCES dim_classification(classification_key),
    
    -- Ranking Category (division + discipline)
    ranking_category VARCHAR(100) NOT NULL,  -- e.g., "Senior Division Iron Rifle"
    division_name VARCHAR(50),               -- Rookie, Intermediate, Senior, Collegiate, Open
    discipline_name VARCHAR(50),             -- Iron Rifle, Optic Pistol, etc.
    
    -- Squad Information
    squad_name VARCHAR(100),                 -- Team name + discipline
    members_count SMALLINT,                  -- Number of squad members (1-4)
    
    -- Performance Data
    total_time DECIMAL(8,3) NOT NULL,        -- Squad total time (sum of top 4 members)
    average_time DECIMAL(8,3),               -- Average time per member
    raw_time DECIMAL(8,3),                   -- Total raw time without penalties
    penalty_time DECIMAL(8,3),               -- Total squad penalties
    
    -- Ranking Position
    overall_rank INTEGER NOT NULL,           -- 1st, 2nd, 3rd, etc. within division+discipline
    total_squads INTEGER NOT NULL,           -- Total squads in this category
    percentile DECIMAL(5,2),                 -- Performance percentile (0-100)
    
    -- Award Information
    award_level VARCHAR(20),                 -- "1st Place", "2nd Place", "3rd Place", etc.
    is_division_winner BOOLEAN DEFAULT false, -- Division winner (1st place in division+discipline)
    
    -- Squad Composition
    is_complete_squad BOOLEAN DEFAULT true,  -- All required members present
    is_mixed_division BOOLEAN DEFAULT false, -- Mixed division classification (Open)
    has_ghost_athletes BOOLEAN DEFAULT false, -- Uses ghost athletes (Rookie division)
    
    -- Member Details (for reference)
    member1_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member1_time DECIMAL(8,3),
    member2_athlete_key BIGINT REFERENCES dim_athlete(athlete_key), 
    member2_time DECIMAL(8,3),
    member3_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member3_time DECIMAL(8,3),
    member4_athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    member4_time DECIMAL(8,3),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(competition_key, team_key, discipline_key),  -- One ranking per squad per discipline per competition
    CHECK (overall_rank > 0),
    CHECK (total_squads > 0),
    CHECK (percentile >= 0 AND percentile <= 100),
    CHECK (total_time > 0),
    CHECK (members_count > 0 AND members_count <= 4)
);

-- Indexes for performance
CREATE INDEX idx_rankings_squad_competition ON rankings_squad(competition_key);
CREATE INDEX idx_rankings_squad_team ON rankings_squad(team_key);
CREATE INDEX idx_rankings_squad_category ON rankings_squad(ranking_category);
CREATE INDEX idx_rankings_squad_rank ON rankings_squad(overall_rank);
CREATE INDEX idx_rankings_squad_division_disc ON rankings_squad(division_name, discipline_name);

-- View for easy querying
CREATE OR REPLACE VIEW v_squad_rankings AS
SELECT 
    rs.squad_ranking_key,
    
    -- Competition info
    c.name as competition_name,
    c.start_date as competition_date,
    
    -- Team info
    t.name as team_name,
    t.paper_name as team_short_name,
    
    -- Ranking info
    rs.ranking_category,
    rs.division_name,
    rs.discipline_name,
    rs.squad_name,
    rs.overall_rank,
    rs.total_squads,
    rs.award_level,
    rs.is_division_winner,
    
    -- Performance
    rs.total_time,
    rs.average_time,
    rs.raw_time,
    rs.penalty_time,
    rs.percentile,
    
    -- Squad composition
    rs.members_count,
    rs.is_complete_squad,
    rs.is_mixed_division,
    rs.has_ghost_athletes,
    
    -- Member info
    a1.fname as member1_first_name,
    a1.lname as member1_last_name,
    rs.member1_time,
    a2.fname as member2_first_name,
    a2.lname as member2_last_name,
    rs.member2_time,
    a3.fname as member3_first_name,
    a3.lname as member3_last_name,
    rs.member3_time,
    a4.fname as member4_first_name,
    a4.lname as member4_last_name,
    rs.member4_time,
    
    rs.created_at

FROM rankings_squad rs
JOIN dim_competition c ON rs.competition_key = c.competition_key
JOIN dim_team t ON rs.team_key = t.team_key
JOIN dim_discipline d ON rs.discipline_key = d.discipline_key
LEFT JOIN dim_classification dc ON rs.classification_key = dc.classification_key
LEFT JOIN dim_athlete a1 ON rs.member1_athlete_key = a1.athlete_key
LEFT JOIN dim_athlete a2 ON rs.member2_athlete_key = a2.athlete_key  
LEFT JOIN dim_athlete a3 ON rs.member3_athlete_key = a3.athlete_key
LEFT JOIN dim_athlete a4 ON rs.member4_athlete_key = a4.athlete_key;

COMMENT ON TABLE rankings_squad IS 'Squad rankings by division + discipline for team awards';
COMMENT ON COLUMN rankings_squad.ranking_category IS 'Category string like "Senior Division Iron Rifle"';
COMMENT ON COLUMN rankings_squad.overall_rank IS 'Position within division+discipline (1st, 2nd, 3rd, etc.)';
COMMENT ON COLUMN rankings_squad.is_division_winner IS 'Division winner (1st place in division+discipline)';
COMMENT ON COLUMN rankings_squad.has_ghost_athletes IS 'Squad uses ghost athletes (allowed in Rookie division)';
-- dim_classification table for divisions and classes
-- Based on user's division data: Rookie(1), Intermediate(2), Senior(3), Collegiate(4), Open(5), Collegiate-Open(6)

CREATE TABLE dim_classification (
    classification_key BIGSERIAL PRIMARY KEY,
    
    -- Division information
    division_id SMALLINT,                    -- 1-6 from user's data (nullable for sub-classes)
    division_name VARCHAR(50) NOT NULL,      -- Rookie, Intermediate, Senior, etc.
    division_order SMALLINT NOT NULL,        -- Display order
    
    -- Class information (more granular than division)
    class_id SMALLINT,                       -- Unique class identifier
    class_name VARCHAR(50),                  -- Rookie, Intermediate/Entry, etc.
    class_full_name VARCHAR(100),            -- Full descriptive name
    
    -- Age and skill constraints
    age_min INTEGER,                         -- Minimum age for this classification
    age_max INTEGER,                         -- Maximum age for this classification
    skill_level VARCHAR(20),                 -- Entry, Advanced, JV, Varsity
    
    -- Special classifications
    is_open_division BOOLEAN DEFAULT false,  -- True for Open and Collegiate-Open
    is_mixed_division BOOLEAN DEFAULT false, -- True when athletes from different divisions compete together
    allows_ghost_athletes BOOLEAN DEFAULT false, -- True for Rookie (up to 2 ghost athletes)
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    display_order SMALLINT,                  -- For UI sorting
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(division_id),  -- Allow NULL, unique if not null
    UNIQUE(class_id),     -- Each class must be unique
    UNIQUE(class_id)
);

-- Create indexes for performance
CREATE INDEX idx_dim_classification_division_id ON dim_classification(division_id);
CREATE INDEX idx_dim_classification_division_name ON dim_classification(division_name);
CREATE INDEX idx_dim_classification_class_id ON dim_classification(class_id);
CREATE INDEX idx_dim_classification_is_active ON dim_classification(is_active);

-- Comments for documentation
COMMENT ON TABLE dim_classification IS 'Dimension table for athlete divisions and classes';
COMMENT ON COLUMN dim_classification.division_id IS 'Division ID from source system (1-6)';
COMMENT ON COLUMN dim_classification.allows_ghost_athletes IS 'True for Rookie division (up to 2 ghost athletes per squad)';
COMMENT ON COLUMN dim_classification.is_open_division IS 'True for Open classifications (mixed skill levels)';

-- Insert division data based on user's screenshot
INSERT INTO dim_classification (
    division_id, division_name, division_order, class_id, class_name, class_full_name, 
    allows_ghost_athletes, is_open_division, display_order
) VALUES
-- Standard Divisions (main divisions)
(1, 'Rookie', 1, 1, 'Rookie', 'Rookie Division', true, false, 1),
(2, 'Intermediate', 2, 2, 'Intermediate', 'Intermediate Division', false, false, 2),
(3, 'Senior', 3, 3, 'Senior', 'Senior Division', false, false, 3),
(4, 'Collegiate', 4, 4, 'Collegiate', 'Collegiate Division', false, false, 4),

-- Open Divisions (mixed skill levels)
(5, 'Open', 5, 5, 'Open', 'Open Division (Mixed Skill Levels)', false, true, 5),
(6, 'Collegiate-Open', 6, 6, 'Collegiate-Open', 'Collegiate Open Division', false, true, 6),

-- Additional class granularity (using NULL division_id for sub-classes)
(NULL, 'Intermediate', 2, 21, 'Intermediate/Entry', 'Intermediate Entry Level', false, false, 21),
(NULL, 'Intermediate', 2, 22, 'Intermediate/Advanced', 'Intermediate Advanced Level', false, false, 22),
(NULL, 'Senior', 3, 31, 'Senior/JV', 'Senior Junior Varsity', false, false, 31),
(NULL, 'Senior', 3, 32, 'Senior/Varsity', 'Senior Varsity', false, false, 32);

-- Function to get classification by division name
CREATE OR REPLACE FUNCTION get_classification_key_by_division(input_division_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    result_key BIGINT;
BEGIN
    -- Try exact match first
    SELECT classification_key INTO result_key
    FROM dim_classification 
    WHERE division_name = input_division_name
    AND is_active = true
    ORDER BY division_order
    LIMIT 1;
    
    IF result_key IS NOT NULL THEN
        RETURN result_key;
    END IF;
    
    -- Try case-insensitive match
    SELECT classification_key INTO result_key
    FROM dim_classification 
    WHERE LOWER(division_name) = LOWER(input_division_name)
    AND is_active = true
    ORDER BY division_order
    LIMIT 1;
    
    RETURN result_key; -- Will be NULL if no match found
END;
$$ LANGUAGE plpgsql;

-- Function to determine squad division classification
CREATE OR REPLACE FUNCTION get_squad_division(
    athlete1_division TEXT,
    athlete2_division TEXT,
    athlete3_division TEXT,
    athlete4_division TEXT
)
RETURNS TEXT AS $$
BEGIN
    -- If all athletes are in the same division, return that division
    IF athlete1_division = athlete2_division AND 
       athlete2_division = athlete3_division AND 
       athlete3_division = athlete4_division THEN
        RETURN athlete1_division;
    END IF;
    
    -- If mixed divisions, classify as "Open"
    RETURN 'Open';
END;
$$ LANGUAGE plpgsql;
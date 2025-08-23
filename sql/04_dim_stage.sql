-- dim_stage table for standardizing stage names and providing proper dimensional modeling

CREATE TABLE dim_stage (
    stage_key BIGSERIAL PRIMARY KEY,
    stage_number SMALLINT NOT NULL,           -- 1, 2, 3, 4 (standardized stage sequence)
    stage_name_standard VARCHAR(50) NOT NULL, -- Standardized friendly name
    stage_short_code VARCHAR(10) NOT NULL,    -- Short code (InOut, !, V, etc.)
    stage_name_long VARCHAR(100),             -- Full descriptive name
    stage_description TEXT,                   -- Detailed description of the stage
    
    -- Common variations (for mapping inconsistent source data)
    stage_name_variations TEXT[],             -- Array of known variations
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    display_order SMALLINT DEFAULT 1,        -- For UI ordering
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    UNIQUE(stage_number),
    UNIQUE(stage_name_standard),
    UNIQUE(stage_short_code)
);

-- Create indexes for performance
CREATE INDEX idx_dim_stage_number ON dim_stage(stage_number);
CREATE INDEX idx_dim_stage_short_code ON dim_stage(stage_short_code);
CREATE INDEX idx_dim_stage_variations ON dim_stage USING GIN(stage_name_variations);

-- Comments for documentation
COMMENT ON TABLE dim_stage IS 'Dimension table for standardizing shooting stage names and codes';
COMMENT ON COLUMN dim_stage.stage_number IS 'Sequential stage number (1-4)';
COMMENT ON COLUMN dim_stage.stage_name_standard IS 'Official standardized stage name';
COMMENT ON COLUMN dim_stage.stage_short_code IS 'Short code for UI/reports (InOut, !, V, etc.)';
COMMENT ON COLUMN dim_stage.stage_name_variations IS 'Array of known variations for mapping source data';

-- Complete stage definitions based on user's Power BI analysis and dim_stage example
-- 8 total stages: 4 Primary + 4 Alternate
-- Stage 1 is almost always 'Go Fast' or 'GoFast'

INSERT INTO dim_stage (stage_number, stage_name_standard, stage_short_code, stage_name_long, stage_name_variations, display_order, is_active) VALUES
-- PRIMARY STAGES (Order 1-4)
(1, 'GoFast', 'GoFast', 'Go Fast Stage', ARRAY['go fast', 'Go Fast', 'GoFast', 'Go-Fast'], 1, true),
(2, 'Focus', 'Focus', 'Focus Stage', ARRAY['focus', 'Focus'], 2, true),
(3, 'SpeedTrap', 'SpeedTrap', 'Speed Trap Stage', ARRAY['speed trap', 'Speed Trap', 'SpeedTrap'], 3, true),
(4, 'InOut', 'InOut', 'In and Out Stage', ARRAY['in and out', 'In and Out', 'InOut', 'In & Out'], 4, true),

-- ALTERNATE STAGES (Order 5-8)
(5, 'Exclamation', '!', 'Exclamation Point Stage', ARRAY['exclamation point', 'Exclamation', '!', 'exclamation'], 5, true),
(6, 'M', 'M', 'M Stage', ARRAY['m', 'M'], 6, true),
(7, 'V for Victory', 'V', 'V for Victory Stage', ARRAY['v', 'V', 'VforV', 'V for Victory'], 7, true),
(8, 'PopQuiz', 'PopQuiz', 'Pop Quiz Stage', ARRAY['pop quiz', 'Pop Quiz', 'PopQuiz', 'popquiz'], 8, true);

-- Add metadata columns for tracking Primary vs Alternate
ALTER TABLE dim_stage ADD COLUMN stage_type VARCHAR(20) DEFAULT 'Primary';
ALTER TABLE dim_stage ADD COLUMN stage_pair_letter VARCHAR(5);

-- Update stage types and pair letters based on your table
UPDATE dim_stage SET stage_type = 'Primary', stage_pair_letter = 'A' WHERE stage_number = 1;
UPDATE dim_stage SET stage_type = 'Primary', stage_pair_letter = 'B' WHERE stage_number = 2;
UPDATE dim_stage SET stage_type = 'Primary', stage_pair_letter = 'D' WHERE stage_number = 3;
UPDATE dim_stage SET stage_type = 'Primary', stage_pair_letter = 'C' WHERE stage_number = 4;
UPDATE dim_stage SET stage_type = 'Alternate', stage_pair_letter = 'D' WHERE stage_number = 5;
UPDATE dim_stage SET stage_type = 'Alternate', stage_pair_letter = 'B' WHERE stage_number = 6;
UPDATE dim_stage SET stage_type = 'Alternate', stage_pair_letter = 'C' WHERE stage_number = 7;
UPDATE dim_stage SET stage_type = 'Alternate', stage_pair_letter = 'C' WHERE stage_number = 8;

-- Function to find stage_key by any name variation
CREATE OR REPLACE FUNCTION get_stage_key(input_stage_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    result_key BIGINT;
BEGIN
    -- First try exact match on standard name
    SELECT stage_key INTO result_key
    FROM dim_stage 
    WHERE stage_name_standard = input_stage_name;
    
    IF result_key IS NOT NULL THEN
        RETURN result_key;
    END IF;
    
    -- Try short code match
    SELECT stage_key INTO result_key
    FROM dim_stage 
    WHERE stage_short_code = input_stage_name;
    
    IF result_key IS NOT NULL THEN
        RETURN result_key;
    END IF;
    
    -- Try variations array
    SELECT stage_key INTO result_key
    FROM dim_stage 
    WHERE input_stage_name = ANY(stage_name_variations);
    
    IF result_key IS NOT NULL THEN
        RETURN result_key;
    END IF;
    
    -- Try case-insensitive match on variations
    SELECT stage_key INTO result_key
    FROM dim_stage 
    WHERE LOWER(input_stage_name) = ANY(
        SELECT LOWER(unnest(stage_name_variations))
    );
    
    RETURN result_key; -- Will be NULL if no match found
END;
$$ LANGUAGE plpgsql;

-- Function to extract stage names from raw_scoreboard payload and return stage keys
CREATE OR REPLACE FUNCTION get_stage_keys_from_payload(payload JSONB)
RETURNS TABLE(stage_position TEXT, stage_key BIGINT, stage_name TEXT) AS $$
BEGIN
    -- Extract stage_one
    IF payload->>'stage_one' IS NOT NULL THEN
        RETURN QUERY SELECT 'stage_one'::TEXT, get_stage_key(payload->>'stage_one'), payload->>'stage_one';
    END IF;
    
    -- Extract stage_two  
    IF payload->>'stage_two' IS NOT NULL THEN
        RETURN QUERY SELECT 'stage_two'::TEXT, get_stage_key(payload->>'stage_two'), payload->>'stage_two';
    END IF;
    
    -- Extract stage_three
    IF payload->>'stage_three' IS NOT NULL THEN
        RETURN QUERY SELECT 'stage_three'::TEXT, get_stage_key(payload->>'stage_three'), payload->>'stage_three';
    END IF;
    
    -- Extract stage_four
    IF payload->>'stage_four' IS NOT NULL THEN
        RETURN QUERY SELECT 'stage_four'::TEXT, get_stage_key(payload->>'stage_four'), payload->>'stage_four';
    END IF;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;
-- dim_stage table for standardizing stage names and providing proper dimensional modeling

CREATE TABLE dim_stage (
    stage_key BIGSERIAL PRIMARY KEY,
    stage_number SMALLINT NOT NULL,           -- 1, 2, 3, 4 (standard stage sequence)
    stage_code VARCHAR(10) NOT NULL,          -- Short code: "InOut", "V", "!", "Accel"
    stage_name_standard VARCHAR(100) NOT NULL, -- Standardized full name
    stage_name_display VARCHAR(100),          -- User-friendly display name
    stage_description TEXT,                   -- Detailed description of the stage
    
    -- Common variations found in source data
    stage_name_variations TEXT[],             -- Array of known variations
    
    -- Stage characteristics
    target_distance_yards INTEGER,           -- Typical shooting distance
    movement_required BOOLEAN DEFAULT false, -- Does stage require shooter movement
    time_limit_seconds INTEGER,              -- Standard time limit (if applicable)
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique stage numbers and codes
    UNIQUE(stage_number),
    UNIQUE(stage_code)
);

-- Create index for fast lookups by variations
CREATE INDEX idx_dim_stage_variations ON dim_stage USING GIN(stage_name_variations);

-- Create function to find stage by any variation
CREATE OR REPLACE FUNCTION find_stage_key_by_name(input_stage_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    stage_key_result BIGINT;
BEGIN
    -- Try exact match on standard name first
    SELECT stage_key INTO stage_key_result
    FROM dim_stage 
    WHERE stage_name_standard ILIKE input_stage_name
    LIMIT 1;
    
    -- If not found, try variations array
    IF stage_key_result IS NULL THEN
        SELECT stage_key INTO stage_key_result
        FROM dim_stage 
        WHERE input_stage_name = ANY(stage_name_variations)
        LIMIT 1;
    END IF;
    
    -- If still not found, try fuzzy match on display name
    IF stage_key_result IS NULL THEN
        SELECT stage_key INTO stage_key_result
        FROM dim_stage 
        WHERE stage_name_display ILIKE '%' || input_stage_name || '%'
        LIMIT 1;
    END IF;
    
    RETURN stage_key_result;
END;
$$ LANGUAGE plpgsql;

-- Sample data structure (you'll provide the actual names)
INSERT INTO dim_stage (
    stage_number, 
    stage_code, 
    stage_name_standard, 
    stage_name_display,
    stage_name_variations,
    stage_description
) VALUES 
-- Stage 1 - In and Out / InOut
(1, 'InOut', 'In and Out', 'In and Out', 
 ARRAY['In and out', 'in & out', 'InOut', 'In & Out', 'In/Out'],
 'Shooter moves between shooting positions'),

-- Stage 2 - V for Victory
(2, 'V', 'V for Victory', 'V for Victory',
 ARRAY['V', 'Victory', 'V-Victory', 'V for victory'],
 'V-shaped shooting pattern or target configuration'),

-- Stage 3 - Exclamation
(3, '!', 'Exclamation', 'Exclamation',
 ARRAY['!', 'Exclamation Point', 'Exclaim', 'Exclamation!'],
 'Exclamation point shooting pattern'),

-- Stage 4 - Accelerator (example)
(4, 'Accel', 'Accelerator', 'Accelerator',
 ARRAY['Accelerator', 'Accel', 'Acceleration', 'Accelerated'],
 'Accelerating pace shooting stage');

-- Update fact_entry_strings to include stage_key (add column)
ALTER TABLE fact_entry_strings ADD COLUMN stage_key BIGINT REFERENCES dim_stage(stage_key);

-- Update fact_stage_performance to include stage_key (when we create it)
-- ALTER TABLE fact_stage_performance ADD COLUMN stage_key BIGINT REFERENCES dim_stage(stage_key);

-- Create view to show stage mapping analysis
CREATE OR REPLACE VIEW vw_stage_mapping_analysis AS
SELECT 
    ds.stage_number,
    ds.stage_code,
    ds.stage_name_standard,
    COUNT(DISTINCT slot.slot_key) as slots_using_stage,
    COUNT(DISTINCT fes.entry_id) as entries_in_stage,
    array_agg(DISTINCT slot.stage ORDER BY slot.stage) as source_stage_names
FROM dim_stage ds
    LEFT JOIN dim_slot slot ON find_stage_key_by_name(slot.stage) = ds.stage_key
    LEFT JOIN fact_entry fe ON slot.slot_key = fe.slot_key
    LEFT JOIN fact_entry_strings fes ON fe.entry_id = fes.entry_id AND fes.stage_no = ds.stage_number
GROUP BY ds.stage_key, ds.stage_number, ds.stage_code, ds.stage_name_standard
ORDER BY ds.stage_number;

-- Populate stage_key in fact_entry_strings based on stage_no
UPDATE fact_entry_strings 
SET stage_key = ds.stage_key
FROM dim_stage ds
WHERE fact_entry_strings.stage_no = ds.stage_number;

COMMENT ON TABLE dim_stage IS 'Dimension table for standardized stage names and characteristics';
COMMENT ON COLUMN dim_stage.stage_code IS 'Short, consistent code for each stage (InOut, V, !, Accel)';
COMMENT ON COLUMN dim_stage.stage_name_variations IS 'Array of all known variations found in source data';
COMMENT ON FUNCTION find_stage_key_by_name IS 'Function to resolve any stage name variation to proper stage_key';
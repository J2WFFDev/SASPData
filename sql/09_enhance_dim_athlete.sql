-- Enhance dim_athlete table with classification columns
-- This adds athlete classification support for proper categorization

-- Add new columns to dim_athlete table
ALTER TABLE dim_athlete 
ADD COLUMN IF NOT EXISTS classification_key INTEGER REFERENCES dim_classification(classification_key),
ADD COLUMN IF NOT EXISTS division_name VARCHAR(50),
ADD COLUMN IF NOT EXISTS class_name VARCHAR(50),
ADD COLUMN IF NOT EXISTS is_ghost_athlete BOOLEAN DEFAULT false;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_athlete_classification ON dim_athlete(classification_key);
CREATE INDEX IF NOT EXISTS idx_athlete_division ON dim_athlete(division_name);
CREATE INDEX IF NOT EXISTS idx_athlete_ghost ON dim_athlete(is_ghost_athlete);

-- Create view for enhanced athlete information
CREATE OR REPLACE VIEW v_athlete_enhanced AS
SELECT 
    a.athlete_key,
    a.ath_id_nat,
    a.fname,
    a.lname,
    a.fname || ' ' || a.lname as full_name,
    a.gender,
    a.bdate,
    a.city,
    a.state_id_nat,
    a.email,
    
    -- Classification information
    a.classification_key,
    a.division_name,
    a.class_name,
    a.is_ghost_athlete,
    
    -- From dim_classification lookup
    dc.division_id,
    dc.division_order,
    dc.allows_ghost_athletes as division_allows_ghosts,
    dc.is_open_division,
    dc.is_mixed_division,
    
    -- Computed fields
    CASE 
        WHEN a.is_ghost_athlete THEN 'Ghost Athlete'
        WHEN a.division_name IS NULL THEN 'Unclassified'
        ELSE a.division_name 
    END as display_division,
    
    CASE 
        WHEN a.is_ghost_athlete THEN 'Ghost'
        WHEN a.class_name IS NULL THEN 'Unclassified'
        ELSE a.class_name 
    END as display_class,
    
    a.created_at,
    a.updated_at

FROM dim_athlete a
LEFT JOIN dim_classification dc ON a.classification_key = dc.classification_key;

-- Function to classify athlete based on raw data patterns
CREATE OR REPLACE FUNCTION classify_athlete(
    p_athlete_key BIGINT,
    p_team_name VARCHAR DEFAULT NULL,
    p_discipline_name VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_classification_key INTEGER;
    v_division_name VARCHAR(50);
    v_class_name VARCHAR(50);
    v_is_ghost BOOLEAN := false;
BEGIN
    -- Check if this is a ghost athlete (common patterns)
    IF p_team_name ILIKE '%ghost%' OR 
       p_team_name ILIKE '%substitute%' OR
       p_discipline_name ILIKE '%ghost%' THEN
        v_is_ghost := true;
        v_division_name := 'Rookie';  -- Ghost athletes typically in Rookie
        v_class_name := 'Ghost';
        
        -- Get the Rookie classification key
        SELECT classification_key INTO v_classification_key
        FROM dim_classification 
        WHERE division_name = 'Rookie' AND class_name = 'Rookie';
        
    ELSE
        -- Default to unclassified for now
        -- This will be enhanced with more sophisticated logic later
        v_classification_key := NULL;
        v_division_name := NULL;
        v_class_name := NULL;
    END IF;
    
    -- Update the athlete record
    UPDATE dim_athlete 
    SET 
        classification_key = v_classification_key,
        division_name = v_division_name,
        class_name = v_class_name,
        is_ghost_athlete = v_is_ghost,
        updated_at = CURRENT_TIMESTAMP
    WHERE athlete_key = p_athlete_key;
    
    RETURN v_classification_key;
END;
$$ LANGUAGE plpgsql;

-- Function to bulk classify athletes from entries
CREATE OR REPLACE FUNCTION bulk_classify_athletes() RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
    v_athlete_rec RECORD;
BEGIN
    -- Classify athletes based on entry patterns
    FOR v_athlete_rec IN 
        SELECT DISTINCT 
            a.athlete_key,
            t.name as team_name,
            d.name as discipline_name
        FROM dim_athlete a
        JOIN fact_entry fe ON a.athlete_key = fe.athlete_key
        JOIN dim_team t ON fe.team_key = t.team_key
        JOIN dim_discipline d ON fe.discipline_key = d.discipline_key
        WHERE a.classification_key IS NULL
    LOOP
        PERFORM classify_athlete(
            v_athlete_rec.athlete_key,
            v_athlete_rec.team_name,
            v_athlete_rec.discipline_name
        );
        v_count := v_count + 1;
    END LOOP;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON COLUMN dim_athlete.classification_key IS 'Foreign key to dim_classification for athlete division/class';
COMMENT ON COLUMN dim_athlete.division_name IS 'Denormalized division name for quick access (Rookie, Intermediate, etc.)';
COMMENT ON COLUMN dim_athlete.class_name IS 'Denormalized class name for quick access (Entry, Advanced, JV, etc.)';
COMMENT ON COLUMN dim_athlete.is_ghost_athlete IS 'True if this is a ghost/substitute athlete';
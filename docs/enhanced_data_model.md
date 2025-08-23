# Enhanced Data Model for SASP Analytics System

## Current State Analysis

### ✅ **What We Have (Good Foundation)**
- **Timing Data**: `fact_entry_strings` with `time_value`, `penalty_value`, `total_value`
- **Stage/String Structure**: `stage_no` (1-4) and `string_no` (1-5) 
- **Athlete Demographics**: `dim_athlete` with `gender`
- **Squad Indicators**: `dim_slot` contains squad/relay information
- **Comprehensive Coverage**: ~60K entries across 4 stages with ~300K strings

### ❌ **What We Need to Add**
- **Division/Class Classifications**: Missing from `dim_athlete`
- **Squad Aggregation Tables**: For team-based analytics
- **Match Aggregation Tables**: For complete match calculations
- **Ranking Tables**: For leaderboard management
- **Analytics Views**: For web reporting

## Proposed Data Model Extensions

### 🆕 **New Dimension Table: dim_stage**
```sql
CREATE TABLE dim_stage (
    stage_key BIGSERIAL PRIMARY KEY,
    stage_number SMALLINT NOT NULL,           -- 1, 2, 3, 4 (standardized)
    stage_name_standard VARCHAR(50) NOT NULL, -- Friendly name (e.g., "Exclamation")
    stage_short_code VARCHAR(10) NOT NULL,    -- Short code (e.g., "!")
    stage_name_long VARCHAR(100),             -- Full descriptive name
    stage_name_variations TEXT[],             -- Known variations for mapping
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **New Dimension Table: dim_classification**
```sql
CREATE TABLE dim_classification (
    classification_key BIGSERIAL PRIMARY KEY,
    division VARCHAR(20) NOT NULL,           -- Rookie, Intermediate, Senior, Collegiate
    class_name VARCHAR(50) NOT NULL,         -- Rookie, Intermediate/Entry, etc.
    age_min INTEGER,                         -- Minimum age for classification
    age_max INTEGER,                         -- Maximum age for classification
    skill_level VARCHAR(20),                 -- Entry, Advanced, JV, Varsity
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **Enhanced dim_athlete (Add Classification)**
```sql
ALTER TABLE dim_athlete ADD COLUMN classification_key BIGINT REFERENCES dim_classification(classification_key);
ALTER TABLE dim_athlete ADD COLUMN division VARCHAR(20);  -- Denormalized for performance
ALTER TABLE dim_athlete ADD COLUMN class_name VARCHAR(50); -- Denormalized for performance
```

### 🆕 **Enhanced fact_stage_performance (Add stage_key)**
```sql
CREATE TABLE fact_stage_performance (
    stage_performance_id BIGSERIAL PRIMARY KEY,
    entry_id BIGINT NOT NULL REFERENCES fact_entry(entry_id),
    athlete_key BIGINT NOT NULL REFERENCES dim_athlete(athlete_key),
    team_key BIGINT REFERENCES dim_team(team_key),
    competition_key BIGINT REFERENCES dim_competition(competition_key),
    slot_key BIGINT REFERENCES dim_slot(slot_key),
    discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
    stage_key BIGINT NOT NULL REFERENCES dim_stage(stage_key),  -- NEW: Proper FK
    stage_no SMALLINT NOT NULL,  -- Keep for backwards compatibility
    
    -- String-level aggregation (drop slowest, keep 4 fastest)
    best_4_times NUMERIC[] NOT NULL,         -- Array of 4 fastest times
    dropped_time NUMERIC,                    -- The slowest time that was dropped
    stage_time_raw NUMERIC NOT NULL,         -- Sum of 4 best raw times
    stage_penalty_total NUMERIC DEFAULT 0,   -- Sum of penalties
    stage_time_total NUMERIC NOT NULL,       -- Final stage time (raw + penalties)
    string_count SMALLINT DEFAULT 5,         -- How many strings were attempted
    
    -- Validation flags
    is_complete BOOLEAN DEFAULT true,        -- All required strings present
    is_valid BOOLEAN DEFAULT true,          -- Not DNF/DQ
    dnf_flag BOOLEAN DEFAULT false,         -- Did Not Finish
    dq_flag BOOLEAN DEFAULT false,          -- Disqualified
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **New Fact Table: fact_match_performance**
```sql
CREATE TABLE fact_match_performance (
    match_performance_id BIGSERIAL PRIMARY KEY,
    entry_id BIGINT NOT NULL REFERENCES fact_entry(entry_id),
    athlete_key BIGINT NOT NULL REFERENCES dim_athlete(athlete_key),
    team_key BIGINT REFERENCES dim_team(team_key),
    competition_key BIGINT REFERENCES dim_competition(competition_key),
    slot_key BIGINT REFERENCES dim_slot(slot_key),
    discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
    classification_key BIGINT REFERENCES dim_classification(classification_key),
    
    -- Match-level aggregation (sum of 4 stages)
    match_time_raw NUMERIC NOT NULL,         -- Sum of all 4 stage raw times
    match_penalty_total NUMERIC DEFAULT 0,   -- Sum of all penalties
    match_time_total NUMERIC NOT NULL,       -- Final match time
    stages_completed SMALLINT DEFAULT 4,     -- How many stages completed
    
    -- Validation flags
    is_complete_match BOOLEAN DEFAULT true,  -- All 4 stages valid
    is_valid_match BOOLEAN DEFAULT true,     -- Not DNF/DQ
    
    -- Classification context
    division VARCHAR(20),                    -- Denormalized for queries
    class_name VARCHAR(50),                  -- Denormalized for queries
    gender VARCHAR(10),                      -- Denormalized for queries
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **New Fact Table: fact_squad_performance**
```sql
CREATE TABLE fact_squad_performance (
    squad_performance_id BIGSERIAL PRIMARY KEY,
    squad_identifier VARCHAR(100) NOT NULL,  -- Unique squad ID (slot + division)
    competition_key BIGINT REFERENCES dim_competition(competition_key),
    slot_key BIGINT REFERENCES dim_slot(slot_key),
    discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
    
    -- Squad composition
    athlete_1_key BIGINT REFERENCES dim_athlete(athlete_key),
    athlete_2_key BIGINT REFERENCES dim_athlete(athlete_key), 
    athlete_3_key BIGINT REFERENCES dim_athlete(athlete_key),
    athlete_4_key BIGINT REFERENCES dim_athlete(athlete_key),
    
    -- Squad classification (must be homogeneous or "Open")
    squad_division VARCHAR(20) NOT NULL,     -- Rookie, Intermediate, Senior, Collegiate, Open
    is_mixed_division BOOLEAN DEFAULT false, -- True if mixed divisions = Open
    
    -- Ghost athlete handling (for Rookie squads)
    ghost_athlete_count SMALLINT DEFAULT 0,  -- Number of ghost athletes (0-2)
    ghost_time_each NUMERIC DEFAULT 100.0,   -- Default time per ghost athlete
    
    -- Squad performance aggregation
    squad_time_total NUMERIC NOT NULL,       -- Sum of 4 athlete match times + ghost times
    valid_athlete_count SMALLINT NOT NULL,   -- Number of real athletes
    
    -- Validation flags
    is_complete_squad BOOLEAN DEFAULT true,  -- All required athletes present
    is_valid_squad BOOLEAN DEFAULT true,     -- All athletes have valid matches
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **New Table: rankings_individual**
```sql
CREATE TABLE rankings_individual (
    ranking_id BIGSERIAL PRIMARY KEY,
    match_performance_id BIGINT NOT NULL REFERENCES fact_match_performance(match_performance_id),
    competition_key BIGINT REFERENCES dim_competition(competition_key),
    athlete_key BIGINT REFERENCES dim_athlete(athlete_key),
    classification_key BIGINT REFERENCES dim_classification(classification_key),
    discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
    
    -- Ranking context
    ranking_category VARCHAR(100) NOT NULL,  -- e.g., "Rookie_Men_Iron_Rifle"
    division VARCHAR(20) NOT NULL,
    class_name VARCHAR(50) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    discipline_name VARCHAR(50) NOT NULL,
    
    -- Performance and ranking
    match_time_total NUMERIC NOT NULL,
    place_overall INTEGER NOT NULL,          -- 1st, 2nd, 3rd, etc.
    place_in_category INTEGER NOT NULL,      -- Place within this specific category
    total_in_category INTEGER NOT NULL,      -- Total participants in category
    percentile_rank NUMERIC,                 -- Performance percentile
    
    -- Awards
    award_level VARCHAR(20),                 -- HOA, 1st Place, 2nd Place, etc.
    is_hoa BOOLEAN DEFAULT false,           -- High Overall Aggregate winner
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 🆕 **New Table: rankings_squad**
```sql
CREATE TABLE rankings_squad (
    squad_ranking_id BIGSERIAL PRIMARY KEY,
    squad_performance_id BIGINT NOT NULL REFERENCES fact_squad_performance(squad_performance_id),
    competition_key BIGINT REFERENCES dim_competition(competition_key),
    discipline_key BIGINT REFERENCES dim_discipline(discipline_key),
    
    -- Ranking context
    ranking_category VARCHAR(100) NOT NULL,  -- e.g., "Senior_1911"
    squad_division VARCHAR(20) NOT NULL,
    discipline_name VARCHAR(50) NOT NULL,
    
    -- Performance and ranking
    squad_time_total NUMERIC NOT NULL,
    place_overall INTEGER NOT NULL,
    place_in_category INTEGER NOT NULL,
    total_in_category INTEGER NOT NULL,
    
    -- Squad details
    squad_identifier VARCHAR(100) NOT NULL,
    athlete_names TEXT[],                    -- Array of athlete names for display
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

## Enhanced Data Processing Pipeline

### 📊 **ETL Process Flow**
```
Raw String Data (fact_entry_strings)
    ↓ (Process: Drop slowest, keep 4 fastest)
Stage Performance (fact_stage_performance)
    ↓ (Process: Sum 4 stages)
Match Performance (fact_match_performance)
    ↓ (Process: Group by squad, handle ghosts)
Squad Performance (fact_squad_performance)
    ↓ (Process: Rank within categories)
Rankings (rankings_individual, rankings_squad)
    ↓ (Process: Generate views)
Web Analytics Dashboard
```

### 🔄 **Aggregation Rules Implementation**
```sql
-- Example: Calculate stage performance (drop slowest string)
INSERT INTO fact_stage_performance (...)
SELECT 
    entry_id,
    stage_no,
    array_agg(time_value ORDER BY time_value)[1:4] as best_4_times,  -- Keep 4 fastest
    array_agg(time_value ORDER BY time_value DESC)[1] as dropped_time, -- Drop slowest
    (array_agg(time_value ORDER BY time_value)[1] + 
     array_agg(time_value ORDER BY time_value)[2] + 
     array_agg(time_value ORDER BY time_value)[3] + 
     array_agg(time_value ORDER BY time_value)[4]) as stage_time_raw
FROM fact_entry_strings
WHERE time_value > 0 AND total_value > 0  -- Valid times only
GROUP BY entry_id, stage_no
HAVING COUNT(*) = 5;  -- Must have all 5 strings
```

## ASCII Tree View: Complete Data Model

```
SASP ANALYTICS DATA MODEL
└── DIMENSIONAL FOUNDATION
    ├── dim_competition (competitions/matches)
    ├── dim_team (shooting teams)
    ├── dim_athlete (individual shooters)
    │   └── [ENHANCED] + classification_key, division, class_name
    ├── dim_discipline (weapon types)
    ├── dim_slot (time/location slots)
    ├── dim_range (firing ranges)
    └── [NEW] dim_classification (divisions & classes)
        ├── Rookie, Intermediate, Senior, Collegiate
        └── Entry, Advanced, JV, Varsity levels

└── RAW PERFORMANCE DATA
    ├── fact_entry (athlete competition entries)
    └── fact_entry_strings (individual string times)
        ├── time_value (raw string time)
        ├── penalty_value (penalties applied)
        └── total_value (final string time)

└── AGGREGATED PERFORMANCE ANALYTICS
    ├── [NEW] fact_stage_performance
    │   ├── best_4_times[] (drop slowest, keep 4)
    │   ├── stage_time_total (sum of 4 best strings)
    │   └── validation flags (complete, valid, DNF, DQ)
    │
    ├── [NEW] fact_match_performance  
    │   ├── match_time_total (sum of 4 stages)
    │   ├── is_complete_match (all 4 stages valid)
    │   └── classification context (division, class, gender)
    │
    └── [NEW] fact_squad_performance
        ├── 4 athlete keys (team members)
        ├── squad_division (homogeneous or "Open")
        ├── ghost_athlete_count (0-2 for Rookie)
        └── squad_time_total (4 athletes + ghosts)

└── RANKINGS & LEADERBOARDS
    ├── [NEW] rankings_individual
    │   ├── HOA awards (High Overall Aggregate)
    │   ├── Category rankings (class + gender + discipline)
    │   └── Performance percentiles
    │
    └── [NEW] rankings_squad
        ├── Squad division rankings
        ├── Team time competitions
        └── Cross-division "Open" categories

└── ANALYTICS VIEWS (for Web Dashboard)
    ├── vw_athlete_performance_summary
    ├── vw_team_leaderboards  
    ├── vw_stage_analysis
    ├── vw_match_results
    ├── vw_squad_standings
    └── vw_classification_rankings
```

## Implementation Priority

### 🎯 **Phase 1: Foundation**
1. Add `dim_stage` table with standardized stage names and short codes
2. Add `dim_classification` table
3. Enhance `dim_athlete` with classification fields
4. Create stage name mapping and classification logic

### 🎯 **Phase 2: Aggregation**
1. Implement `fact_stage_performance` (string → stage aggregation)
2. Implement `fact_match_performance` (stage → match aggregation)  
3. Create ETL scripts for aggregation pipeline

### 🎯 **Phase 3: Squad Analytics**
1. Implement `fact_squad_performance` with ghost athlete logic
2. Create squad identification and classification logic
3. Handle mixed-division "Open" classification

### 🎯 **Phase 4: Rankings**
1. Implement `rankings_individual` and `rankings_squad`
2. Create ranking calculation procedures
3. Build leaderboard update automation

### 🎯 **Phase 5: Web Dashboard**
1. Create analytics views optimized for web queries
2. Build REST API endpoints for dashboard
3. Implement real-time ranking updates

This enhanced model maintains your existing star schema foundation while adding the specialized aggregation and ranking capabilities needed for competitive shooting analytics!
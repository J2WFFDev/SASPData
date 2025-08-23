# Silver Layer Dimensional Model Relationships

## Star Schema Overview

The silver layer implements a **star schema** with fact tables at the center connected to dimension tables via surrogate keys.

## Core Fact Tables (Star Centers)

### 🎯 fact_entry (Main Fact Table)
**Purpose**: One record per athlete per shooting event
**Grain**: athlete + competition + slot + discipline + team combination
**Records**: ~64,000+ entries

**Dimensional Relationships:**
```
fact_entry
├── competition_key → dim_competition (which competition)
├── team_key → dim_team (athlete's team)
├── athlete_key → dim_athlete (which shooter)
├── slot_key → dim_slot (time/location of event)
└── discipline_key → dim_discipline (what type of shooting)
```

**Measures (Facts):**
- `spp_final` - Final shooting score
- `spp_x_count` - Count of X-ring hits
- `entry_dt` - Entry timestamp
- Performance metrics

### 🎯 fact_entry_strings (Detail Fact Table)
**Purpose**: Individual string/stage performance within each entry
**Grain**: entry + stage + string combination
**Records**: ~1.3M+ string records

**Dimensional Relationships:**
```
fact_entry_strings
├── entry_id → fact_entry (parent entry)
├── competition_key → dim_competition (inherited from parent)
├── team_key → dim_team (inherited from parent)
├── athlete_key → dim_athlete (inherited from parent)
├── slot_key → dim_slot (inherited from parent)
└── discipline_key → dim_discipline (inherited from parent)
```

**Measures (Facts):**
- `string_score` - Score for this string
- `stage_number` - Which stage (1, 2, 3, etc.)
- `string_number` - Which string within stage (1-5 typically)
- `string_x_count` - X-ring hits in this string

### 🎯 fact_schedule (Schedule Fact Table)
**Purpose**: When and where shooting events occur
**Grain**: One record per scheduled slot
**Records**: ~500+ schedule entries

**Dimensional Relationships:**
```
fact_schedule
├── competition_key → dim_competition (which competition)
├── slot_key → dim_slot (the scheduled slot)
├── range_key → dim_range (which firing range)
└── discipline_key → dim_discipline (type of shooting scheduled)
```

**Measures (Facts):**
- `start_time` - When slot begins
- `duration_minutes` - How long slot lasts
- `capacity` - Maximum participants

## Dimension Tables (Star Points)

### 📋 dim_competition
**Purpose**: Competition/match information
**Primary Key**: `competition_key` (surrogate)
**Natural Key**: `competition_id_nat` (from source system)
**Attributes**: name, description, start_date, end_date, location

### 📋 dim_team
**Purpose**: Shooting team information
**Primary Key**: `team_key` (surrogate)
**Natural Key**: `team_id_nat` (from source system)
**Attributes**: name, organization, state, region, contact_info

### 📋 dim_athlete
**Purpose**: Individual shooter information
**Primary Key**: `athlete_key` (surrogate)
**Natural Key**: `athlete_id_nat` (from source system)
**Attributes**: fname, lname, age, gender, classification, contact_info

### 📋 dim_slot
**Purpose**: Time/location slots for events
**Primary Key**: `slot_key` (surrogate)
**Natural Key**: `slot_rid_nat` (critical: equals schedule.rid from raw data)
**Attributes**: name, description, relay_number, squad_number

### 📋 dim_discipline
**Purpose**: Types of shooting disciplines
**Primary Key**: `discipline_key` (surrogate)
**Natural Key**: `discipline_id_nat` (from source system)
**Attributes**: name, description, scoring_method, target_type

### 📋 dim_range
**Purpose**: Physical firing ranges
**Primary Key**: `range_key` (surrogate)
**Natural Key**: `range_id_nat` (from source system)
**Attributes**: name, description, location, capacity

## Bridge Tables (Many-to-Many Relationships)

### 🌉 bridge_team_athlete
**Purpose**: Handles team membership over time (athletes can switch teams)
**Grain**: team + athlete + time_period combination

**Relationships:**
```
bridge_team_athlete
├── team_key → dim_team
├── athlete_key → dim_athlete
├── competition_key → dim_competition (when membership was active)
├── start_date (when athlete joined team)
└── end_date (when athlete left team, NULL if still active)
```

## Key Relationship Patterns

### 🔗 Primary Star Schema Join Pattern
```sql
-- Main fact with all dimensions
SELECT 
    comp.name as competition,
    team.name as team,
    ath.fname || ' ' || ath.lname as athlete,
    slot.name as slot,
    disc.name as discipline,
    fe.spp_final as final_score
FROM fact_entry fe
    JOIN dim_competition comp ON fe.competition_key = comp.competition_key
    JOIN dim_team team ON fe.team_key = team.team_key  
    JOIN dim_athlete ath ON fe.athlete_key = ath.athlete_key
    JOIN dim_slot slot ON fe.slot_key = slot.slot_key
    JOIN dim_discipline disc ON fe.discipline_key = disc.discipline_key
```

### 🔗 Entry → Strings Drill-Down Pattern
```sql
-- From entry to individual string performance
SELECT 
    fe.entry_id,
    ath.fname || ' ' || ath.lname as athlete,
    fe.spp_final as total_score,
    fes.stage_number,
    fes.string_number,
    fes.string_score
FROM fact_entry fe
    JOIN dim_athlete ath ON fe.athlete_key = ath.athlete_key
    JOIN fact_entry_strings fes ON fe.entry_id = fes.entry_id
ORDER BY fe.entry_id, fes.stage_number, fes.string_number
```

### 🔗 Schedule → Performance Pattern
```sql
-- From scheduled events to actual performance
SELECT 
    fs.start_time,
    slot.name as scheduled_slot,
    disc.name as discipline,
    COUNT(fe.entry_id) as actual_participants,
    AVG(fe.spp_final) as avg_score
FROM fact_schedule fs
    JOIN dim_slot slot ON fs.slot_key = slot.slot_key
    JOIN dim_discipline disc ON fs.discipline_key = disc.discipline_key
    LEFT JOIN fact_entry fe ON fs.slot_key = fe.slot_key 
        AND fs.discipline_key = fe.discipline_key
GROUP BY fs.start_time, slot.name, disc.name
```

## Critical Join Keys

### ⚠️ Most Important Relationship
**`slot_key`** is the **critical join** between schedule and performance:
- `dim_slot.slot_rid_nat` = `schedule.rid` from raw data
- `fact_entry.slot_key` links to this same slot
- This connects "when was it scheduled" to "who actually shot"

### 🔑 Surrogate Key Benefits
- **Performance**: Integer joins faster than string/UUID joins
- **Stability**: Source system IDs can change, surrogate keys don't
- **History**: Can track changes to dimension attributes over time
- **Consistency**: All keys follow same pattern (`table_key`)

## Data Volume Relationships

**Explosion Pattern (1 → Many):**
- 1 Competition → 50-200 Slots → 5,000-15,000 Entries → 100,000-300,000 Strings
- 1 Team → 10-50 Athletes → 100-500 Entries per competition
- 1 Athlete → 1-5 Entries per competition → 20-100 Strings per entry

**Aggregation Pattern (Many → 1):**
- Bridge tables allow proper team/athlete history tracking
- Fact tables roll up to dimension attributes for reporting
- String-level detail aggregates to entry-level totals

This star schema enables efficient analytics queries while maintaining data integrity and performance.
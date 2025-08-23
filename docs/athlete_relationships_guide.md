# Understanding Foreign Key Fields in dim_athlete

## What You're Seeing in the Screenshot

When you looked at `dim_athlete` and saw:
- `public.bridge_team_athlete` 
- `public.fact_entry`

These are **NOT actual column names** in the table. They are **reference indicators** showing that this athlete record has relationships with other tables.

## What These Mean

### 🔗 `public.bridge_team_athlete`
**Purpose**: Shows this athlete has team membership records
**What it does**: Links athletes to teams they've been on over time
**Why it exists**: Athletes can switch teams, so we need history

**Table Structure**:
```sql
bridge_team_athlete
├── bridge_id (PK)
├── athlete_key (FK) → dim_athlete
├── team_key (FK) → dim_team  
├── competition_key (FK) → dim_competition
├── start_date (when they joined)
├── end_date (when they left, NULL if still active)
└── created_at, updated_at
```

### 🔗 `public.fact_entry`
**Purpose**: Shows this athlete has performance records
**What it does**: Links athletes to their competition entries and scores
**Why it exists**: This is where actual shooting performance data lives

**Table Structure**:
```sql
fact_entry
├── entry_id (PK)
├── athlete_key (FK) → dim_athlete
├── team_key (FK) → dim_team
├── competition_key (FK) → dim_competition
├── slot_key (FK) → dim_slot
├── discipline_key (FK) → dim_discipline
├── spp_final (final score)
├── spp_x_count (X-ring hits)
└── entry_dt (when they shot)
```

## How to Use These Relationships

### 1. Find an Athlete's Teams
```sql
-- What teams has this athlete been on?
SELECT 
    da.fname || ' ' || da.lname as athlete_name,
    dt.name as team_name,
    bta.start_date,
    bta.end_date,
    CASE WHEN bta.end_date IS NULL THEN 'Current' ELSE 'Past' END as status
FROM dim_athlete da
    JOIN bridge_team_athlete bta ON da.athlete_key = bta.athlete_key
    JOIN dim_team dt ON bta.team_key = dt.team_key
WHERE da.athlete_key = 123  -- Replace with actual athlete_key
ORDER BY bta.start_date DESC;
```

### 2. Find an Athlete's Performance History
```sql
-- What are this athlete's competition results?
SELECT 
    da.fname || ' ' || da.lname as athlete_name,
    dc.name as competition,
    dd.name as discipline,
    fe.spp_final as final_score,
    fe.entry_dt::date as competition_date
FROM dim_athlete da
    JOIN fact_entry fe ON da.athlete_key = fe.athlete_key
    LEFT JOIN dim_competition dc ON fe.competition_key = dc.competition_key
    LEFT JOIN dim_discipline dd ON fe.discipline_key = dd.discipline_key
WHERE da.athlete_key = 123  -- Replace with actual athlete_key
ORDER BY fe.entry_dt DESC;
```

### 3. Find Team Roster
```sql
-- Who's on this team right now?
SELECT 
    dt.name as team_name,
    da.fname || ' ' || da.lname as athlete_name,
    da.email
FROM dim_team dt
    JOIN bridge_team_athlete bta ON dt.team_key = bta.team_key
    JOIN dim_athlete da ON bta.athlete_key = da.athlete_key
WHERE dt.team_key = 456  -- Replace with actual team_key
    AND bta.end_date IS NULL  -- Still active
ORDER BY da.lname, da.fname;
```

### 4. Athlete Performance Summary
```sql
-- Summary of athlete's shooting performance
SELECT 
    da.fname || ' ' || da.lname as athlete_name,
    COUNT(fe.entry_id) as total_entries,
    COUNT(DISTINCT fe.competition_key) as competitions_entered,
    COUNT(DISTINCT fe.discipline_key) as disciplines_shot,
    AVG(fe.spp_final) as avg_score,
    MAX(fe.spp_final) as best_score,
    MIN(fe.entry_dt) as first_competition,
    MAX(fe.entry_dt) as latest_competition
FROM dim_athlete da
    JOIN fact_entry fe ON da.athlete_key = fe.athlete_key
WHERE da.athlete_key = 123  -- Replace with actual athlete_key
    AND fe.spp_final > 0
GROUP BY da.athlete_key, da.fname, da.lname;
```

## Practical Investigation Steps

### Step 1: Pick an Athlete to Explore
```sql
-- Find an athlete with data
SELECT 
    athlete_key,
    fname || ' ' || lname as name,
    email
FROM dim_athlete 
WHERE fname IS NOT NULL 
    AND athlete_key IN (
        SELECT DISTINCT athlete_key 
        FROM fact_entry 
        WHERE spp_final > 0
    )
LIMIT 10;
```

### Step 2: Explore Their Relationships
Use the athlete_key from Step 1 in the queries above.

### Step 3: Understand the Data Patterns
- How many teams has this athlete been on?
- How many competitions have they entered?
- What disciplines do they shoot?
- How has their performance changed over time?

## Key Points

1. **Bridge tables handle many-to-many relationships** - athletes can be on multiple teams
2. **Fact tables contain the actual measurements** - scores, dates, performance metrics
3. **Dimension tables contain the descriptive attributes** - names, addresses, team info
4. **Foreign keys create the links** - athlete_key connects everything together
5. **Time-based relationships** - bridge tables track when relationships started/ended

The screenshot you saw is showing that this particular athlete has connections to both team membership data and performance data - which means you can explore both their team history and their shooting results.
# Silver Layer Entity Relationship Diagram

## Complete Star Schema Relationships (Silver Layer Only)

```
                        DIMENSIONAL STAR SCHEMA
                     (Silver Layer - PostgreSQL)

         ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
         │ dim_competition │    │   dim_team      │    │  dim_athlete    │
         │                 │    │                 │    │                 │
         │ competition_key ├────┤ team_key        ├────┤ athlete_key     │
         │ competition_id_nat    │ team_id_nat     │    │ athlete_id_nat  │
         │ name            │    │ name            │    │ fname, lname    │
         │ start_date      │    │ organization    │    │ age, gender     │
         │ end_date        │    │ state, region   │    │ classification  │
         └─────────────────┘    └─────────────────┘    └─────────────────┘
                   │                       │                       │
                   │                       │                       │
            ┌──────┴───────────────────────┴───────────────────────┴──────┐
            │                                                             │
            │                    FACT_ENTRY                               │
            │                   (MAIN FACT)                               │
            │  ┌─────────────────────────────────────────────────────┐    │
            │  │ entry_id (PK)                                       │    │
            │  │ competition_key (FK) ──→ dim_competition           │    │
            │  │ team_key (FK) ──────────→ dim_team                 │    │
            │  │ athlete_key (FK) ───────→ dim_athlete              │    │
            │  │ slot_key (FK) ──────────→ dim_slot                 │    │
            │  │ discipline_key (FK) ────→ dim_discipline           │    │
            │  │                                                     │    │
            │  │ MEASURES:                                           │    │
            │  │ spp_final, spp_x_count, entry_dt                  │    │
            │  └─────────────────────────────────────────────────────┘    │
            └──────────────────────────┬──────────────────────────────────┘
                                       │
                                       │ 1:many
                                       ▼
                    ┌─────────────────────────────────────────────────────┐
                    │               FACT_ENTRY_STRINGS                    │
                    │                 (DETAIL FACT)                       │
                    │  ┌───────────────────────────────────────────────┐  │
                    │  │ entry_string_id (PK)                          │  │
                    │  │ entry_id (FK) ──→ fact_entry                  │  │
                    │  │ competition_key (FK) ──→ dim_competition      │  │
                    │  │ team_key (FK) ──────────→ dim_team            │  │
                    │  │ athlete_key (FK) ───────→ dim_athlete         │  │
                    │  │ slot_key (FK) ──────────→ dim_slot            │  │
                    │  │ discipline_key (FK) ────→ dim_discipline      │  │
                    │  │                                               │  │
                    │  │ MEASURES:                                     │  │
                    │  │ stage_number, string_number                   │  │
                    │  │ string_score, string_x_count                  │  │
                    │  └───────────────────────────────────────────────┘  │
                    └─────────────────────────────────────────────────────┘

         ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
         │   dim_slot      │    │ dim_discipline  │    │   dim_range     │
         │                 │    │                 │    │                 │
         │ slot_key        ├────┤ discipline_key  ├────┤ range_key       │
         │ slot_rid_nat    │    │ discipline_id_nat    │ range_id_nat    │
         │ name            │    │ name            │    │ name            │
         │ description     │    │ description     │    │ description     │
         │ relay_number    │    │ scoring_method  │    │ location        │
         │ squad_number    │    │ target_type     │    │ capacity        │
         └─────────────────┘    └─────────────────┘    └─────────────────┘
                   │                       │                       │
                   │                       │                       │
            ┌──────┴───────────────────────┴───────────────────────┴──────┐
            │                                                             │
            │                   FACT_SCHEDULE                             │
            │                (INDEPENDENT FACT)                           │
            │  ┌─────────────────────────────────────────────────────┐    │
            │  │ schedule_id (PK)                                    │    │
            │  │ competition_key (FK) ──→ dim_competition           │    │
            │  │ slot_key (FK) ──────────→ dim_slot                 │    │
            │  │ range_key (FK) ─────────→ dim_range                │    │
            │  │ discipline_key (FK) ────→ dim_discipline           │    │
            │  │                                                     │    │
            │  │ MEASURES:                                           │    │
            │  │ start_time, duration_minutes, capacity             │    │
            │  └─────────────────────────────────────────────────────┘    │
            └─────────────────────────────────────────────────────────────┘

                        ┌─────────────────────────────────────┐
                        │        BRIDGE_TEAM_ATHLETE          │
                        │        (MANY-TO-MANY BRIDGE)        │
                        │  ┌───────────────────────────────┐  │
                        │  │ bridge_id (PK)                │  │
                        │  │ team_key (FK) ──→ dim_team    │  │
                        │  │ athlete_key (FK) ─→ dim_athlete  │
                        │  │ competition_key (FK) ─→ dim_competition │
                        │  │ start_date, end_date          │  │
                        │  │ is_active                     │  │
                        │  └───────────────────────────────┘  │
                        └─────────────────────────────────────┘

```

## Key Relationship Patterns

### 🎯 **Primary Star Pattern** (Most Common Query)
```sql
-- Join fact_entry to all dimensions
SELECT athlete.fname, team.name, competition.name, entry.spp_final
FROM fact_entry entry
  JOIN dim_athlete athlete ON entry.athlete_key = athlete.athlete_key
  JOIN dim_team team ON entry.team_key = team.team_key
  JOIN dim_competition comp ON entry.competition_key = comp.competition_key
  JOIN dim_slot slot ON entry.slot_key = slot.slot_key
  JOIN dim_discipline disc ON entry.discipline_key = disc.discipline_key
```

### 🔗 **Parent-Child Fact Pattern** (Entry → Strings)
```sql
-- From summary to detail performance
SELECT entry.entry_id, entry.spp_final, strings.string_score
FROM fact_entry entry
  JOIN fact_entry_strings strings ON entry.entry_id = strings.entry_id
ORDER BY entry.entry_id, strings.stage_number, strings.string_number
```

### 📅 **Schedule-Performance Pattern** (Planning vs Reality)
```sql
-- Compare scheduled slots to actual participation
SELECT schedule.start_time, slot.name, COUNT(entry.entry_id) as participants
FROM fact_schedule schedule
  JOIN dim_slot slot ON schedule.slot_key = slot.slot_key
  LEFT JOIN fact_entry entry ON schedule.slot_key = entry.slot_key
GROUP BY schedule.start_time, slot.name
```

### 🔄 **Bridge Table Pattern** (Team History)
```sql
-- Track athlete team changes over time
SELECT athlete.fname, team.name, bridge.start_date, bridge.end_date
FROM bridge_team_athlete bridge
  JOIN dim_athlete athlete ON bridge.athlete_key = athlete.athlete_key
  JOIN dim_team team ON bridge.team_key = team.team_key
WHERE bridge.is_active = true
```

## Cardinality Summary

| Relationship | Type | Example |
|-------------|------|---------|
| dim_* → fact_entry | **1:many** | 1 athlete → many entries |
| fact_entry → fact_entry_strings | **1:many** | 1 entry → ~20 strings |
| dim_team ↔ dim_athlete | **many:many** | via bridge_team_athlete |
| dim_slot ← fact_schedule | **1:many** | 1 slot → many schedules |
| All _key fields | **surrogate** | Integer PKs for performance |

## Critical Join Points

1. **`slot_key`** - Links schedule planning to actual performance
2. **`entry_id`** - Connects summary scores to detailed string performance  
3. **`competition_key`** - Time-bounds all data to specific matches
4. **Bridge tables** - Handle many-to-many without data duplication

## Data Volumes (Current)
- **fact_entry**: 64,294 records (main grain)
- **fact_entry_strings**: 1,285,820 records (20× detail expansion)
- **fact_schedule**: ~500 records (planning data)
- **Dimensions**: 512 competitions, 228 teams, 5,159 athletes, 73,078 slots, 15 disciplines
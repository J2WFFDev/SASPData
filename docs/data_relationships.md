## SASP Data Model - Table Relationships & Data Flow

### 🔄 **BRONZE → SILVER TRANSFORMATION FLOW**

```
RAW SCOREBOARD (522 records)
│
├─ scoreboard.teams[].ent_id ────────────┐
├─ scoreboard.teams[].disciplines[].athletes[].comp_id ──┐
├─ scoreboard.teams[].disciplines[].athletes[].disc_id ──┐
├─ scoreboard.teams[].disciplines[].athletes[].ath_id ───┐
├─ scoreboard.teams[].disciplines[].athletes[].slot_id ──┐
│                                                        │
▼                                                        │
FACT_ENTRY (64,294 records)                             │
│                                                        │
├─ team_key ◄─────────────────┬─ DIM_TEAM (228)         │
├─ competition_key ◄──────────┼─ DIM_COMPETITION (512)  │
├─ discipline_key ◄───────────┼─ DIM_DISCIPLINE (15)    │
├─ athlete_key ◄──────────────┼─ DIM_ATHLETE (5,159)    │
├─ slot_key ◄─────────────────┼─ DIM_SLOT (73,078)      │
│                             │                         │
└─ spp_final, station, etc.   │                         │
                              │                         │
▼                             │                         │
FACT_ENTRY_STRINGS (1.28M records)                     │
└─ entry_id, stage_no, string_no, time_value, etc.     │
                                                        │
                                                        │
RAW SCHEDULE (521 records) ─────────────────────────────┘
│
├─ schedule.slots[].rid ═══════════════════════════════════ CRITICAL JOIN!
│                                                          slot_id = rid
├─ schedule.slots[].lineup[] ──┐
│                              │
▼                              ▼
DIM_SLOT (73,078)         FACT_SCHEDULE (128,238)
│
└─ slot_key ◄─── Used by fact_entry.slot_key


RAW TEAMS (150 records)
│
├─ teams.id ──────────────────┐
├─ teams.home_range ──────────┼─┐
│                             │ │
▼                             │ │
DIM_TEAM (228)                │ │
└─ team_id_nat, name, org     │ │
                              │ │
                              │ │
RAW COMPETITION (6 records) ──┘ │
│                               │
├─ competition.id ─────────────┐ │
├─ competition.range.id ───────┼─┘
│                             │
▼                             ▼
DIM_COMPETITION (512)    DIM_RANGE (94)
└─ comp_id_nat, name     └─ range_id_nat, name
```

### 🎯 **CRITICAL DATA RELATIONSHIPS**

**1. THE CENTRAL JOIN: `slot_id = rid`**
- `scoreboard.teams[].disciplines[].athletes[].slot_id` 
- `schedule.slots[].rid`
- This is THE key relationship that connects performance data to scheduling

**2. TEAM HIERARCHY:**
```
DIM_TEAM ──┐
           ├─ fact_entry.team_key
           └─ bridge_team_athlete (future many-to-many)
              └─ DIM_ATHLETE
```

**3. PERFORMANCE DATA UNPIVOTING:**
```
scoreboard.athletes[].spp1_1 → fact_entry_strings (stage=1, string=1)
scoreboard.athletes[].spp1_2 → fact_entry_strings (stage=1, string=2)
...
scoreboard.athletes[].spp4_5 → fact_entry_strings (stage=4, string=5)
```

### 📊 **DATA VOLUME ANALYSIS**

| Table | Records | Purpose | Key Fields |
|-------|---------|---------|------------|
| **raw_scoreboard** | 522 | Source of truth for performance | teams[].disciplines[].athletes[] |
| **fact_entry** | 64,294 | One record per athlete performance | All dimension keys + spp_final |
| **fact_entry_strings** | 1,285,820 | 20 strings per entry (4 stages × 5 strings) | stage_no, string_no, time_value |
| **dim_slot** | 73,078 | Schedule slots from raw_schedule | slot_rid_nat = rid |
| **dim_athlete** | 5,159 | Unique athletes across all competitions | ath_id_nat |

### 🔍 **NESTED DATA EXTRACTION PATTERNS**

**Scoreboard Processing:**
```sql
-- Extract teams
jsonb_array_elements(payload->'teams') AS team

-- Extract disciplines per team  
jsonb_array_elements(team->'disciplines') AS discipline

-- Extract athletes per discipline
jsonb_array_elements(discipline->'athletes') AS athlete

-- Performance data (20 time values per athlete)
athlete->>'spp1_1', athlete->>'spp1_2', ..., athlete->>'spp4_5'
```

**Schedule Processing:**
```sql  
-- Extract slots
jsonb_array_elements(payload->'slots') AS slot

-- Extract lineup per slot
jsonb_array_elements(slot->'lineup') AS lineup_entry

-- Extract flights per slot
jsonb_array_elements(slot->'flights') AS flight
```
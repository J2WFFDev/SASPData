# SASP Data Warehouse Model

## Overview
This document describes the silver layer transformations for SASP shooting competition data, transforming raw JSON ingests into a normalized dimensional model suitable for analytics using **PostgreSQL + Python**.

## Architecture
```
Raw Layer (Bronze) → Silver Layer → Analytics
JSON from APIs    → Dims + Facts → Reports/Dashboards
```

## Technology Stack
- **Database**: PostgreSQL 16 with JSONB support
- **ETL**: Python 3.12 with psycopg2, requests, python-dotenv, pyyaml
- **Automation**: PowerShell scripts for Windows environment

## Data Flow
1. **Raw Tables** (existing): `raw_teams`, `raw_schedule`, `raw_scoreboard`, `raw_competition`
2. **Silver Dimensions**: `dim_team`, `dim_athlete`, `dim_competition`, `dim_discipline`, `dim_slot`, `dim_range`
3. **Silver Facts**: `fact_entry`, `fact_entry_strings`, `fact_schedule`

## Key Relationships (FK Matrix)

| Source | Target | Key Relationship |
|--------|---------|------------------|
| `scoreboard.comp_id` | `dim_competition.competition_id_nat` | Competition reference |
| `scoreboard.ent_id` | `dim_team.team_id_nat` | Team reference |
| `scoreboard.disc_id` | `dim_discipline.discipline_id_nat` | Discipline reference |
| `scoreboard.slot_id` | `dim_slot.slot_rid_nat` | Schedule slot reference |
| `scoreboard.ath_id` | `dim_athlete.ath_id_nat` | Athlete reference |
| `schedule.slot.rid` | `scoreboard.slot_id` | **Critical join point** |
| `competition.range.id` | `dim_range.range_id_nat` | Range hosting |
| `team.home_range_id` | `dim_range.range_id_nat` | Team home range |

## Unified Schema (ASCII)

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   dim_team      │    │ dim_competition │    │   dim_range     │
│─────────────────│    │─────────────────│    │─────────────────│
│ team_key (PK)   │    │ comp_key (PK)   │    │ range_key (PK)  │
│ team_id_nat     │────┤ comp_id_nat     │    │ range_id_nat    │
│ name            │    │ name            │    │ name            │
│ org             │    │ stage_one...    │    │ type_id         │
│ state_id_nat    │    │ start_date      │    │ contact         │
│ home_range_id ──┼────┤ range_id_nat ───┼────┤ phone           │
└─────────────────┘    └─────────────────┘    │ email           │
                                              └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  dim_athlete    │    │   fact_entry    │    │   dim_slot      │
│─────────────────│    │─────────────────│    │─────────────────│
│ athlete_key(PK) │────┤ athlete_key(FK) │    │ slot_key (PK)   │
│ ath_id_nat      │    │ team_key (FK) ──┼────┤ slot_rid_nat    │
│ fname, lname    │    │ comp_key (FK)   │    │ number          │
│ gender          │    │ disc_key (FK)   │    │ name            │
│ bdate           │    │ slot_key (FK) ──┼────┤ discipline_name │
│ address         │    │ station         │    │ stage           │
│ city, state     │    │ spp_final       │    │ location_name   │
│ email           │    │ is_valid        │    │ expanded        │
└─────────────────┘    │ dq_tag, dnf_tag │    └─────────────────┘
                       │ proc_pen        │
                       └─────────────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │ fact_entry_strings  │
                   │─────────────────────│
                   │ entry_string_id(PK) │
                   │ entry_id (FK)       │
                   │ stage_no (1-4)      │
                   │ string_no (1-5)     │
                   │ time_value          │
                   │ penalty_value       │
                   │ total_value         │
                   └─────────────────────┘
```

## Critical Validation Rules
1. **slot_id == rid**: `scoreboard.slot_id` must equal `schedule.slot.rid`
2. **Natural Key Uniqueness**: All `*_id_nat` fields must be unique within their dimension
3. **Referential Integrity**: All foreign keys must resolve to valid dimension records
4. **Data Completeness**: All critical competition/team/athlete data must be present before fact loading

## Transform Sequence
1. Load dimensions (competition, range, team, athlete, discipline, slot)
2. Validate referential integrity 
3. Load facts (entry, entry_strings, schedule)
4. Run quality checks and aggregation validation

## File Organization
- `docs/` - This documentation
- `sql/` - DDL for silver tables
- `silver/` - Python transformation scripts
- `scripts/` - Orchestration and utilities
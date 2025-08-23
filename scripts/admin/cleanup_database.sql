-- Database cleanup script to remove derived tables
-- Keeps only raw_* tables and essential tracking tables
-- Generated: 2025-08-21

-- Drop derived fact tables
DROP TABLE IF EXISTS fact_penalty CASCADE;
DROP TABLE IF EXISTS fact_stage_series_result CASCADE;
DROP TABLE IF EXISTS fact_stage_total CASCADE;
DROP TABLE IF EXISTS fact_team_snapshot CASCADE;

-- Drop dimension tables
DROP TABLE IF EXISTS dim_athlete CASCADE;
DROP TABLE IF EXISTS dim_classification CASCADE;
DROP TABLE IF EXISTS dim_competition CASCADE;
DROP TABLE IF EXISTS dim_contact CASCADE;
DROP TABLE IF EXISTS dim_discipline CASCADE;
DROP TABLE IF EXISTS dim_person CASCADE;
DROP TABLE IF EXISTS dim_range CASCADE;
DROP TABLE IF EXISTS dim_registration_type CASCADE;
DROP TABLE IF EXISTS dim_scoreboard CASCADE;
DROP TABLE IF EXISTS dim_series CASCADE;
DROP TABLE IF EXISTS dim_stage CASCADE;
DROP TABLE IF EXISTS dim_state CASCADE;
DROP TABLE IF EXISTS dim_team CASCADE;

-- Drop silver/processed tables
DROP TABLE IF EXISTS silver_property CASCADE;
DROP TABLE IF EXISTS silver_schedule CASCADE;
DROP TABLE IF EXISTS silver_schedule_slot CASCADE;
DROP TABLE IF EXISTS silver_slot_flight CASCADE;
DROP TABLE IF EXISTS silver_slot_lineup CASCADE;

-- Drop mapping/bridge tables
DROP TABLE IF EXISTS map_roster CASCADE;
DROP TABLE IF EXISTS map_team_members CASCADE;
DROP TABLE IF EXISTS bridge_competition_invited_team CASCADE;

-- Drop other derived tables
DROP TABLE IF EXISTS competition_stage CASCADE;

-- Keep these essential tables:
-- raw_competition (source data)
-- raw_schedule (source data)
-- raw_scoreboard (source data)  
-- raw_teams (source data)
-- url_status (API endpoint tracking)

SELECT 'Database cleanup completed - only raw tables and url_status remain' as status;
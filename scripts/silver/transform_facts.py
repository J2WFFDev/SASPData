#!/usr/bin/env python3
"""
Transform raw scoreboard and schedule data into silver fact tables.
Processes normalized scoreboard entries into fact_entry and fact_entry_strings.
"""

import json
import logging
from typing import List, Dict, Any, Optional
from transform_utils import (
    get_db_conn, logger, get_dimension_key, safe_int, safe_float, safe_bool,
    to_date_key, to_time_key, ensure_date_dimension, ensure_time_dimension
)

def transform_scoreboard_to_facts(conn):
    """Transform raw_scoreboard data into fact_entry and fact_entry_strings"""
    logger.info("Transforming scoreboard to facts...")
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_scoreboard WHERE payload IS NOT NULL")
        
        entry_count = 0
        string_count = 0
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                teams = data.get('teams', [])
                
                for team in teams:
                    disciplines = team.get('disciplines', [])
                    
                    for discipline in disciplines:
                        athletes = discipline.get('athletes', [])
                        
                        for athlete in athletes:
                            entry_id = transform_single_entry(conn, athlete)
                            if entry_id:
                                entry_count += 1
                                strings = transform_entry_strings(conn, entry_id, athlete)
                                string_count += strings
                                
            except Exception as e:
                logger.error(f"Error processing scoreboard payload: {e}")
                
        logger.info(f"Transformed {entry_count} entries with {string_count} strings")
    
    conn.commit()

def transform_single_entry(conn, athlete_data: Dict[str, Any]) -> Optional[int]:
    """Transform a single athlete entry into fact_entry"""
    
    try:
        # Get dimension keys
        comp_key = get_dimension_key(conn, 'dim_competition', 'competition_id_nat', 
                                   safe_int(athlete_data.get('comp_id')))
        team_key = get_dimension_key(conn, 'dim_team', 'team_id_nat',
                                   safe_int(athlete_data.get('ent_id')))
        disc_key = get_dimension_key(conn, 'dim_discipline', 'discipline_id_nat',
                                   safe_int(athlete_data.get('disc_id')))
        athlete_key = get_dimension_key(conn, 'dim_athlete', 'ath_id_nat',
                                      safe_int(athlete_data.get('ath_id')))
        slot_key = get_dimension_key(conn, 'dim_slot', 'slot_rid_nat',
                                   safe_int(athlete_data.get('slot_id')))
        
        # Convert dates and times
        date_key = to_date_key(athlete_data.get('date'))
        time_key = to_time_key(athlete_data.get('time'))
        reg_date_key = to_date_key(athlete_data.get('reg_date', '').split(' ')[0] if athlete_data.get('reg_date') else None)
        
        # Ensure date/time dimensions exist
        if date_key:
            ensure_date_dimension(conn, date_key)
        if time_key:
            ensure_time_dimension(conn, time_key)
        if reg_date_key:
            ensure_date_dimension(conn, reg_date_key)
        
        # Prepare fact data
        fact_data = {
            'competition_key': comp_key,
            'team_key': team_key,
            'discipline_key': disc_key,
            'athlete_key': athlete_key,
            'slot_key': slot_key,
            'station': safe_int(athlete_data.get('station')),
            'number': safe_int(athlete_data.get('number')),
            'lid': safe_int(athlete_data.get('lid')),
            'date_key': date_key,
            'time_key': time_key,
            'location': athlete_data.get('location'),
            'flight': athlete_data.get('flight'),
            'manual_scoring': safe_bool(athlete_data.get('manual_scoring')),
            'is_valid': safe_bool(athlete_data.get('is_valid', True)),
            'eligible': safe_bool(athlete_data.get('eligible', True)),
            'dq_tag': safe_bool(athlete_data.get('dq_tag')),
            'dnf_tag': safe_bool(athlete_data.get('dnf_tag')),
            'proc_pen': safe_float(athlete_data.get('proc_pen')),
            'spp_final': safe_float(athlete_data.get('spp_final')),
            'reg_date_key': reg_date_key,
            'reg_who': athlete_data.get('reg_who')
        }
        
        # Insert fact_entry
        with conn.cursor() as cur:
            fields = [k for k, v in fact_data.items() if v is not None]
            values = [v for k, v in fact_data.items() if v is not None]
            placeholders = ', '.join(['%s'] * len(values))
            field_names = ', '.join(fields)
            
            cur.execute(f"""
                INSERT INTO fact_entry ({field_names})
                VALUES ({placeholders})
                RETURNING entry_id
            """, values)
            
            result = cur.fetchone()
            entry_id = result[0] if result else None
            
            if entry_id:
                logger.debug(f"Created fact_entry {entry_id} for athlete {athlete_data.get('ath_id')}")
            
            return entry_id
            
    except Exception as e:
        logger.error(f"Error transforming entry for athlete {athlete_data.get('ath_id')}: {e}")
        return None

def transform_entry_strings(conn, entry_id: int, athlete_data: Dict[str, Any]) -> int:
    """Transform stage/string data into fact_entry_strings"""
    
    string_count = 0
    
    try:
        with conn.cursor() as cur:
            # Process stage 1-4, string 1-5 combinations
            for stage in range(1, 5):  # Stages 1-4
                for string in range(1, 6):  # Strings 1-5
                    
                    # Get the field values for this stage/string
                    time_field = f'spp{stage}_{string}'
                    pen_field = f'spp{stage}_pen{string}'
                    tot_field = f'spp{stage}_tot{string}'
                    
                    time_value = safe_float(athlete_data.get(time_field))
                    penalty_value = safe_float(athlete_data.get(pen_field))
                    total_value = safe_float(athlete_data.get(tot_field))
                    
                    # Only insert if at least one value exists
                    if time_value is not None or penalty_value is not None or total_value is not None:
                        cur.execute("""
                            INSERT INTO fact_entry_strings 
                            (entry_id, stage_no, string_no, time_value, penalty_value, total_value)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (entry_id, stage, string, time_value, penalty_value, total_value))
                        
                        string_count += 1
                        logger.debug(f"Created string record: entry {entry_id}, stage {stage}, string {string}")
        
        return string_count
        
    except Exception as e:
        logger.error(f"Error transforming strings for entry {entry_id}: {e}")
        return 0

def transform_schedule_to_facts(conn):
    """Transform raw_schedule data into fact_schedule"""
    logger.info("Transforming schedule to facts...")
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_schedule WHERE payload IS NOT NULL")
        
        schedule_count = 0
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                slots = data.get('slots', [])
                
                for slot in slots:
                    slot_rid = safe_int(slot.get('rid'))
                    if not slot_rid:
                        continue
                        
                    # Get slot dimension key
                    slot_key = get_dimension_key(conn, 'dim_slot', 'slot_rid_nat', slot_rid)
                    if not slot_key:
                        logger.warning(f"No dim_slot found for rid {slot_rid}")
                        continue
                    
                    # Get competition key (assume from context or extract from data)
                    comp_key = None  # TODO: Extract from schedule context if available
                    
                    # Process lineup entries
                    lineup = slot.get('lineup', [])
                    for lineup_entry in lineup:
                        lineup_id = safe_int(lineup_entry.get('lid'))
                        
                        fact_data = {
                            'competition_key': comp_key,
                            'slot_key': slot_key,
                            'lineup_id': lineup_id,
                            'station': safe_int(lineup_entry.get('station')),
                            'athlete_name': lineup_entry.get('name'),
                            'team_name': lineup_entry.get('team'),
                            'class_label': lineup_entry.get('class'),
                            'is_open': safe_bool(lineup_entry.get('is_open')),
                            'exists_flag': safe_bool(lineup_entry.get('exists', True))
                        }
                        
                        # Insert fact_schedule
                        fields = [k for k, v in fact_data.items() if v is not None]
                        values = [v for k, v in fact_data.items() if v is not None]
                        placeholders = ', '.join(['%s'] * len(values))
                        field_names = ', '.join(fields)
                        
                        cur.execute(f"""
                            INSERT INTO fact_schedule ({field_names})
                            VALUES ({placeholders})
                        """, values)
                        
                        schedule_count += 1
                        
            except Exception as e:
                logger.error(f"Error processing schedule payload: {e}")
        
        logger.info(f"Transformed {schedule_count} schedule entries")
    
    conn.commit()

def build_team_performance_aggregates(conn):
    """Build pre-aggregated team performance metrics"""
    logger.info("Building team performance aggregates...")
    
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agg_team_performance 
            (competition_key, team_key, discipline_key, total_athletes, valid_entries, 
             dq_count, dnf_count, avg_score, best_score, worst_score, total_penalties)
            SELECT 
                fe.competition_key,
                fe.team_key,
                fe.discipline_key,
                COUNT(DISTINCT fe.athlete_key) as total_athletes,
                COUNT(CASE WHEN fe.is_valid THEN 1 END) as valid_entries,
                COUNT(CASE WHEN fe.dq_tag THEN 1 END) as dq_count,
                COUNT(CASE WHEN fe.dnf_tag THEN 1 END) as dnf_count,
                AVG(CASE WHEN fe.is_valid AND NOT fe.dq_tag AND NOT fe.dnf_tag THEN fe.spp_final END) as avg_score,
                MIN(CASE WHEN fe.is_valid AND NOT fe.dq_tag AND NOT fe.dnf_tag THEN fe.spp_final END) as best_score,
                MAX(CASE WHEN fe.is_valid AND NOT fe.dq_tag AND NOT fe.dnf_tag THEN fe.spp_final END) as worst_score,
                SUM(COALESCE(fe.proc_pen, 0)) as total_penalties
            FROM fact_entry fe
            WHERE fe.competition_key IS NOT NULL 
              AND fe.team_key IS NOT NULL 
              AND fe.discipline_key IS NOT NULL
            GROUP BY fe.competition_key, fe.team_key, fe.discipline_key
            ON CONFLICT (competition_key, team_key, discipline_key) 
            DO UPDATE SET
                total_athletes = EXCLUDED.total_athletes,
                valid_entries = EXCLUDED.valid_entries,
                dq_count = EXCLUDED.dq_count,
                dnf_count = EXCLUDED.dnf_count,
                avg_score = EXCLUDED.avg_score,
                best_score = EXCLUDED.best_score,
                worst_score = EXCLUDED.worst_score,
                total_penalties = EXCLUDED.total_penalties,
                calculated_at = now()
        """)
        
        agg_count = cur.rowcount
        logger.info(f"Created/updated {agg_count} team performance aggregates")
    
    conn.commit()

def main():
    """Main transformation pipeline for facts"""
    logger.info("Starting silver fact transformations...")
    
    conn = get_db_conn()
    conn.autocommit = False  # Use transactions
    
    try:
        # Transform facts
        transform_scoreboard_to_facts(conn)
        transform_schedule_to_facts(conn)
        
        # Build aggregates
        build_team_performance_aggregates(conn)
        
        logger.info("All fact transformations completed successfully!")
        
    except Exception as e:
        logger.error(f"Fact transformation failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
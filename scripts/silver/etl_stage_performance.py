#!/usr/bin/env python3
"""Simple ETL to populate fact_stage_performance table with stage aggregation."""

import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('PGHOST'),
        port=os.getenv('PGPORT'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

def process_stage_aggregation(limit=100):
    """Process stage aggregation for a limited number of entries."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            print(f'🎯 Processing stage aggregation for up to {limit} entries...')
            
            # Get distinct entries to process
            cur.execute(f"""
                SELECT DISTINCT entry_id, competition_key, team_key, athlete_key, discipline_key, slot_key
                FROM fact_entry
                WHERE entry_id IN (
                    SELECT DISTINCT entry_id FROM fact_entry_strings LIMIT {limit}
                )
                ORDER BY entry_id;
            """)
            
            entries = cur.fetchall()
            print(f'📊 Found {len(entries)} entries to process')
            
            processed = 0
            for entry_id, comp_key, team_key, athlete_key, disc_key, slot_key in entries:
                
                # Get all stages for this entry
                cur.execute("""
                    SELECT stage_no, string_no, time_value, penalty_value, total_value
                    FROM fact_entry_strings
                    WHERE entry_id = %s
                    ORDER BY stage_no, string_no;
                """, (entry_id,))
                
                strings_data = cur.fetchall()
                
                # Group by stage
                stages = {}
                for stage_no, string_no, time_val, pen_val, total_val in strings_data:
                    if stage_no not in stages:
                        stages[stage_no] = []
                    stages[stage_no].append({
                        'string_no': string_no,
                        'time_value': float(time_val) if time_val else 0.0,
                        'penalty_value': float(pen_val) if pen_val else 0.0,
                        'total_value': float(total_val) if total_val else 0.0
                    })
                
                # Process each stage
                for stage_no, stage_strings in stages.items():
                    
                    # Map stage number to stage name for lookup
                    stage_names = {
                        1: 'GoFast',
                        2: 'Focus', 
                        3: 'SpeedTrap',
                        4: 'InOut'
                    }
                    
                    if stage_no not in stage_names:
                        print(f'⚠️  Unknown stage number {stage_no}, skipping...')
                        continue
                    
                    stage_name = stage_names[stage_no]
                    
                    # Get stage_key
                    cur.execute("SELECT get_stage_key(%s)", (stage_name,))
                    stage_key_result = cur.fetchone()
                    if not stage_key_result or stage_key_result[0] is None:
                        print(f'⚠️  No stage key found for stage {stage_name}, skipping...')
                        continue
                    
                    stage_key = stage_key_result[0]
                    
                    # Sort strings by total time (ascending), but exclude zero times
                    valid_strings = [s for s in stage_strings if s['total_value'] > 0]
                    valid_strings.sort(key=lambda x: x['total_value'])
                    
                    # Skip stages with no valid times
                    if not valid_strings:
                        print(f'⚠️  No valid times for stage {stage_name}, skipping...')
                        continue
                    
                    # Keep 4 fastest, drop slowest if we have 5
                    if len(valid_strings) == 5:
                        # Drop the slowest (last one after sorting)
                        fastest_4 = valid_strings[:4]
                        dropped_string = 5  # String number of the dropped one
                    else:
                        # Keep all we have
                        fastest_4 = valid_strings
                        dropped_string = None
                    
                    # Calculate totals
                    total_raw = sum(s['time_value'] for s in fastest_4)
                    total_total = sum(s['total_value'] for s in fastest_4)
                    total_penalties = sum(s['penalty_value'] for s in fastest_4)
                    
                    # Pad with zeros if less than 4 strings
                    while len(fastest_4) < 4:
                        fastest_4.append({
                            'time_value': 0.0,
                            'total_value': 0.0,
                            'penalty_value': 0.0
                        })
                    
                    # Insert into fact_stage_performance
                    cur.execute("""
                        INSERT INTO fact_stage_performance (
                            entry_id, stage_key, competition_key, team_key, athlete_key, 
                            discipline_key, slot_key, total_raw_time, total_total_time, 
                            total_penalties, strings_count, dropped_string_number,
                            string1_raw, string1_total, string1_penalties,
                            string2_raw, string2_total, string2_penalties,
                            string3_raw, string3_total, string3_penalties,
                            string4_raw, string4_total, string4_penalties
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        ) ON CONFLICT (entry_id, stage_key) DO NOTHING;
                    """, (
                        entry_id, stage_key, comp_key, team_key, athlete_key,
                        disc_key, slot_key, total_raw, total_total, total_penalties,
                        len(valid_strings), dropped_string,
                        fastest_4[0]['time_value'], fastest_4[0]['total_value'], fastest_4[0]['penalty_value'],
                        fastest_4[1]['time_value'], fastest_4[1]['total_value'], fastest_4[1]['penalty_value'],
                        fastest_4[2]['time_value'], fastest_4[2]['total_value'], fastest_4[2]['penalty_value'],
                        fastest_4[3]['time_value'], fastest_4[3]['total_value'], fastest_4[3]['penalty_value']
                    ))
                
                processed += 1
                if processed % 10 == 0:
                    print(f'  ✓ Processed {processed}/{len(entries)} entries...')
            
            conn.commit()
            
            # Check results
            cur.execute("SELECT COUNT(*) FROM fact_stage_performance;")
            stage_count = cur.fetchone()[0]
            
            print(f'\n✅ Stage aggregation complete!')
            print(f'📊 Total stage performance records: {stage_count}')
            
    except Exception as e:
        print(f'❌ Error: {e}')
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    process_stage_aggregation(limit=50)  # Start with 50 entries
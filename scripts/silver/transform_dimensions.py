#!/usr/bin/env python3
"""
Transform raw JSON data into silver dimension tables.
Processes raw_competition, raw_teams, raw_schedule data into normalized dimensions.
"""

import json
import logging
from typing import List, Dict, Any
from transform_utils import (
    get_db_conn, logger, upsert_dimension, safe_int, safe_float, 
    to_date_key, ensure_date_dimension
)

def transform_competitions(conn):
    """Transform raw_competition data into dim_competition"""
    logger.info("Transforming competitions...")
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_competition WHERE payload IS NOT NULL")
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                # Handle both single competition and array formats
                competitions = data.get('data', [data]) if 'data' in data else [data]
                
                for comp in competitions:
                    if not comp.get('id'):
                        continue
                        
                    dim_data = {
                        'name': comp.get('name'),
                        'org': comp.get('org'),
                        'type': comp.get('type'),
                        'status': comp.get('status'),
                        'shooting_style': comp.get('shooting_style'),
                        'stage_one': comp.get('stage_one'),
                        'stage_two': comp.get('stage_two'), 
                        'stage_three': comp.get('stage_three'),
                        'stage_four': comp.get('stage_four'),
                        'start_date': comp.get('start_date'),
                        'end_date': comp.get('end_date'),
                        'open_date': comp.get('open_date'),
                        'close_date': comp.get('close_date'),
                        'hosting_team_id_nat': safe_int(comp.get('hosting_team_id')),
                        'range_id_nat': safe_int(comp.get('range', {}).get('id') if comp.get('range') else None)
                    }
                    
                    comp_key = upsert_dimension(
                        conn, 'dim_competition', 'competition_id_nat', 
                        comp['id'], dim_data
                    )
                    
                    if comp_key:
                        # Ensure date dimensions exist for this competition
                        for date_field in ['start_date', 'end_date', 'open_date', 'close_date']:
                            date_key = to_date_key(comp.get(date_field))
                            if date_key:
                                ensure_date_dimension(conn, date_key)
                        
                        logger.debug(f"Processed competition {comp['id']}: {comp.get('name')}")
            
            except Exception as e:
                logger.error(f"Error processing competition: {e}")
    
    conn.commit()
    logger.info("Competition transformation complete")

def transform_ranges(conn):
    """Transform range data from competitions and teams into dim_range"""
    logger.info("Transforming ranges...")
    
    ranges_seen = set()
    
    # Extract ranges from competitions
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_competition WHERE payload IS NOT NULL")
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                competitions = data.get('data', [data]) if 'data' in data else [data]
                
                for comp in competitions:
                    range_info = comp.get('range')
                    if range_info and range_info.get('id') and range_info['id'] not in ranges_seen:
                        ranges_seen.add(range_info['id'])
                        
                        dim_data = {
                            'name': range_info.get('name'),
                            'type_id': safe_int(range_info.get('type_id')),
                            'contact': range_info.get('contact'),
                            'phone': range_info.get('phone'),
                            'email': range_info.get('email'),
                            'url': range_info.get('url')
                        }
                        
                        upsert_dimension(
                            conn, 'dim_range', 'range_id_nat',
                            range_info['id'], dim_data
                        )
            except Exception as e:
                logger.error(f"Error processing range from competition: {e}")
    
    # Extract ranges from teams (home_range_id)
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_teams WHERE payload IS NOT NULL")
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                # Teams payload can be single object or have 'data' array
                if 'home_range_id' in data and data.get('home_range_id') not in ranges_seen:
                    range_id = data['home_range_id']
                    if range_id:
                        ranges_seen.add(range_id)
                        
                        # Minimal range record (just ID, may be enriched later)
                        dim_data = {
                            'name': f'Range {range_id}',  # Placeholder name
                        }
                        
                        upsert_dimension(
                            conn, 'dim_range', 'range_id_nat',
                            range_id, dim_data
                        )
                        
            except Exception as e:
                logger.error(f"Error processing range from team: {e}")
    
    conn.commit()
    logger.info("Range transformation complete")

def transform_teams(conn):
    """Transform raw_teams data into dim_team"""
    logger.info("Transforming teams...")
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_teams WHERE payload IS NOT NULL")
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                # Handle single team object
                if not data.get('id'):
                    continue
                    
                dim_data = {
                    'name': data.get('name'),
                    'org': data.get('org'),
                    'paper_name': data.get('paper_name'),
                    'paper_email': data.get('paper_email'),
                    'state_id_nat': safe_int(data.get('state_id')),
                    'home_range_id_nat': safe_int(data.get('home_range_id'))
                }
                
                team_key = upsert_dimension(
                    conn, 'dim_team', 'team_id_nat',
                    data['id'], dim_data
                )
                
                if team_key:
                    logger.debug(f"Processed team {data['id']}: {data.get('name')}")
                    
            except Exception as e:
                logger.error(f"Error processing team: {e}")
    
    conn.commit()
    logger.info("Team transformation complete")

def transform_schedule_slots(conn):
    """Transform raw_schedule slot data into dim_slot"""
    logger.info("Transforming schedule slots...")
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_schedule WHERE payload IS NOT NULL")
        
        for (payload,) in cur.fetchall():
            try:
                if isinstance(payload, str):
                    data = json.loads(payload)
                else:
                    data = payload
                
                slots = data.get('slots', [])
                
                for slot in slots:
                    if not slot.get('rid'):
                        continue
                        
                    dim_data = {
                        'number': safe_int(slot.get('number')),
                        'name': slot.get('name'),
                        'stage': slot.get('stage'),
                        'discipline_name': slot.get('discipline'),
                        'location_name': data.get('location_name'),  # From parent schedule
                        'range_name': data.get('range_name'),        # From parent schedule
                        'expanded': slot.get('expanded', False)
                    }
                    
                    slot_key = upsert_dimension(
                        conn, 'dim_slot', 'slot_rid_nat',
                        slot['rid'], dim_data
                    )
                    
                    if slot_key:
                        logger.debug(f"Processed slot {slot['rid']}: {slot.get('name')}")
                        
            except Exception as e:
                logger.error(f"Error processing schedule slot: {e}")
    
    conn.commit()
    logger.info("Schedule slot transformation complete")

def extract_disciplines_from_scoreboard(conn):
    """Extract unique disciplines from scoreboard data into dim_discipline"""
    logger.info("Extracting disciplines from scoreboard...")
    
    disciplines_seen = set()
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_scoreboard WHERE payload IS NOT NULL")
        
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
                        disc_name = discipline.get('name', 'Unknown')
                        
                        # Extract disc_id and comp_id from athlete records
                        athletes = discipline.get('athletes', [])
                        for athlete in athletes:
                            disc_id = safe_int(athlete.get('disc_id'))
                            comp_id = safe_int(athlete.get('comp_id'))
                            
                            if disc_id and comp_id and (disc_id, comp_id) not in disciplines_seen:
                                disciplines_seen.add((disc_id, comp_id))
                                
                                dim_data = {
                                    'name': disc_name,
                                    'competition_id_nat': comp_id
                                }
                                
                                disc_key = upsert_dimension(
                                    conn, 'dim_discipline', 'discipline_id_nat',
                                    disc_id, dim_data
                                )
                                
                                if disc_key:
                                    logger.debug(f"Processed discipline {disc_id}: {disc_name}")
                                break  # Only need one athlete to get disc_id/comp_id for this discipline
                                
            except Exception as e:
                logger.error(f"Error extracting disciplines: {e}")
    
    conn.commit()
    logger.info("Discipline extraction complete")

def extract_athletes_from_scoreboard(conn):
    """Extract unique athletes from scoreboard data into dim_athlete"""
    logger.info("Extracting athletes from scoreboard...")
    
    athletes_seen = set()
    
    with conn.cursor() as cur:
        cur.execute("SELECT payload FROM raw_scoreboard WHERE payload IS NOT NULL")
        
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
                            ath_id = safe_int(athlete.get('ath_id'))
                            
                            if ath_id and ath_id not in athletes_seen:
                                athletes_seen.add(ath_id)
                                
                                dim_data = {
                                    'fname': athlete.get('fname'),
                                    'lname': athlete.get('lname'),
                                    'gender': athlete.get('gender'),
                                    'bdate': athlete.get('bdate'),
                                    'address': athlete.get('address'),
                                    'city': athlete.get('city'),
                                    'state_id_nat': safe_int(athlete.get('state_id')),
                                    'zip': athlete.get('zip'),
                                    'phone': athlete.get('phone'),
                                    'email': athlete.get('email'),
                                    'email2': athlete.get('email2')
                                }
                                
                                athlete_key = upsert_dimension(
                                    conn, 'dim_athlete', 'ath_id_nat',
                                    ath_id, dim_data
                                )
                                
                                if athlete_key:
                                    name = f"{athlete.get('fname', '')} {athlete.get('lname', '')}".strip()
                                    logger.debug(f"Processed athlete {ath_id}: {name}")
                                    
            except Exception as e:
                logger.error(f"Error extracting athletes: {e}")
    
    conn.commit()
    logger.info("Athlete extraction complete")

def main():
    """Main transformation pipeline for dimensions"""
    logger.info("Starting silver dimension transformations...")
    
    conn = get_db_conn()
    conn.autocommit = False  # Use transactions
    
    try:
        # Transform in dependency order
        transform_ranges(conn)
        transform_competitions(conn) 
        transform_teams(conn)
        transform_schedule_slots(conn)
        extract_disciplines_from_scoreboard(conn)
        extract_athletes_from_scoreboard(conn)
        
        logger.info("All dimension transformations completed successfully!")
        
    except Exception as e:
        logger.error(f"Dimension transformation failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
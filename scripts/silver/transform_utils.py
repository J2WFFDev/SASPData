"""
Silver transformation utilities for SASP data warehouse.
Provides common functions for transforming raw JSON data into normalized dimensions and facts.
"""

import os
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional, Union
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("silver_transform")

load_dotenv()

def get_db_conn():
    """Get database connection using environment variables"""
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "saspdata"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )

def to_date_key(date_str: Optional[str]) -> Optional[int]:
    """Convert date string to YYYYMMDD integer key"""
    if not date_str:
        return None
    try:
        # Handle various date formats
        if 'T' in date_str:
            date_str = date_str.split('T')[0]
        
        # Remove dashes and convert to int
        clean_date = date_str.replace('-', '').replace('/', '')
        
        # Validate it's 8 digits
        if len(clean_date) == 8 and clean_date.isdigit():
            return int(clean_date)
        
        # Try parsing and reformatting
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return int(dt.strftime('%Y%m%d'))
        
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse date: {date_str}")
        return None

def to_time_key(time_str: Optional[str]) -> Optional[int]:
    """Convert time string to HHMM integer key"""
    if not time_str:
        return None
    try:
        # Parse "HH:MM AM/PM" format
        match = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)', time_str.upper())
        if not match:
            # Try 24-hour format
            match = re.match(r'(\d{1,2}):(\d{2})', time_str)
            if match:
                hour, minute = int(match.group(1)), int(match.group(2))
                return hour * 100 + minute
            return None
            
        hour, minute, ampm = int(match.group(1)), int(match.group(2)), match.group(3)
        
        # Convert to 24-hour format
        if ampm == 'PM' and hour != 12:
            hour += 12
        elif ampm == 'AM' and hour == 12:
            hour = 0
            
        return hour * 100 + minute
        
    except (ValueError, AttributeError):
        logger.warning(f"Could not parse time: {time_str}")
        return None

def safe_float(value: Union[str, int, float, None]) -> Optional[float]:
    """Safely convert value to float"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value: Union[str, int, None]) -> Optional[int]:
    """Safely convert value to int"""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_bool(value: Union[str, bool, int, None]) -> bool:
    """Safely convert value to boolean"""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return False

def upsert_dimension(conn, table: str, natural_key_field: str, natural_key_value: Any, 
                    data: Dict[str, Any], key_field: str = None) -> Optional[int]:
    """
    Upsert a dimension record and return the surrogate key.
    
    Args:
        conn: Database connection
        table: Target table name
        natural_key_field: Name of natural key field
        natural_key_value: Value of natural key
        data: Dictionary of field->value pairs to insert/update
        key_field: Name of surrogate key field (defaults to table_key)
    
    Returns:
        Surrogate key value or None if operation failed
    """
    if key_field is None:
        key_field = table.replace('dim_', '') + '_key'
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if record exists
            cur.execute(f"SELECT {key_field} FROM {table} WHERE {natural_key_field} = %s", 
                       (natural_key_value,))
            existing = cur.fetchone()
            
            if existing:
                # Update existing record
                if data:  # Only update if there's data to update
                    set_clause = ', '.join([f"{k} = %s" for k in data.keys()] + ["updated_at = now()"])
                    values = list(data.values()) + [natural_key_value]
                    
                    cur.execute(f"UPDATE {table} SET {set_clause} WHERE {natural_key_field} = %s", 
                               values)
                    logger.debug(f"Updated {table} record with {natural_key_field}={natural_key_value}")
                
                return existing[key_field]
            else:
                # Insert new record
                data[natural_key_field] = natural_key_value
                fields = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                
                cur.execute(f"""
                    INSERT INTO {table} ({fields}) 
                    VALUES ({placeholders}) 
                    RETURNING {key_field}
                """, list(data.values()))
                
                result = cur.fetchone()
                logger.debug(f"Inserted {table} record with {natural_key_field}={natural_key_value}")
                return result[key_field] if result else None
                
    except Exception as e:
        logger.error(f"Error upserting {table}: {e}")
        conn.rollback()
        return None

def get_dimension_key(conn, table: str, natural_key_field: str, natural_key_value: Any,
                     key_field: str = None) -> Optional[int]:
    """
    Get surrogate key for a dimension record by natural key.
    
    Args:
        conn: Database connection
        table: Table name
        natural_key_field: Natural key field name  
        natural_key_value: Natural key value
        key_field: Surrogate key field name (defaults to table_key)
    
    Returns:
        Surrogate key value or None if not found
    """
    if key_field is None:
        key_field = table.replace('dim_', '') + '_key'
    
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {key_field} FROM {table} WHERE {natural_key_field} = %s", 
                       (natural_key_value,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting key from {table}: {e}")
        return None

def ensure_date_dimension(conn, date_key: int):
    """Ensure a date exists in dim_date"""
    if not date_key:
        return
        
    try:
        with conn.cursor() as cur:
            # Check if date exists
            cur.execute("SELECT 1 FROM dim_date WHERE date_key = %s", (date_key,))
            if cur.fetchone():
                return
                
            # Parse date_key back to date
            date_str = str(date_key)
            if len(date_str) == 8:
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                
                full_date = f"{year}-{month:02d}-{day:02d}"
                
                cur.execute("""
                    INSERT INTO dim_date (date_key, full_date, year, month, day, dow, week_of_year, is_weekend)
                    VALUES (%s, %s, %s, %s, %s, 
                           EXTRACT(DOW FROM %s::date),
                           EXTRACT(WEEK FROM %s::date),
                           EXTRACT(DOW FROM %s::date) IN (0, 6))
                """, (date_key, full_date, year, month, day, full_date, full_date, full_date))
                
                logger.debug(f"Created dim_date record for {date_key}")
                
    except Exception as e:
        logger.error(f"Error ensuring date dimension for {date_key}: {e}")

def ensure_time_dimension(conn, time_key: int):
    """Ensure a time exists in dim_time"""
    if not time_key:
        return
        
    try:
        with conn.cursor() as cur:
            # Check if time exists
            cur.execute("SELECT 1 FROM dim_time WHERE time_key = %s", (time_key,))
            if cur.fetchone():
                return
                
            # Parse time_key back to hour/minute
            hour = time_key // 100
            minute = time_key % 100
            
            am_pm = 'AM' if hour < 12 else 'PM'
            display_hour = hour if hour <= 12 else hour - 12
            if display_hour == 0:
                display_hour = 12
                
            cur.execute("""
                INSERT INTO dim_time (time_key, hour, minute, am_pm)
                VALUES (%s, %s, %s, %s)
            """, (time_key, hour, minute, am_pm))
            
            logger.debug(f"Created dim_time record for {time_key}")
            
    except Exception as e:
        logger.error(f"Error ensuring time dimension for {time_key}: {e}")

def truncate_silver_tables(conn):
    """Truncate all silver tables for clean rebuild"""
    tables = [
        'fact_entry_strings',
        'fact_schedule', 
        'fact_entry',
        'agg_team_performance',
        'bridge_team_athlete',
        'dim_slot',
        'dim_discipline', 
        'dim_athlete',
        'dim_team',
        'dim_range',
        'dim_competition',
        'dim_date',
        'dim_time'
    ]
    
    try:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                logger.info(f"Truncated {table}")
        conn.commit()
        logger.info("All silver tables truncated successfully")
    except Exception as e:
        logger.error(f"Error truncating silver tables: {e}")
        conn.rollback()
        raise
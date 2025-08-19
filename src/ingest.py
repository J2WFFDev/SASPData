import os
import sys
import json
import hashlib
import logging
from typing import Dict, List
import requests
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sasp_ingest")

# Configure endpoints (edit/add as needed)
ENDPOINTS = {
    "raw_scoreboard": [
        "https://virtual.sssfonline.com/api/shot/sasp-scoreboard/624"
    ],
    "raw_schedule": [
        "https://virtual.sssfonline.com/api/shot/sasp-schedule/624"
    ],
    "raw_teams": [
        "https://virtual.sssfonline.com/api/teams/1894"
    ]
}

def canonicalize_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def get_db_conn():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "saspdata"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )
    conn.autocommit = True
    return conn

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(open(os.path.join(os.path.dirname(__file__), "..", "db", "ddl.sql")).read())

def ingest_endpoint(conn, table: str, url: str):
    try:
        resp = requests.get(url, timeout=30)
    except Exception as e:
        logger.error("Failed to GET %s : %s", url, e)
        return False
    if resp.status_code != 200:
        logger.warning("Non-200 from %s: %s", url, resp.status_code)
        return False
    try:
        payload_obj = resp.json()
    except Exception as e:
        logger.error("Invalid JSON from %s : %s", url, e)
        return False

    canonical = canonicalize_json(payload_obj)
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # extract match_number if present
    match_number = None
    if isinstance(payload_obj, dict):
        match_number = payload_obj.get("match_number") or payload_obj.get("MatchNumber") or payload_obj.get("matchNumber")

    insert_sql = f"""
    INSERT INTO {table} (match_number, payload, source, source_hash)
    VALUES (%s, %s::jsonb, %s, %s)
    ON CONFLICT (source_hash) DO NOTHING
    RETURNING id;
    """

    with conn.cursor() as cur:
        try:
            cur.execute(insert_sql, (match_number, Json(payload_obj), url, source_hash))
            row = cur.fetchone()
            if row:
                logger.info("Inserted into %s: url=%s hash=%s id=%s", table, url, source_hash, row[0])
                return True
            else:
                logger.info("Already exists in %s (source_hash=%s) for url=%s", table, source_hash, url)
                return False
        except Exception as e:
            logger.exception("DB insert failed for %s : %s", url, e)
            return False

def run_ingest(endpoints: Dict[str, List[str]] = ENDPOINTS):
    conn = get_db_conn()
    try:
        ensure_tables(conn)
        inserted = 0
        tried = 0
        for table, urls in endpoints.items():
            for url in urls:
                tried += 1
                if ingest_endpoint(conn, table, url):
                    inserted += 1
        logger.info("Ingest finished: tried=%d inserted=%d", tried, inserted)
    finally:
        conn.close()

if __name__ == "__main__":
    run_ingest()

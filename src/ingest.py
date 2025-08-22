import os
import sys
import json
import hashlib
import logging
from typing import Dict, List
import requests
import psycopg2
import time
import random
import re
import os
import sys
import json
import hashlib
import logging
from typing import Dict, List
import requests
import psycopg2
from psycopg2.extras import Json
import yaml
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sasp_ingest")

load_dotenv()


def http_get_with_backoff(url, timeout=30, max_retries=5, initial_backoff=0.5, multiplier=2.0, max_backoff=60):
    """GET with retries, exponential backoff and full jitter.

    Returns requests.Response or raises the last exception.
    """
    attempt = 0
    last_exc = None
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=timeout)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            resp = None
            should_retry = True
        else:
            # Retry on 429 and 5xx
            if resp.status_code == 429 or (500 <= resp.status_code < 600):
                should_retry = True
            else:
                should_retry = False

        if not should_retry:
            if resp is not None:
                return resp
            # network exception and not retrying
            raise last_exc

        if attempt >= max_retries:
            # Exhausted retries: return last response if present else raise
            logger.warning('Exhausted retries (%d) for %s', max_retries, url)
            if resp is not None:
                return resp
            raise last_exc

        # Honor Retry-After header if present
        sleep_for = None
        if resp is not None:
            ra = resp.headers.get('Retry-After')
            if ra:
                try:
                    sleep_for = int(ra)
                except ValueError:
                    sleep_for = None

        if sleep_for is None:
            exp = initial_backoff * (multiplier ** (attempt - 1))
            cap = min(exp, max_backoff)
            sleep_for = random.uniform(0, cap)

        logger.info('Retry attempt=%d for %s sleeping=%.2fs', attempt, url, sleep_for)
        time.sleep(sleep_for)


# Configure endpoints: prefer config/endpoints.yml, fallback to hardcoded ENDPOINTS
DEFAULT_ENDPOINTS = {
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


def load_endpoints(cfg_path: str = None) -> Dict[str, List[str]]:
    if cfg_path is None:
        cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "endpoints.yml")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                return {k: v if isinstance(v, list) else [v] for k, v in data.items()}
        except Exception:
            logger.exception("Failed to load endpoints from %s", cfg_path)
    return DEFAULT_ENDPOINTS


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
    ddl_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "ddl.sql")
    if not os.path.exists(ddl_path):
        logger.warning("DDL file not found at %s; skipping ensure_tables", ddl_path)
        return
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    # Split the DDL into individual statements and execute separately.
    # This is more robust on Windows and avoids server-side syntax errors
    # when client libraries or encodings add noise. We also skip SQL
    # comments and continue past statements that raise permission errors.
    statements = []
    cur_lines = []
    for line in ddl.splitlines():
        # skip single-line comments
        if line.strip().startswith('--'):
            continue
        cur_lines.append(line)
        if ';' in line:
            stmt = '\n'.join(cur_lines).strip()
            # remove trailing semicolon(s)
            while stmt.endswith(';'):
                stmt = stmt[:-1].rstrip()
            if stmt:
                statements.append(stmt)
            cur_lines = []
    # leftover
    if cur_lines:
        stmt = '\n'.join(cur_lines).strip()
        if stmt:
            statements.append(stmt)

    with conn.cursor() as cur:
        for stmt in statements:
            try:
                cur.execute(stmt)
            except Exception as e:
                # Log and continue - extension creation may require superuser
                logger.warning("DDL statement failed (continuing): %s ; error=%s", stmt.split('\n',1)[0], e)
    # Ensure URL status tracking table exists (used to record 404s and decide future scans)
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS url_status (
            id BIGSERIAL PRIMARY KEY,
            resource TEXT NOT NULL,
            match_id BIGINT,
            url TEXT UNIQUE NOT NULL,
            last_checked TIMESTAMPTZ DEFAULT now(),
            last_status INTEGER,
            consecutive_404 INTEGER DEFAULT 0,
            first_404_at TIMESTAMPTZ,
            last_success_at TIMESTAMPTZ,
            permanent_404 BOOLEAN DEFAULT false,
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_url_status_resource_match ON url_status(resource, match_id);
        """)


def ingest_endpoint(conn, table: str, url: str):
    try:
        resp = http_get_with_backoff(url, timeout=int(os.getenv('HTTP_TIMEOUT', '30')),
                                     max_retries=int(os.getenv('MAX_RETRIES', '5')),
                                     initial_backoff=float(os.getenv('BACKOFF_INITIAL', '0.5')),
                                     multiplier=float(os.getenv('BACKOFF_MULTIPLIER', '2.0')),
                                     max_backoff=float(os.getenv('BACKOFF_MAX', '60')))
    except Exception as e:
        logger.error("Failed to GET %s : %s", url, e)
        return False
    if resp.status_code != 200:
        logger.warning("Non-200 from %s: %s", url, resp.status_code)
        # record status for URL tracking
        try:
            update_url_status(conn, table, url, resp.status_code, success=False)
        except Exception:
            logger.exception('Failed to update url_status for %s', url)
        return False
    try:
        payload_obj = resp.json()
    except Exception as e:
        logger.error("Invalid JSON from %s : %s", url, e)
        return False

    canonical = canonicalize_json(payload_obj)
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # extract match_number if present in payload, otherwise try to parse from the URL
    match_number = None
    if isinstance(payload_obj, dict):
        match_number = payload_obj.get("match_number") or payload_obj.get("MatchNumber") or payload_obj.get("matchNumber")
    if match_number in (None, ""):
        # fallback to extracting numeric id from the URL path
        try:
            match_number = extract_match_id_from_url(url)
        except Exception:
            match_number = None

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
                try:
                    update_url_status(conn, table, url, 200, success=True)
                except Exception:
                    logger.exception('Failed to update url_status after insert for %s', url)
                return True
            else:
                logger.info("Already exists in %s (source_hash=%s) for url=%s", table, source_hash, url)
                try:
                    update_url_status(conn, table, url, 200, success=True)
                except Exception:
                    logger.exception('Failed to update url_status for existing %s', url)
                return False
        except Exception as e:
            logger.exception("DB insert failed for %s : %s", url, e)
            try:
                update_url_status(conn, table, url, None, success=False)
            except Exception:
                logger.exception('Failed to update url_status after DB error for %s', url)
            return False


def extract_match_id_from_url(url: str):
    m = re.search(r"/(\d+)(?:\/?$)", url)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def update_url_status(conn, resource: str, url: str, status_code: int, success: bool = False):
    """Insert or update a row in url_status reflecting the latest attempt.

    Logic:
    - On success (HTTP 200): set last_success_at, last_checked, last_status; reset consecutive_404 and permanent flag.
    - On 404: increment consecutive_404, set first_404_at if null; consider setting permanent_404 when consecutive_404 >= PERM_404_CONSECUTIVE
      and the match_id appears to be older than the current max seen match minus FUTURE_MARGIN.
    - Other errors: record last_status and last_checked.
    """
    perm_threshold = int(os.getenv('PERM_404_CONSECUTIVE', '5'))
    future_margin = int(os.getenv('FUTURE_MARGIN', '10'))
    match_id = extract_match_id_from_url(url)

    with conn.cursor() as cur:
        # upsert the basic row if not exists
        cur.execute("""
        INSERT INTO url_status(resource, match_id, url, last_checked, last_status)
        VALUES (%s, %s, %s, now(), %s)
        ON CONFLICT (url) DO UPDATE SET last_checked = now(), last_status = EXCLUDED.last_status
        RETURNING id, consecutive_404, first_404_at, permanent_404;
        """, (resource, match_id, url, status_code))
        res = cur.fetchone()
        if not res:
            return
        row_id, consecutive_404, first_404_at, permanent_404 = res

        if success:
            cur.execute("""
            UPDATE url_status SET last_success_at = now(), consecutive_404 = 0, first_404_at = NULL, permanent_404 = false, last_status = 200
            WHERE id = %s
            """, (row_id,))
            return

        # not success
        if status_code == 404:
            consecutive_404 = (consecutive_404 or 0) + 1
            if not first_404_at:
                cur.execute("UPDATE url_status SET first_404_at = now() WHERE id = %s", (row_id,))
            # check if we should mark permanent
            permanent = False
            try:
                cur.execute(f"SELECT MAX(match_number) FROM {resource} WHERE match_number IS NOT NULL")
                max_match = cur.fetchone()[0]
            except Exception:
                max_match = None

            if consecutive_404 >= perm_threshold and max_match is not None and match_id is not None:
                try:
                    if match_id <= (max_match - future_margin):
                        permanent = True
                except Exception:
                    permanent = False

            cur.execute("""
            UPDATE url_status SET consecutive_404 = %s, permanent_404 = %s, last_status = %s WHERE id = %s
            """, (consecutive_404, permanent, status_code, row_id))
        else:
            # other non-200
            cur.execute("UPDATE url_status SET last_status = %s WHERE id = %s", (status_code, row_id))


def is_permanent_404(conn, resource: str, url: str) -> bool:
    """Return True if url is marked permanent_404 in the url_status table."""
    with conn.cursor() as cur:
        cur.execute("SELECT permanent_404 FROM url_status WHERE url = %s", (url,))
        r = cur.fetchone()
        if not r:
            return False
        return bool(r[0])


def run_ingest(endpoints: Dict[str, List[str]] = None):
    if endpoints is None:
        endpoints = load_endpoints()
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

"""
Ingest all matches for scoreboard and schedule.

Usage:
  - Configure `config/endpoints.yml` with at least one example URL (current file contains a single URL with an ID).
  - Option A (discovery): If the base endpoint (without trailing id) returns a JSON with `data` array, the script will extract IDs automatically.
  - Option B (range): Set environment variables `START_ID` and `END_ID` (integers) to iterate a numeric range. Defaults: START_ID=580 END_ID=640.

Runs idempotent inserts via `src.ingest.ingest_endpoint`.
"""

import os
import sys
import re
import time
import logging
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

# Ensure repo root is on PYTHONPATH (same pattern as other scripts)
root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

# import ingest utilities from src
from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ingest_all_matches')

DEFAULT_START = int(os.getenv('START_ID', '580'))
DEFAULT_END = int(os.getenv('END_ID', '640'))
PAUSE_SEC = float(os.getenv('PAUSE_SEC', '0.25'))


def try_discover_ids(base_url):
    """Try to GET the base URL and extract a list of ids from a `data` array or top-level list."""
    try:
        from src import ingest as _ingest
        r = _ingest.http_get_with_backoff(base_url, timeout=15)
        if r is None or r.status_code != 200:
            return []
        j = r.json()
        ids = set()
        # if it's a dict with data array
        if isinstance(j, dict) and isinstance(j.get('data'), list):
            for item in j['data']:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
        # if it's a list of objects
        elif isinstance(j, list):
            for item in j:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
        return sorted(ids)
    except Exception as e:
        log.debug('Discovery failed for %s: %s', base_url, e)
        return []


def make_template_from_example(url):
    """If URL ends with a numeric id, return a template with '{}' to format with an id."""
    m = re.search(r"(.*?)(\d+)(\/?$)", url)
    if m:
        prefix = m.group(1)
        return prefix + "{}"
    return None


def ingest_range_for_url(conn, table, url_template, start, end):
    inserted = 0
    tried = 0
    for mid in range(start, end + 1):
        url = url_template.format(mid)
        # Skip URLs previously marked as permanent 404
        try:
            if ingest.is_permanent_404(conn, table, url):
                log.info('Skipping permanently missing url=%s', url)
                continue
        except Exception:
            log.exception('Failed to check url_status for %s', url)
        tried += 1
        ok = ingest.ingest_endpoint(conn, table, url)
        if ok:
            inserted += 1
        time.sleep(PAUSE_SEC)
    log.info('%s: tried=%d inserted=%d', table, tried, inserted)
    return inserted


def ingest_ids_for_url(conn, table, url_template, ids):
    inserted = 0
    tried = 0
    for mid in ids:
        url = url_template.format(mid)
        # Skip permanently missing
        try:
            if ingest.is_permanent_404(conn, table, url):
                log.info('Skipping permanently missing url=%s', url)
                continue
        except Exception:
            log.exception('Failed to check url_status for %s', url)
        tried += 1
        ok = ingest.ingest_endpoint(conn, table, url)
        if ok:
            inserted += 1
        time.sleep(PAUSE_SEC)
    log.info('%s: tried=%d inserted=%d', table, tried, inserted)
    return inserted


def main():
    endpoints = ingest.load_endpoints()
    conn = ingest.get_db_conn()
    try:
        ingest.ensure_tables(conn)
        total_inserted = 0
        scoreboard_ids = []

        # --- Step 1: ingest scoreboard and collect match ids ---
        sb_urls = endpoints.get('raw_scoreboard', [])
        if not sb_urls:
            log.warning('No raw_scoreboard endpoints configured; nothing to do for scoreboard')
        else:
            # process first scoreboard endpoint (others would be redundant)
            sb_url = sb_urls[0]
            log.info('Processing scoreboard endpoint %s', sb_url)
            base = re.sub(r"/\d+/?$", '', sb_url)
            ids = try_discover_ids(base)
            if ids:
                log.info('Discovered %d scoreboard ids from %s', len(ids), base)
                tpl = make_template_from_example(sb_url) or (base + '/{}')
                total_inserted += ingest_ids_for_url(conn, 'raw_scoreboard', tpl, ids)
                scoreboard_ids = ids
            else:
                tpl = make_template_from_example(sb_url)
                if tpl:
                    start = int(os.getenv('START_ID', DEFAULT_START))
                    end = int(os.getenv('END_ID', DEFAULT_END))
                    log.info('No discovery; iterating scoreboard ids %d..%d using template %s', start, end, tpl)
                    total_inserted += ingest_range_for_url(conn, 'raw_scoreboard', tpl, start, end)
                    scoreboard_ids = list(range(start, end + 1))
                else:
                    ok = ingest.ingest_endpoint(conn, 'raw_scoreboard', sb_url)
                    if ok:
                        total_inserted += 1

        # --- Step 2: ingest schedule for the same match ids ---
        sch_urls = endpoints.get('raw_schedule', [])
        if not sch_urls:
            # try to derive schedule URL by replacing 'sasp-scoreboard' with 'sasp-schedule' on the scoreboard template
            if sb_urls:
                candidate = sb_urls[0].replace('sasp-scoreboard', 'sasp-schedule')
                tpl = make_template_from_example(candidate) or (re.sub(r"/\d+/?$", '', candidate) + '/{}')
                log.info('No raw_schedule endpoint configured; derived schedule template %s', tpl)
            else:
                tpl = None
                log.warning('No raw_schedule endpoints and no scoreboard to derive from; skipping schedule ingest')
        else:
            tpl = make_template_from_example(sch_urls[0]) or (re.sub(r"/\d+/?$", '', sch_urls[0]) + '/{}')

        if tpl and scoreboard_ids:
            log.info('Ingesting schedule for %d scoreboard ids using template %s', len(scoreboard_ids), tpl)
            total_inserted += ingest_ids_for_url(conn, 'raw_schedule', tpl, scoreboard_ids)
        else:
            log.info('No scoreboard ids to use for schedule ingest; skipped')

    # --- Step 3: ingest competition data ---
    # Competition ingestion has been moved to a standalone script `scripts/ingest_competitions.py`.
    # Run that script separately to ingest `raw_competition` pages or ids.
    # Example (PowerShell):
    #   $env:END_PAGE='50'; python .\scripts\ingest_competitions.py
        

        log.info('All endpoints complete. total_inserted=%d', total_inserted)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
"""
Ingest all matches for scoreboard and schedule.

Usage:
  - Configure `config/endpoints.yml` with at least one example URL (current file contains a single URL with an ID).
  - Option A (discovery): If the base endpoint (without trailing id) returns a JSON with `data` array, the script will extract IDs automatically.
  - Option B (range): Set environment variables `START_ID` and `END_ID` (integers) to iterate a numeric range. Defaults: START_ID=580 END_ID=640.

Runs idempotent inserts via `src.ingest.ingest_endpoint`.
"""

import os
import sys
import re
import time
import logging
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

# Ensure repo root is on PYTHONPATH (same pattern as other scripts)
root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

# import ingest utilities from src
from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ingest_all_matches')

DEFAULT_START = int(os.getenv('START_ID', '580'))
DEFAULT_END = int(os.getenv('END_ID', '640'))
PAUSE_SEC = float(os.getenv('PAUSE_SEC', '0.25'))


def try_discover_ids(base_url):
    """Try to GET the base URL and extract a list of ids from a `data` array or top-level list."""
    try:
        from src import ingest as _ingest
        r = _ingest.http_get_with_backoff(base_url, timeout=15)
        if r is None or r.status_code != 200:
            return []
        j = r.json()
        ids = set()
        # if it's a dict with data array
        if isinstance(j, dict) and isinstance(j.get('data'), list):
            for item in j['data']:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
        # if it's a list of objects
        elif isinstance(j, list):
            for item in j:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
        return sorted(ids)
    except Exception as e:
        log.debug('Discovery failed for %s: %s', base_url, e)
        return []


def make_template_from_example(url):
    """If URL ends with a numeric id, return a template with '{}' to format with an id."""
    m = re.search(r"(.*?)(\d+)(\/?$)", url)
    if m:
        prefix = m.group(1)
        return prefix + "{}"
    return None


def ingest_range_for_url(conn, table, url_template, start, end):
    inserted = 0
    tried = 0
    for mid in range(start, end + 1):
        url = url_template.format(mid)
        # Skip URLs previously marked as permanent 404
        try:
            if ingest.is_permanent_404(conn, table, url):
                log.info('Skipping permanently missing url=%s', url)
                continue
        except Exception:
            log.exception('Failed to check url_status for %s', url)
        tried += 1
        ok = ingest.ingest_endpoint(conn, table, url)
        if ok:
            inserted += 1
        time.sleep(PAUSE_SEC)
    log.info('%s: tried=%d inserted=%d', table, tried, inserted)
    return inserted


def ingest_ids_for_url(conn, table, url_template, ids):
    inserted = 0
    tried = 0
    for mid in ids:
        url = url_template.format(mid)
        # Skip permanently missing
        try:
            if ingest.is_permanent_404(conn, table, url):
                log.info('Skipping permanently missing url=%s', url)
                continue
        except Exception:
            log.exception('Failed to check url_status for %s', url)
        tried += 1
        ok = ingest.ingest_endpoint(conn, table, url)
        if ok:
            inserted += 1
        time.sleep(PAUSE_SEC)
    log.info('%s: tried=%d inserted=%d', table, tried, inserted)
    return inserted


def main():
    endpoints = ingest.load_endpoints()
    conn = ingest.get_db_conn()
    try:
        ingest.ensure_tables(conn)
        total_inserted = 0
        scoreboard_ids = []

        # --- Step 1: ingest scoreboard and collect match ids ---
        sb_urls = endpoints.get('raw_scoreboard', [])
        if not sb_urls:
            log.warning('No raw_scoreboard endpoints configured; nothing to do for scoreboard')
        else:
            # process first scoreboard endpoint (others would be redundant)
            sb_url = sb_urls[0]
            log.info('Processing scoreboard endpoint %s', sb_url)
            base = re.sub(r"/\d+/?$", '', sb_url)
            ids = try_discover_ids(base)
            if ids:
                log.info('Discovered %d scoreboard ids from %s', len(ids), base)
                tpl = make_template_from_example(sb_url) or (base + '/{}')
                total_inserted += ingest_ids_for_url(conn, 'raw_scoreboard', tpl, ids)
                scoreboard_ids = ids
            else:
                tpl = make_template_from_example(sb_url)
                if tpl:
                    start = int(os.getenv('START_ID', DEFAULT_START))
                    end = int(os.getenv('END_ID', DEFAULT_END))
                    log.info('No discovery; iterating scoreboard ids %d..%d using template %s', start, end, tpl)
                    total_inserted += ingest_range_for_url(conn, 'raw_scoreboard', tpl, start, end)
                    scoreboard_ids = list(range(start, end + 1))
                else:
                    ok = ingest.ingest_endpoint(conn, 'raw_scoreboard', sb_url)
                    if ok:
                        total_inserted += 1

        # --- Step 2: ingest schedule for the same match ids ---
        sch_urls = endpoints.get('raw_schedule', [])
        if not sch_urls:
            # try to derive schedule URL by replacing 'sasp-scoreboard' with 'sasp-schedule' on the scoreboard template
            if sb_urls:
                candidate = sb_urls[0].replace('sasp-scoreboard', 'sasp-schedule')
                tpl = make_template_from_example(candidate) or (re.sub(r"/\d+/?$", '', candidate) + '/{}')
                log.info('No raw_schedule endpoint configured; derived schedule template %s', tpl)
            else:
                tpl = None
                log.warning('No raw_schedule endpoints and no scoreboard to derive from; skipping schedule ingest')
        else:
            tpl = make_template_from_example(sch_urls[0]) or (re.sub(r"/\d+/?$", '', sch_urls[0]) + '/{}')

        if tpl and scoreboard_ids:
            log.info('Ingesting schedule for %d scoreboard ids using template %s', len(scoreboard_ids), tpl)
            total_inserted += ingest_ids_for_url(conn, 'raw_schedule', tpl, scoreboard_ids)
        else:
            log.info('No scoreboard ids to use for schedule ingest; skipped')

    # --- Step 3: ingest competition data ---
    # Competition ingestion has been moved to a standalone script `scripts/ingest_competitions.py`.
    # Run that script separately to ingest `raw_competition` pages or ids.
    # Example (PowerShell):
    #   $env:END_PAGE='50'; python .\scripts\ingest_competitions.py
        

        log.info('All endpoints complete. total_inserted=%d', total_inserted)
    finally:
        conn.close()


if __name__ == '__main__':
    main()

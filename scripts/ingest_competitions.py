"""
Ingest competition pages or competition ids into `raw_competition`.

Usage:
  - Configure `config/endpoints.yml` with a `raw_competition` entry (paged or id template).
  - Set env vars: `END_PAGE` (for paged endpoints), `START_ID`/`END_ID` (for range ingestion), `PAUSE_SEC`.

This script isolates competition ingest from scoreboard/schedule ingestion.
"""

import os
import re
import time
import logging
from dotenv import load_dotenv

root = os.path.dirname(os.path.dirname(__file__))
import sys
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ingest_competitions')

PAUSE_SEC = float(os.getenv('PAUSE_SEC', '0.25'))
FORCE_FULL_PAGES = os.getenv('FORCE_FULL_PAGES', '').lower() in ('1','true','yes')


def try_discover_ids(base_url):
    try:
        r = ingest.http_get_with_backoff(base_url, timeout=15)
        if r is None or r.status_code != 200:
            return []
        j = r.json()
        ids = set()
        if isinstance(j, dict) and isinstance(j.get('data'), list):
            for item in j['data']:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
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

        comp_urls = endpoints.get('raw_competition', [])
        if not comp_urls:
            log.warning('No raw_competition endpoints configured; nothing to do')
            return

        for comp_url in comp_urls:
            log.info('Processing competition endpoint %s', comp_url)
            # paged API
            if 'page=' in comp_url:
                m = re.search(r"(.*page=)(\d+)(.*)$", comp_url)
                if m:
                    prefix = m.group(1)
                    start_page = int(m.group(2))
                    suffix = m.group(3) or ''
                    max_pages = int(os.getenv('END_PAGE', '50'))
                    consecutive_empty = 0
                    page = start_page
                    # if FORCE_FULL_PAGES is set, ignore the consecutive-empty early stop
                    while page <= max_pages and (consecutive_empty < 3 or FORCE_FULL_PAGES):
                        url = f"{prefix}{page}{suffix}"
                        try:
                            if ingest.is_permanent_404(conn, 'raw_competition', url):
                                log.info('Skipping permanently missing url=%s', url)
                                page += 1
                                continue
                        except Exception:
                            log.exception('Failed to check url_status for %s', url)
                        # perform GET so we can inspect pagination metadata
                        from src import ingest as _ingest
                        r = _ingest.http_get_with_backoff(url, timeout=15)
                        if r is None:
                            log.warning('No response for %s', url)
                            consecutive_empty += 1
                            page += 1
                            continue
                        if r.status_code != 200:
                            log.warning('Non-200 from %s: %s', url, r.status_code)
                            consecutive_empty += 1
                            page += 1
                            continue
                        # try to inspect pagination links/meta
                        is_last_page = False
                        try:
                            j = r.json()
                            # links.next null indicates end
                            links = j.get('links') if isinstance(j, dict) else None
                            meta = j.get('meta') if isinstance(j, dict) else None
                            if isinstance(links, dict) and links.get('next') is None:
                                is_last_page = True
                            if isinstance(meta, dict) and 'last_page' in meta:
                                try:
                                    last_page = int(meta.get('last_page'))
                                    if page >= last_page:
                                        is_last_page = True
                                except Exception:
                                    pass
                        except Exception:
                            log.debug('Failed to parse JSON pagination for %s', url)

                        # now call ingest_endpoint to store payload (idempotent)
                        ok = ingest.ingest_endpoint(conn, 'raw_competition', url)
                        if ok:
                            total_inserted += 1
                            consecutive_empty = 0
                        else:
                            consecutive_empty += 1
                        page += 1
                        if is_last_page and not FORCE_FULL_PAGES:
                            log.info('Detected last page at %s; stopping pagination', url)
                            break
                else:
                    ok = ingest.ingest_endpoint(conn, 'raw_competition', comp_url)
                    if ok:
                        total_inserted += 1
            else:
                base = re.sub(r"/\d+/?$", '', comp_url)
                ids = try_discover_ids(base)
                if ids:
                    log.info('Discovered %d competition ids from %s', len(ids), base)
                    tplc = make_template_from_example(comp_url) or (base + '/{}')
                    total_inserted += ingest_ids_for_url(conn, 'raw_competition', tplc, ids)
                else:
                    tplc = make_template_from_example(comp_url)
                    if tplc:
                        start = int(os.getenv('START_ID', '580'))
                        end = int(os.getenv('END_ID', '640'))
                        log.info('No discovery; iterating competition ids %d..%d using template %s', start, end, tplc)
                        total_inserted += ingest_range_for_url(conn, 'raw_competition', tplc, start, end)
                    else:
                        ok = ingest.ingest_endpoint(conn, 'raw_competition', comp_url)
                        if ok:
                            total_inserted += 1

        log.info('Competition ingest complete. total_inserted=%d', total_inserted)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
"""
Ingest competition pages or competition ids into `raw_competition`.

Usage:
  - Configure `config/endpoints.yml` with a `raw_competition` entry (paged or id template).
  - Set env vars: `END_PAGE` (for paged endpoints), `START_ID`/`END_ID` (for range ingestion), `PAUSE_SEC`.

This script isolates competition ingest from scoreboard/schedule ingestion.
"""

import os
import re
import time
import logging
from dotenv import load_dotenv

root = os.path.dirname(os.path.dirname(__file__))
import sys
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ingest_competitions')

PAUSE_SEC = float(os.getenv('PAUSE_SEC', '0.25'))
FORCE_FULL_PAGES = os.getenv('FORCE_FULL_PAGES', '').lower() in ('1','true','yes')


def try_discover_ids(base_url):
    try:
        r = ingest.http_get_with_backoff(base_url, timeout=15)
        if r is None or r.status_code != 200:
            return []
        j = r.json()
        ids = set()
        if isinstance(j, dict) and isinstance(j.get('data'), list):
            for item in j['data']:
                if isinstance(item, dict):
                    for k in ('id', 'match_number', 'matchId', 'external_id'):
                        if k in item and item[k] is not None:
                            ids.add(int(item[k]))
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

        comp_urls = endpoints.get('raw_competition', [])
        if not comp_urls:
            log.warning('No raw_competition endpoints configured; nothing to do')
            return

        for comp_url in comp_urls:
            log.info('Processing competition endpoint %s', comp_url)
            # paged API
            if 'page=' in comp_url:
                m = re.search(r"(.*page=)(\d+)(.*)$", comp_url)
                if m:
                    prefix = m.group(1)
                    start_page = int(m.group(2))
                    suffix = m.group(3) or ''
                    max_pages = int(os.getenv('END_PAGE', '50'))
                    consecutive_empty = 0
                    page = start_page
                    # if FORCE_FULL_PAGES is set, ignore the consecutive-empty early stop
                    while page <= max_pages and (consecutive_empty < 3 or FORCE_FULL_PAGES):
                        url = f"{prefix}{page}{suffix}"
                        try:
                            if ingest.is_permanent_404(conn, 'raw_competition', url):
                                log.info('Skipping permanently missing url=%s', url)
                                page += 1
                                continue
                        except Exception:
                            log.exception('Failed to check url_status for %s', url)
                        # perform GET so we can inspect pagination metadata
                        from src import ingest as _ingest
                        r = _ingest.http_get_with_backoff(url, timeout=15)
                        if r is None:
                            log.warning('No response for %s', url)
                            consecutive_empty += 1
                            page += 1
                            continue
                        if r.status_code != 200:
                            log.warning('Non-200 from %s: %s', url, r.status_code)
                            consecutive_empty += 1
                            page += 1
                            continue
                        # try to inspect pagination links/meta
                        is_last_page = False
                        try:
                            j = r.json()
                            # links.next null indicates end
                            links = j.get('links') if isinstance(j, dict) else None
                            meta = j.get('meta') if isinstance(j, dict) else None
                            if isinstance(links, dict) and links.get('next') is None:
                                is_last_page = True
                            if isinstance(meta, dict) and 'last_page' in meta:
                                try:
                                    last_page = int(meta.get('last_page'))
                                    if page >= last_page:
                                        is_last_page = True
                                except Exception:
                                    pass
                        except Exception:
                            log.debug('Failed to parse JSON pagination for %s', url)

                        # now call ingest_endpoint to store payload (idempotent)
                        ok = ingest.ingest_endpoint(conn, 'raw_competition', url)
                        if ok:
                            total_inserted += 1
                            consecutive_empty = 0
                        else:
                            consecutive_empty += 1
                        page += 1
                        if is_last_page and not FORCE_FULL_PAGES:
                            log.info('Detected last page at %s; stopping pagination', url)
                            break
                else:
                    ok = ingest.ingest_endpoint(conn, 'raw_competition', comp_url)
                    if ok:
                        total_inserted += 1
            else:
                base = re.sub(r"/\d+/?$", '', comp_url)
                ids = try_discover_ids(base)
                if ids:
                    log.info('Discovered %d competition ids from %s', len(ids), base)
                    tplc = make_template_from_example(comp_url) or (base + '/{}')
                    total_inserted += ingest_ids_for_url(conn, 'raw_competition', tplc, ids)
                else:
                    tplc = make_template_from_example(comp_url)
                    if tplc:
                        start = int(os.getenv('START_ID', '580'))
                        end = int(os.getenv('END_ID', '640'))
                        log.info('No discovery; iterating competition ids %d..%d using template %s', start, end, tplc)
                        total_inserted += ingest_range_for_url(conn, 'raw_competition', tplc, start, end)
                    else:
                        ok = ingest.ingest_endpoint(conn, 'raw_competition', comp_url)
                        if ok:
                            total_inserted += 1

        log.info('Competition ingest complete. total_inserted=%d', total_inserted)
    finally:
        conn.close()


if __name__ == '__main__':
    main()

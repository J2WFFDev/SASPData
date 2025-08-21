import os
import sys
import logging
from dotenv import load_dotenv

root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ingest_teams_range')

START = int(os.getenv('START_ID', '1800'))
END = int(os.getenv('END_ID', '1900'))
PAUSE = float(os.getenv('PAUSE_SEC', '0.1'))
TEMPLATE = os.getenv('TEAMS_URL_TEMPLATE', 'https://virtual.sssfonline.com/api/teams/{}')

def main():
    conn = ingest.get_db_conn()
    try:
        ingest.ensure_tables(conn)
        inserted = 0
        for tid in range(START, END+1):
            url = TEMPLATE.format(tid)
            try:
                if ingest.is_permanent_404(conn, 'raw_teams', url):
                    log.info('Skipping permanently missing %s', url)
                    continue
            except Exception:
                log.exception('url_status check failed for %s', url)
            ok = ingest.ingest_endpoint(conn, 'raw_teams', url)
            if ok:
                inserted += 1
        log.info('Done teams ingest: %d inserted', inserted)
    finally:
        conn.close()

if __name__ == '__main__':
    main()

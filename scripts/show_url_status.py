"""
Show URL status table rows for review.

Usage: python scripts/show_url_status.py [--permanent-only]
"""
import sys
import os
import json

root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest

import psycopg2
from dotenv import load_dotenv
load_dotenv()

conn = ingest.get_db_conn()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT id, resource, match_id, url, last_checked, last_status, consecutive_404, permanent_404, note FROM url_status ORDER BY id DESC LIMIT 200")
        rows = cur.fetchall()
    print(json.dumps([{
        'id': r[0], 'resource': r[1], 'match_id': r[2], 'url': r[3], 'last_checked': str(r[4]),
        'last_status': r[5], 'consecutive_404': r[6], 'permanent_404': bool(r[7]), 'note': r[8]
    } for r in rows], indent=2))
finally:
    conn.close()

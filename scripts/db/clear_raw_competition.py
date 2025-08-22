"""
Clear the `raw_competition` bronze table.

WARNING: This will permanently delete rows from `raw_competition` in the configured database.
"""

import os
import logging
from dotenv import load_dotenv

root = os.path.dirname(os.path.dirname(__file__))
import sys
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('clear_raw_competition')


def main():
    conn = ingest.get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE raw_competition RESTART IDENTITY CASCADE')
                log.info('Truncated raw_competition and reset identity')
    finally:
        conn.close()


if __name__ == '__main__':
    main()

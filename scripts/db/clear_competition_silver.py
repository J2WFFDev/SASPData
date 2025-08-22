"""
Clear competition-related silver tables.

WARNING: This will permanently delete rows from the competition silver tables in the configured database.
"""

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
log = logging.getLogger('clear_competition_silver')


def main():
    conn = ingest.get_db_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    TRUNCATE TABLE bridge_competition_invited_team, competition_stage, dim_competition,
                                   dim_classification, dim_registration_type, dim_contact, dim_range,
                                   dim_team, dim_state
                    RESTART IDENTITY CASCADE
                    """
                )
                log.info('Truncated competition silver tables and reset identities')
    finally:
        conn.close()


if __name__ == '__main__':
    main()

import sys
import os
from typing import List

# Ensure repo root is on PYTHONPATH
root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest as ingest_module

def run_matches(match_ids: List[int]):
    # Build endpoints dict for the three raw tables, replacing the scoreboard URL for the given match ids
    endpoints = ingest_module.load_endpoints()
    # For scoreboard, replace urls with the desired match endpoints
    sb_urls = [f"https://virtual.sssfonline.com/api/shot/sasp-scoreboard/{mid}" for mid in match_ids]
    endpoints['raw_scoreboard'] = sb_urls
    # Keep other endpoints as-is
    ingest_module.run_ingest(endpoints)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print('Usage: ingest_matches.py <match_id> [match_id ...]')
        return
    match_ids = [int(x) for x in argv]
    run_matches(match_ids)

if __name__ == '__main__':
    main()
import sys
import os
from typing import List

# Ensure repo root is on PYTHONPATH
root = os.path.dirname(os.path.dirname(__file__))
if root not in sys.path:
    sys.path.insert(0, root)

from src import ingest as ingest_module

def run_matches(match_ids: List[int]):
    # Build endpoints dict for the three raw tables, replacing the scoreboard URL for the given match ids
    endpoints = ingest_module.load_endpoints()
    # For scoreboard, replace urls with the desired match endpoints
    sb_urls = [f"https://virtual.sssfonline.com/api/shot/sasp-scoreboard/{mid}" for mid in match_ids]
    endpoints['raw_scoreboard'] = sb_urls
    # Keep other endpoints as-is
    ingest_module.run_ingest(endpoints)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        print('Usage: ingest_matches.py <match_id> [match_id ...]')
        return
    match_ids = [int(x) for x in argv]
    run_matches(match_ids)

if __name__ == '__main__':
    main()

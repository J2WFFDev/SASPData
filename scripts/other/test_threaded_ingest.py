#!/usr/bin/env python3
"""Test the threaded team ingester with a small range."""

import sys
import os
sys.path.append('.')

from scripts.threaded_team_ingest import ThreadedTeamIngester
import time

def test_threaded_ingest():
    """Test threaded ingestion with a small range."""
    print("=== TESTING THREADED TEAM INGESTION ===")
    print("Testing with teams 3200-3210 (11 teams)...")
    
    ingester = ThreadedTeamIngester(
        max_workers=5,  # Smaller for testing
        rate_limit_per_minute=24
    )
    
    start_time = time.time()
    results = ingester.process_team_range(3200, 3210, batch_size=5)
    duration = time.time() - start_time
    
    print(f"\n=== TEST RESULTS ===")
    print(f"Processed: {results['processed']}")
    print(f"Successful: {results['successful']}")
    print(f"Not found: {results['not_found']}")
    print(f"Errors: {results['errors']}")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Rate: {results['processed']/duration:.1f} teams/second")
    
    if results['successful'] > 0:
        print("\n*** SUCCESS! Threaded ingestion is working! ***")
        print("\nReady to run full ingestion (1-4000 teams)")
        print("Estimated time: 10-20 minutes with 25 concurrent threads")
        return True
    else:
        print("\n*** TEST FAILED - check the logs ***")
        return False

if __name__ == "__main__":
    test_threaded_ingest()
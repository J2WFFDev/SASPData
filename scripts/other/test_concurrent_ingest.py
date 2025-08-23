#!/usr/bin/env python3
"""Test the concurrent team ingester with a small range."""

import asyncio
import sys
import os
sys.path.append('.')

from scripts.concurrent_team_ingest import ConcurrentTeamIngester

async def test_concurrent_ingest():
    """Test concurrent ingestion with a small range."""
    print("🧪 Testing concurrent team ingestion with 50 teams...")
    
    ingester = ConcurrentTeamIngester(
        max_concurrent=10,  # Smaller for testing
        rate_limit_per_minute=24
    )
    
    # Test with teams 3200-3250 (50 teams)
    results = await ingester.process_team_range(3200, 3250, batch_size=25)
    
    print(f"🎯 Test Results:")
    print(f"  Processed: {results['processed']}")
    print(f"  Successful: {results['successful']}")
    print(f"  Not found: {results['not_found']}")
    print(f"  Errors: {results['errors']}")
    
    if results['successful'] > 0:
        print("✅ Concurrent ingestion is working!")
        
        # Ask if user wants to run full ingestion
        print("\n" + "="*60)
        print("🚀 READY FOR FULL INGESTION")
        print("Would you like to run the full 1-4000 team ingestion?")
        print("This will use 25 concurrent requests with rate limiting.")
        print("Estimated time: 15-30 minutes (much faster than sequential)")
        print("="*60)
        
        return True
    else:
        print("❌ Test failed - check the logs")
        return False

if __name__ == "__main__":
    asyncio.run(test_concurrent_ingest())
#!/usr/bin/env python3
"""
Production team ingestion script with optimized concurrent processing.
Run this to ingest all teams 1-4000 with proper rate limiting and error handling.
"""

from scripts.threaded_team_ingest import ThreadedTeamIngester
import time
import sys

def run_full_team_ingestion():
    """Run the complete team ingestion process."""
    print("=" * 80)
    print("🚀 SASP TEAM INGESTION - FULL PRODUCTION RUN")
    print("=" * 80)
    print()
    print("Configuration:")
    print("  • Teams: 1 to 4000 (4000 total)")
    print("  • Concurrent workers: 5")
    print("  • Rate limit: 20 requests/minute")
    print("  • Batch size: 50 teams per database insert")
    print("  • Estimated time: 30-45 minutes")
    print()
    
    # Ask for confirmation
    response = input("Are you ready to start the full ingestion? (y/N): ")
    if response.lower() != 'y':
        print("Cancelled by user.")
        return
    
    print("\n🚀 Starting full team ingestion...")
    
    # Initialize ingester with production settings
    ingester = ThreadedTeamIngester(
        max_workers=5,           # Conservative to prevent connection issues
        rate_limit_per_minute=20 # Conservative rate limit
    )
    
    # Run the ingestion
    start_time = time.time()
    results = ingester.process_team_range(
        start_id=1, 
        end_id=4000, 
        batch_size=50
    )
    duration = time.time() - start_time
    
    # Final report
    print("\n" + "=" * 80)
    print("🎯 FINAL INGESTION REPORT")
    print("=" * 80)
    print(f"Total processed: {results['processed']:,}")
    print(f"Successfully inserted: {results['successful']:,}")
    print(f"Not found (404): {results['not_found']:,}")
    print(f"Errors: {results['errors']:,}")
    print(f"Success rate: {(results['successful']/results['processed']*100):.1f}%")
    print(f"Total duration: {duration/60:.1f} minutes")
    print(f"Average rate: {results['processed']/duration:.1f} teams/second")
    
    if results['successful'] > 3500:  # 87.5% success rate
        print("\n✅ INGESTION SUCCESSFUL!")
        print("The team database is now complete and ready for analytics.")
    elif results['successful'] > 2000:  # 50% success rate
        print("\n⚠️  PARTIAL SUCCESS")
        print("Some teams failed to ingest. Check logs for details.")
        print("You may want to re-run ingestion for missing teams.")
    else:
        print("\n❌ INGESTION FAILED")
        print("Too many teams failed to ingest. Check configuration and network.")
    
    print("\nLogs saved to: team_ingestion_threaded.log")
    print("=" * 80)

if __name__ == "__main__":
    try:
        run_full_team_ingestion()
    except KeyboardInterrupt:
        print("\n\n⚠️  Ingestion interrupted by user")
        print("Partial results may be in the database.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
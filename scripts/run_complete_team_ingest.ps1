# Complete team ingestion with rate limiting
# This script will load ALL teams from 1 to 5000 with proper API rate limiting

param(
    [int]$MaxTeamId = 5000,
    [switch]$DryRun = $false
)

# Load environment
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match "^([^#].*?)=(.*)$") {
            $name = $matches[1]
            $value = $matches[2]
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

Write-Host "=== SASP Complete Team Ingestion ===" -ForegroundColor Green
Write-Host "Target: Load teams 1 to $MaxTeamId" -ForegroundColor Yellow
Write-Host "Rate limit: ~24 requests per minute (safe for API)" -ForegroundColor Yellow
Write-Host "Estimated time: $(($MaxTeamId / 24 * 1.2)) minutes" -ForegroundColor Yellow

if ($DryRun) {
    Write-Host "DRY RUN MODE - No actual ingestion" -ForegroundColor Cyan
}

# Confirm before starting
$confirmation = Read-Host "Continue with team ingestion? (y/n)"
if ($confirmation -ne 'y' -and $confirmation -ne 'Y') {
    Write-Host "Ingestion cancelled" -ForegroundColor Red
    exit 1
}

try {
    # Check if virtual environment exists
    if (Test-Path "venv\Scripts\activate.ps1") {
        Write-Host "Activating virtual environment..." -ForegroundColor Cyan
        & "venv\Scripts\activate.ps1"
    }

    # Run the ingestion script
    Write-Host "Starting team ingestion..." -ForegroundColor Green
    if ($DryRun) {
        python scripts\ingest_all_teams.py --dry-run --max-id $MaxTeamId
    } else {
        python scripts\ingest_all_teams.py
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Team ingestion completed successfully!" -ForegroundColor Green
        
        # Show final stats
        Write-Host "`nFinal statistics:" -ForegroundColor Cyan
        python -c "
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', '5432')),
        dbname=os.getenv('PGDATABASE', 'saspd'),
        user=os.getenv('PGUSER', 'sasp_dba'),
        password=os.getenv('PGPASSWORD', '')
    )
    
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM raw_teams')
        total = cur.fetchone()[0]
        
        cur.execute('SELECT MIN(id), MAX(id) FROM raw_teams WHERE id IS NOT NULL')
        min_id, max_id = cur.fetchone()
        
        print(f'Teams loaded: {total:,}')
        print(f'ID range: {min_id} to {max_id}')
        
        # Check for gaps
        cur.execute('''
            SELECT COUNT(*) as gaps
            FROM generate_series(1, %s) AS s(id)
            WHERE s.id NOT IN (SELECT id FROM raw_teams WHERE id IS NOT NULL)
        ''', (max_id,))
        gaps = cur.fetchone()[0]
        print(f'Missing IDs in range: {gaps:,}')
        
    conn.close()
    
except Exception as e:
    print(f'Stats failed: {e}')
"
    } else {
        Write-Host "Team ingestion failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    
} catch {
    Write-Host "Error during team ingestion: $_" -ForegroundColor Red
    exit 1
}
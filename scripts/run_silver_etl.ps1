# Script: scripts\run_silver_etl.ps1
# Purpose: Run the silver layer ETL transformations

# Move to repo root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $scriptDir
Set-Location ..

# Activate venv if it exists
if (Test-Path ".venv\Scripts\Activate.ps1") {
    . .venv\Scripts\Activate.ps1
    Write-Host "Activated Python virtual environment"
} else {
    Write-Host "Warning: No virtual environment found at .venv"
}

# Ensure dependencies are installed
pip install -r requirements.txt

# Load environment variables from .env
$envFile = Join-Path (Get-Location) '.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^[\s#]') { return }
        if ($_ -match '^[\s]*$') { return }
        $parts = $_ -split '=', 2
        if ($parts.Length -eq 2) {
            $name = $parts[0].Trim()
            $value = $parts[1].Trim()
            if ($name -ne '') { Set-Item -Path "Env:\$name" -Value $value }
        }
    }
    Write-Host "Loaded environment variables from .env"
} else {
    Write-Host "Warning: No .env file found"
}

# Run the silver ETL
Write-Host "Starting Silver Layer ETL..."
python scripts\silver\run_silver_etl.py --rebuild

if ($LASTEXITCODE -eq 0) {
    Write-Host "Silver ETL completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Silver ETL failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}
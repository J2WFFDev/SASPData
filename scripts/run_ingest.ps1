# Script: scripts\run_ingest.ps1
# Purpose: create/activate venv, load .env into environment, install deps, and run the ingest.

# Move to the scripts directory then to repo root
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $scriptDir
Set-Location ..

# Create venv if missing
if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

# If Postgres client is installed in the common location, ensure its bin is on PATH for this session
$pgBin = 'C:\Program Files\PostgreSQL\16\bin'
if (Test-Path $pgBin) {
    if (-not ($env:Path -like "*$pgBin*")) {
        $env:Path = "$pgBin;" + $env:Path
        Write-Host "Prepended $pgBin to PATH for this session"
    } else {
        Write-Host "$pgBin already present in PATH"
    }
} else {
    Write-Host "Postgres bin not found at $pgBin — leaving PATH unchanged"
}

# Activate venv
if (Test-Path ".venv\Scripts\Activate.ps1") {
    . .venv\Scripts\Activate.ps1
} else {
    Write-Host "Warning: venv activation script not found. Continuing without venv activation."
}

# Install dependencies
pip install -r requirements.txt

# Ensure .env exists, copy example if it doesn't
if (-not (Test-Path ".env")) {
    Copy-Item .env.example .env -Force
    Write-Host "Copied .env.example to .env — edit it with DB credentials before running ingest."
}

# Load .env into process environment variables
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
} else {
    Write-Host "No .env file found at $envFile"
}

# Run the ingest
python -m src.cli ingest

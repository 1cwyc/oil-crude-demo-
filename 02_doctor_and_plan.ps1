param(
    [string]$Config = "configs\tanker_pipeline_20250715.json"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ConfigPath = Join-Path $ProjectRoot $Config

if (-not (Test-Path -LiteralPath $Python)) {
    throw ".venv was not found. Run 01_setup_environment.ps1 first."
}

Push-Location $ProjectRoot
try {
    & $Python .\run_pipeline.py --config $ConfigPath doctor
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $Python .\run_pipeline.py --config $ConfigPath plan
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

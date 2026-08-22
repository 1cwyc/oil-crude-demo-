$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $Python)) {
    throw ".venv was not found. Run 01_setup_environment.ps1 first."
}

Push-Location $ProjectRoot
try {
    & $Python -m unittest discover -s tests -v
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $Python .\run_pipeline.py --config .\configs\sample_validation.json run
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

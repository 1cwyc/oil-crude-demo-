param(
    [string]$PythonCommand = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvRoot = Join-Path $ProjectRoot ".venv"
$VenvPython = Join-Path $VenvRoot "Scripts\python.exe"

if (-not (Test-Path -LiteralPath $VenvPython)) {
    if ($PythonCommand) {
        & $PythonCommand -m venv $VenvRoot
    } elseif (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3.11 -m venv $VenvRoot
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        & python -m venv $VenvRoot
    } else {
        throw "Python was not found. Install 64-bit Python 3.11 and retry."
    }
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

& $VenvPython -m pip install --upgrade pip --disable-pip-version-check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $VenvPython -m pip install -r (Join-Path $ProjectRoot "requirements.txt") --disable-pip-version-check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Environment setup completed: $VenvPython"
Write-Host "Next: run 02_doctor_and_plan.ps1."

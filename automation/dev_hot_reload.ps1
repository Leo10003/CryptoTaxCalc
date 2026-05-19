<# 
  dev_hot_reload.ps1
  Fast local iteration loop:
    - Ensures venv
    - Runs smoke tests
    - Restarts API (optional)
    - Logs timings & results

  Usage:
    pwsh ./automation/dev_hot_reload.ps1
    pwsh ./automation/dev_hot_reload.ps1 -NoServer
    pwsh ./automation/dev_hot_reload.ps1 -Port 8000 -Host 127.0.0.1

  Notes:
    - Writes hot_reload.log + states.log into support_bundles/_meta/
    - Never crashes the shell; logs errors and returns non-zero exit when failing
#>

param(
  [int]$Port = 8000,
  [string]$ApiHost = "127.0.0.1",
  [switch]$NoServer
)

$ErrorActionPreference = "Stop"

# Project root = repo root (script/..)
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$meta = Join-Path $root "support_bundles\_meta"
if (-not (Test-Path $meta)) { New-Item -ItemType Directory -Force -Path $meta | Out-Null }
$log = Join-Path $meta "hot_reload.log"
$states = Join-Path $meta "states.log"

function Write-State($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -Path $states -Value "[${ts}] DEV_HOT: $msg"
}
function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Add-Content -Path $log -Value "[${ts}] $msg"
}

Write-State "START"
Log "Starting dev hot reload"

# Ensure venv
$venvPy = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPy)) {
  Log "ERROR: .venv\\Scripts\\python.exe not found"
  Write-State "NO_VENV"
  exit 2
}

# Record versions (use -c, not bash heredoc)
try {
  $pyCmd = 'import sys, platform; print("python:", sys.version.replace("\n"," ")); print("platform:", platform.platform())'
  $pyv = & $venvPy -c $pyCmd
  foreach ($line in $pyv) { Log $line }
} catch {
  Log "WARN: failed to read python/platform: $($_.Exception.Message)"
}

# Run smoke tests
Write-State "SMOKE_BEGIN"
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$pytest = Join-Path $root ".venv\Scripts\pytest.exe"
if (-not (Test-Path $pytest)) {
  Log "ERROR: pytest not found in .venv"
  Write-State "SMOKE_NO_PYTEST"
  exit 3
}

$smokeOut = Join-Path $meta "smoke_run.txt"
try {
  Log "Running: pytest -q -m smoke -rA"
  Push-Location $root
  & $pytest -q -m smoke -rA *> $smokeOut
  $code = $LASTEXITCODE
  Pop-Location
  $sw.Stop()
  Log ("Smoke exit code: {0} in {1} ms" -f $code, $sw.ElapsedMilliseconds)
  if ($code -ne 0) {
    Log "ERROR: smoke tests failed (see smoke_run.txt)"
    Write-State "SMOKE_FAIL"
    exit $code
  }
  Write-State "SMOKE_PASS"
} catch {
  $sw.Stop()
  Log "ERROR: pytest crashed: $($_.Exception.Message)"
  Write-State "SMOKE_CRASH"
  exit 4
}

if ($NoServer) {
  Log "NoServer flag set: skipping API restart"
  Write-State "DONE"
  exit 0
}

# Kill any existing uvicorn (best-effort)
try {
  $procs = Get-Process -ErrorAction SilentlyContinue | Where-Object {
    ($_.ProcessName -match 'uvicorn|python') -and ($_.Path) -and ($_.Path -like '*\.venv\Scripts\python.exe')
  }
  foreach ($p in $procs) {
    try {
      # Try graceful close; if no window, fall back to Stop-Process
      $null = $p.CloseMainWindow()
      Start-Sleep -Milliseconds 150
      if (!$p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
    } catch {
      try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
    }
  }
  Start-Sleep -Milliseconds 300
} catch {
  Log "WARN: process scan failed: $($_.Exception.Message)"
}

# Start API (reload mode so you can iterate)
Write-State "API_START"
$uvicorn = Join-Path $root ".venv\Scripts\uvicorn.exe"
if (-not (Test-Path $uvicorn)) {
  Log "ERROR: uvicorn not found in .venv"
  Write-State "NO_UVICORN"
  exit 5
}

try {
  Log "Starting API: uvicorn cryptotaxcalc.app:app --reload --host $ApiHost --port $Port"
  Push-Location (Join-Path $root "src")
  Start-Process -FilePath $uvicorn -ArgumentList "cryptotaxcalc.app:app","--reload","--host",$ApiHost,"--port",$Port `
    -NoNewWindow
  Pop-Location
  Write-State "API_STARTED"
  Log "API started"
} catch {
  Log "ERROR: failed to start API: $($_.Exception.Message)"
  Write-State "API_FAIL"
  exit 6
}

Write-State "DONE"
exit 0

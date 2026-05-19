Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Repo root = parent of automation/
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir

function Get-FreePort {
  $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
  $listener.Start()
  $port = ($listener.LocalEndpoint).Port
  $listener.Stop()
  return $port
}

function Wait-ForHealth([int]$Port, [int]$MaxAttempts = 50, [int]$SleepMs = 200) {
  $url = "http://127.0.0.1:$Port/health"
  for ($i = 0; $i -lt $MaxAttempts; $i++) {
    try {
      $resp = Invoke-RestMethod -Uri $url -Method GET -TimeoutSec 2
      if ($null -ne $resp) { return $true }
    } catch {
      Start-Sleep -Milliseconds $SleepMs
    }
  }
  return $false
}

$uvicorn = $null

Push-Location $repoRoot
try {
  $pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
  if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
  }

  $port = Get-FreePort

  $logDir = Join-Path $repoRoot "automation\logs"
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null
  $ts = Get-Date -Format "yyyyMMdd_HHmmss"
  $uvicornLog = Join-Path $logDir "uvicorn_smoke_$ts.log"

  $env:WATCHFILES_FORCE_POLLING = "false"

  $uvicornArgs = @(
    "-m","uvicorn","cryptotaxcalc.app:app",
    "--host","127.0.0.1","--port",$port.ToString(),
    "--no-access-log"
  )

  $uvicorn = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList $uvicornArgs `
    -PassThru `
    -WindowStyle Hidden `
    -RedirectStandardOutput $uvicornLog `
    -RedirectStandardError $uvicornLog

  if (-not (Wait-ForHealth -Port $port)) {
    Write-Host "[SMOKE] ERROR: /health did not become ready. See: $uvicornLog"
    exit 2
  }

  # Run canonical smoke tests (should validate /calculate/v2 after we update smoke_test.py)
  $pytestArgs = @("-m","pytest","-q","tests\smoke_test.py")
  $pytest = Start-Process -FilePath $pythonExe -ArgumentList $pytestArgs -Wait -PassThru -NoNewWindow

  exit $pytest.ExitCode
}
finally {
  if ($uvicorn -and -not $uvicorn.HasExited) {
    Stop-Process -Id $uvicorn.Id -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 200
  }
  Pop-Location
}

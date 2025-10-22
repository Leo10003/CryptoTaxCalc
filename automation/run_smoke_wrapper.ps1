# automation\run_smoke_wrapper.ps1
$ErrorActionPreference = "Stop"
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Resolve-Path "$PSScriptRoot\.."
$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$script = Join-Path $root "automation\run_smoke_and_email.py"
$logDir = Join-Path $root "automation\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir ("smoke_" + (Get-Date -Format "yyyy-MM-dd_HH-mm-ss") + ".log")

"[$(Get-Date -Format s)] START wrapper" | Tee-Object -FilePath $log -Append
"Python: $venvPy" | Tee-Object -FilePath $log -Append
"Script: $script" | Tee-Object -FilePath $log -Append
"PWD: $root" | Tee-Object -FilePath $log -Append

Push-Location $root
try {
  & $venvPy $script *>> $log
  "[$(Get-Date -Format s)] EXIT $LASTEXITCODE" | Tee-Object -FilePath $log -Append
} catch {
  "[$(Get-Date -Format s)] ERROR $_" | Tee-Object -FilePath $log -Append
  exit 1
} finally {
  Pop-Location
}

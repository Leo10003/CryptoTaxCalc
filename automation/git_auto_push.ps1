param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Continue'   # don't abort before we capture output

# --- logging setup ---
$LogDir = Join-Path $ProjectRoot "automation\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$ts = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LogPath = Join-Path $LogDir "git_auto_push_$ts.log"
function Write-Log([string]$msg) {
    Add-Content -Path $LogPath -Value ("[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg)
}

Set-Location $ProjectRoot
Write-Log "=== Auto-push started ==="
Write-Log "ProjectRoot: $ProjectRoot"

# Optional: show remote for troubleshooting
$remoteOut = & git remote -v 2>&1
Write-Log "git remote -v:`n$remoteOut"

# Stage
Write-Log "Running git add -A"
& git add -A | Out-Null

# Commit (will no-op if nothing to commit)
$commitMsg = "auto: sync $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "Running git commit"
$commitOut = & git commit -m $commitMsg 2>&1
$commitCode = $LASTEXITCODE
Write-Log "Commit exit: $commitCode"
Write-Log "Commit output:`n$commitOut"

# Push (capture output regardless of success/failure)
Write-Log "Running git push"
$pushOut = & git push 2>&1
$pushCode = $LASTEXITCODE
Write-Log "Push exit: $pushCode"
Write-Log "Push output:`n$pushOut"

# Return non-zero on failure so FastAPI can indicate error
if ($pushCode -ne 0) {
    Write-Log "ERROR: git push failed with exit code $pushCode"
    exit 1
}
Write-Log "SUCCESS: push completed"
exit 0

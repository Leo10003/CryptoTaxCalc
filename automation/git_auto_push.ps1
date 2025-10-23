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

function Invoke-Git($cmdArgs, [switch]$AllowFail) {
    $out = & git @cmdArgs 2>&1
    $code = $LASTEXITCODE
    if (-not $AllowFail -and $code -ne 0) {
        throw "git $cmdArgs failed ($code): `n$out"
    }
    return @{ out = $out; code = $code }
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
Write-Log "Syncing with remote before push"

# Ensure we’re on main and have an upstream
Invoke-Git @("checkout","-q","main")
$upstream = (& git rev-parse --abbrev-ref --symbolic-full-name "@{u}" 2>$null)
if (-not $upstream) {
    Write-Log "No upstream set; wiring main -> origin/main"
    Invoke-Git @("branch","--set-upstream-to=origin/main","main") -AllowFail
}

# Fetch latest from origin
$fetch = Invoke-Git @("fetch","origin") -AllowFail
Write-Log ("Fetch output:`n{0}" -f $fetch.out)

# Compare positions
$behind = (& git rev-list --count "HEAD..origin/main" 2>$null)
$ahead  = (& git rev-list --count "origin/main..HEAD" 2>$null)
if (-not $behind) { $behind = 0 }
if (-not $ahead)  { $ahead  = 0 }

Write-Log "Ahead=$ahead, Behind=$behind"

# If remote is ahead, rebase our local commits on top of it
if ([int]$behind -gt 0) {
    Write-Log "Remote is ahead by $behind commit(s); running: git pull --rebase origin main"
    $pull = Invoke-Git @("pull","--rebase","origin","main") -AllowFail
    if ($pull.code -ne 0) {
        Write-Log ("ERROR: rebase failed. Output:`n{0}" -f $pull.out)
        throw "Aborting push due to rebase error."
    }
    Write-Log ("Rebase output:`n{0}" -f $pull.out)
}

# Now push
Write-Log "Running git push"
$push = Invoke-Git @("push","origin","HEAD:main") -AllowFail
Write-Log ("Push exit={0}`nPush output:`n{1}" -f $push.code, $push.out)
if ($push.code -ne 0) { throw "git push failed" }

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

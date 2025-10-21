# automation/git_auto_push.ps1
# Purpose: Auto-commit & push any local changes to GitHub.
# Adds: logging (daily files) + 30-day retention cleanup.

param(
    [string]$ProjectRoot = (Resolve-Path "$PSScriptRoot\..").Path
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- Paths -------------------------------------------------------
$logsDir = Join-Path $PSScriptRoot "logs"
if (-not (Test-Path $logsDir)) { New-Item -ItemType Directory -Force -Path $logsDir | Out-Null }

$logPath = Join-Path $logsDir ("git_auto_push_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

function Write-Log {
    param([string]$msg)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:sszzz"
    $line = "[{0}] {1}" -f $timestamp, $msg
    $line | Out-File -FilePath $logPath -Append -Encoding UTF8
}

# --- Log header --------------------------------------------------
Write-Log "=== Auto-push started ==="
Write-Log "ProjectRoot: $ProjectRoot"
Push-Location $ProjectRoot

try {
    # Ensure repo
    if (-not (Test-Path ".git")) {
        throw "No .git directory found at $ProjectRoot. Initialize git before using this script."
    }

    # Configure safe directory (for some Git installations)
    git config --global --add safe.directory "$ProjectRoot" | Out-Null

    # Status
    $status = git status --porcelain
    if ([string]::IsNullOrWhiteSpace($status)) {
        Write-Log "No changes to commit."
    }
    else {
        # Stage & commit
        git add -A
        $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ssK"
        $commitMsg = "Auto-sync: $stamp"
        git commit -m "$commitMsg" | Out-Null
        Write-Log "Committed: $commitMsg"
    }

    # Always attempt push (safe even if nothing to push)
    git push | Tee-Object -Variable pushOut | Out-Null
    Write-Log "Push output: $pushOut"

    Write-Log "=== Auto-push complete ==="
}
catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    throw
}
finally {
    Pop-Location
}

# --- Cleanup old logs (>30 days) --------------------------------
try {
    Get-ChildItem -Path $logsDir -File |
        Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
        Remove-Item -Force -ErrorAction SilentlyContinue
    Write-Log "Old logs cleanup completed (kept last 30 days)."
}
catch {
    Write-Log "Log cleanup failed: $($_.Exception.Message)"
}

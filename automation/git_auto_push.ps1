# automation/git_auto_push.ps1
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$ErrorActionPreference = "Stop"

# --- config ---
$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir   = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath  = Join-Path $logDir ("git_auto_push_{0}.log" -f (Get-Date -Format "yyyy-MM-dd"))

function Write-Log([string]$msg) {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line  = "[$stamp] $msg"
    $line | Out-File -FilePath $logPath -Append -Encoding utf8
    Write-Output $line
}

# Retain 30 days of logs
Get-ChildItem $logDir -Filter "git_auto_push_*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -Force -ErrorAction SilentlyContinue

# --- work ---
$commitOut = ""
$pushOut   = ""

try {
    Set-Location $repoRoot
    Write-Log "Running git add -A"
    git add -A | Out-Null

    Write-Log "Running git commit"
    $commitOut = git commit -m ("auto: sync {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss")) 2>&1
    if (-not $commitOut -or $commitOut -match "nothing to commit") {
        Write-Log "No changes to commit."
    } else {
        Write-Log "Commit output:`n$commitOut"
    }

    Write-Log "Running git push"
    $pushOut = git push 2>&1
    Write-Log "Push output:`n$pushOut"

    exit 0
}
catch {
    Write-Log ("ERROR: {0}" -f $_.Exception.Message)
    if ($_.InvocationInfo.PositionMessage) {
        Write-Log $_.InvocationInfo.PositionMessage
    }
    exit 1
}

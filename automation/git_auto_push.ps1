# automation/git_auto_push.ps1
# Safe auto-push with stale lock handling + strict git exit-code checks + optional worktree bootstrap.

param(
  [Parameter(Mandatory=$true)]
  [string]$RepoRoot,                 # where autosync runs (ideally a dedicated worktree)
  [string]$RemoteName = "origin",
  [string]$Branch = "main",
  [int]$MaxDeletePct = 2,
  [int]$MaxDeleteAbs = 50,
  [string[]]$OnlyPaths = @(),
  [switch]$WhatIf,
  [string]$WorktreeRoot = ""         # OPTIONAL: dev repo root to bootstrap a worktree at RepoRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function TS { (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") }

function Invoke-Git {
  param([Parameter(Mandatory=$true)][string[]]$Args)
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = (Get-Command git -ErrorAction Stop).Source
  $psi.Arguments = ($Args -join ' ')
  $psi.WorkingDirectory = $PWD.Path
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute = $false
  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  [void]$p.Start()
  $out = $p.StandardOutput.ReadToEnd()
  $err = $p.StandardError.ReadToEnd()
  $p.WaitForExit()
  if ($p.ExitCode -ne 0) {
    throw "git $($Args -join ' ') failed (exit $($p.ExitCode)): $err$out"
  }
  if (-not [string]::IsNullOrWhiteSpace($out)) { Write-Host $out.TrimEnd() }
  if (-not [string]::IsNullOrWhiteSpace($err)) { Write-Warning $err.TrimEnd() }
}

function Test-GitBusy {
  try { return $null -ne (Get-Process git -ErrorAction SilentlyContinue) } catch { return $false }
}

function Clear-StaleIndexLock {
  param([Parameter(Mandatory=$true)][string]$RepoPath)
  $lockPath = Join-Path $RepoPath ".git\index.lock"
  if (Test-Path $lockPath) {
    if (Test-GitBusy) {
      Write-Warning "[WARN] $(TS) .git\index.lock exists and git appears active. Not removing."
      return $false
    } else {
      Write-Warning "[WARN] $(TS) Removing stale lock: $lockPath"
      Remove-Item -Force -LiteralPath $lockPath
      return $true
    }
  }
  return $false
}

# --- optional: ensure worktree exists ---
if ($WorktreeRoot -and (Test-Path $WorktreeRoot)) {
  $gitDir = Join-Path $WorktreeRoot ".git"
  if (-not (Test-Path $gitDir)) { throw "WorktreeRoot is not a git repo: $WorktreeRoot" }
  if (-not (Test-Path $RepoRoot)) {
    Write-Host "[INFO] $(TS) Creating autosync worktree at $RepoRoot tracking $RemoteName/$Branch ..."
    Push-Location $WorktreeRoot
    Invoke-Git @('fetch',$RemoteName,'--quiet')
    Invoke-Git @('worktree','add','--detach',$RepoRoot,"$RemoteName/$Branch")
    Pop-Location
    Push-Location $RepoRoot
    Invoke-Git @('checkout','-b',$Branch,'--track',"$RemoteName/$Branch")
    Pop-Location
  }
}

# --- main ---
$root = (Resolve-Path $RepoRoot).Path
$meta = Join-Path $root "support_bundles\_meta"
New-Item -ItemType Directory -Force -Path $meta | Out-Null
$logPath = Join-Path $meta ("git_autosync_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
Start-Transcript -Path $logPath -Append | Out-Null

try {
  Set-Location $root
  if (-not (Test-Path ".git")) { throw "Not a git repo: $root" }
  $gitBin = (Get-Command git -ErrorAction Stop).Source
  Write-Host "[INFO] $(TS) git=$gitBin"
  Write-Host "[INFO] $(TS) Remote=$RemoteName Branch=$Branch"

  # Abort if working tree dirty (protect developer clone)
  $porcelain = (& git status --porcelain)
  if (-not [string]::IsNullOrWhiteSpace($porcelain)) {
    throw "Working tree has uncommitted changes. Aborting autosync to avoid touching dev files."
  }

  # Clean stale lock if any
  [void](Clear-StaleIndexLock -RepoPath $root)

  # safer pulls
  Invoke-Git @('config','--local','pull.rebase','true')
  Invoke-Git @('config','--local','merge.ff','only')

  # fetch + divergence
  Invoke-Git @('fetch', $RemoteName, '--quiet')
  $counts = & git rev-list --left-right --count "$RemoteName/$Branch...$Branch" 2>$null
  $behind, $ahead = 0,0
  if ($counts) {
    $parts = $counts -split '\s+'
    if ($parts.Count -ge 2) { $behind = [int]$parts[0]; $ahead=[int]$parts[1] }
  }
  Write-Host "[INFO] $(TS) Divergence: behind=$behind ahead=$ahead"

  if ($behind -gt 0) {
    Write-Host "[INFO] $(TS) Pulling with rebase…"
    Invoke-Git @('pull','--rebase',$RemoteName,$Branch)
  }

  # count tracked & deletions
  $trackedRaw = (& git ls-files -z) -join ""
  $trackedCount = (($trackedRaw -split "`0") | Where-Object { $_ -ne "" } | Measure-Object).Count
  $porc = (& git status --porcelain -z) -join ""
  $entries = ($porc -split "`0") | Where-Object { $_ -ne "" }
  $delCount = 0
  foreach ($e in $entries) { if ($e.Length -ge 2 -and $e.Substring(0,2).Contains('D')) { $delCount++ } }
  $delPct = if ($trackedCount -gt 0) { [math]::Round(100.0 * $delCount / $trackedCount, 2) } else { 0 }
  Write-Host "[INFO] $(TS) tracked=$trackedCount deletions=$delCount (${delPct}%)"
  if (($delCount -gt $MaxDeleteAbs) -or ($delPct -gt $MaxDeletePct)) {
    throw "Deletion threshold exceeded (del=$delCount, pct=$delPct%). Aborting push."
  }

  # Stage
  if ($WhatIf) {
    Write-Host "[DRY-RUN] Would stage: " + ($(if ($OnlyPaths) { $OnlyPaths -join ', ' } else { 'ALL changes (git add -A)' }))
  } else {
    if ($OnlyPaths.Count -gt 0) { foreach ($p in $OnlyPaths) { if (Test-Path $p) { Invoke-Git @('add',$p) } } }
    else { Invoke-Git @('add','-A') }
  }

  $diffIndex = (& git diff --cached --name-only)
  if ([string]::IsNullOrWhiteSpace($diffIndex)) {
    Write-Host "[INFO] $(TS) Nothing to commit. Exiting."
    exit 0
  } else {
    Write-Host "[INFO] Staged files:`n$diffIndex"
  }

  if ($WhatIf) {
    Write-Host "[DRY-RUN] Would tag, commit and push. Exiting."
    exit 0
  }

  # Safety tag + commit + push
  $stamp = (Get-Date -Format "yyyyMMdd_HHmmss")
  $safeTag = "autosync/safety/$stamp"
  Invoke-Git @('tag','-f',$safeTag)
  $hostname = [System.Net.Dns]::GetHostName()
  $user = $env:USERNAME
  $msg = "auto: sync $stamp on $hostname by $user"
  Invoke-Git @('commit','-m', $msg)
  Invoke-Git @('push',$RemoteName,"HEAD:$Branch")

  Write-Host "[OK] $(TS) Autosync completed."
  exit 0
}
catch {
  Write-Host "[ERROR] $(TS) $($_.Exception.Message)"
  exit 2
}
finally {
  Stop-Transcript | Out-Null
}

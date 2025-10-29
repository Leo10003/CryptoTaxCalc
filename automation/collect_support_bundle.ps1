# automation\collect_support_bundle.ps1
# Purpose: Collect a clean, timestamped support bundle ZIP inside /support_bundles
# Compatible with PowerShell 7+ (works on Windows PowerShell 5 with limited features)
# Usage:  pwsh ./automation/collect_support_bundle.ps1 -ApiBase "http://127.0.0.1:8000" -TailLines 300
# Params:
#   -ApiBase         : Base URL for API health checks (default http://127.0.0.1:8000)
#   -TailLines       : How many lines to tail from each log (default 400)
#   -ExpectedTables  : Expected DB tables for "missing tables" check (default: calc_runs, transactions, fx_rates, fx_batches)

[CmdletBinding()]
param(
  [string]$ApiBase = "http://127.0.0.1:8000",
  [int]$TailLines = 400,
  [string[]]$ExpectedTables = @('calc_runs','transactions','fx_rates','fx_batches')
)

$ErrorActionPreference = "Continue"
$ProgressPreference = "SilentlyContinue"

# -----------------------------
# 0) Helpers
# -----------------------------
function Write-TextSafe {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)][string]$Path,
    [Parameter(Mandatory=$true)][string]$Content
  )
  try {
    $dir = Split-Path -Parent $Path
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  } catch {}
  try { $Content | Out-File -FilePath $Path -Encoding UTF8 -Force } catch {}
}

function Get-FirstExistingPath {
  [CmdletBinding()]
  param([string[]]$Candidates)
  foreach ($c in $Candidates) {
    if ($c -and (Test-Path -LiteralPath $c)) { return $c }
  }
  return $null
}

function Compress-Bundle {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory=$true)][string]$SourceDir,
    [Parameter(Mandatory=$true)][string]$ZipPath
  )

  # Build explicit file list (avoid wildcard bugs)
  $items = @(Get-ChildItem -LiteralPath $SourceDir -Recurse -File -ErrorAction SilentlyContinue |
             ForEach-Object { $_.FullName })
  if (-not $items -or $items.Count -eq 0) {
    throw "No files collected into $SourceDir"
  }

  # Ensure target is clear
  if (Test-Path -LiteralPath $ZipPath) {
    Remove-Item -LiteralPath $ZipPath -Force -ErrorAction SilentlyContinue
  }

  # Try Compress-Archive with retries (AV/file locks)
  $ok = $false
  for ($i=1; $i -le 3 -and -not $ok; $i++) {
    try {
      Compress-Archive -LiteralPath $items -DestinationPath $ZipPath -Force
      $ok = $true
    } catch {
      Start-Sleep -Seconds (2 * $i)
      if ($i -eq 3) {
        # Optional fallback to 7z if available
        $cmd = Get-Command 7z -ErrorAction SilentlyContinue
        if ($null -ne $cmd) {
          $sevenZip = $cmd.Source
        } else {
          $sevenZip = $null
        }

        if ($sevenZip) {
          try {
            Push-Location $SourceDir
            & $sevenZip a -tzip "$ZipPath" ".\*" -mx=5 | Out-Null
            Pop-Location
            $ok = $true
          } catch {
            Pop-Location
            throw "7z fallback failed: $($_.Exception.Message)"
          }
        } else {
          throw "Compress-Archive failed and 7z not available: $($_.Exception.Message)"
        }
      }
    }
  }
  return $ok
}

# -----------------------------
# 1) Paths & temp workspace
# -----------------------------
try {
  $projRoot = Split-Path -Parent $PSScriptRoot
} catch {
  $projRoot = (Resolve-Path ".").Path
}
$stamp = (Get-Date).ToString("yyyy-MM-dd_HH-mm-ss")
$bundleRoot = Join-Path $projRoot "support_bundles"
New-Item -ItemType Directory -Force -Path $bundleRoot | Out-Null

$bundleDir = Join-Path $bundleRoot "bundle_$stamp"
$newZip    = Join-Path $bundleRoot "support_bundle_$stamp.zip"
New-Item -ItemType Directory -Force -Path $bundleDir | Out-Null

$metaDir = Join-Path $bundleDir "_meta"
New-Item -ItemType Directory -Force -Path $metaDir | Out-Null

# Start transcript (we'll stop it BEFORE zipping to release file lock)
$transcriptPath = Join-Path $metaDir "build_transcript.txt"
try { Start-Transcript -Path $transcriptPath -Force -ErrorAction SilentlyContinue | Out-Null } catch {}

# -----------------------------
# 2) Collect metadata & context
# -----------------------------
try {
  Write-TextSafe (Join-Path $metaDir "ps_version.json") ((Get-Variable PSVersionTable -ValueOnly | ConvertTo-Json -Depth 5))
} catch {}
try { Write-TextSafe (Join-Path $metaDir "env.txt") ((Get-ChildItem Env: | Sort-Object Name | Format-Table -AutoSize | Out-String)) } catch {}
try { Write-TextSafe (Join-Path $metaDir "host_info.txt") ((Get-ComputerInfo | Out-String)) } catch {}
try { Write-TextSafe (Join-Path $metaDir "modules.txt") ((Get-Module -ListAvailable | Select-Object Name, Version, Path | Sort-Object Name | Format-Table -AutoSize | Out-String)) } catch {}
try {
  if (Test-Path (Join-Path $projRoot ".git")) {
    Write-TextSafe (Join-Path $metaDir "git_status.txt") ((git status --porcelain -b 2>$null) | Out-String)
    Write-TextSafe (Join-Path $metaDir "git_log.txt") ((git log -n 20 --oneline 2>$null) | Out-String)
  }
} catch {}

# -----------------------------
# 3) Collect repo files of interest
# -----------------------------
$collectList = @(
  ".env",
  "README.md",
  "requirements.txt",
  "pyproject.toml",
  "Pipfile","Pipfile.lock","poetry.lock","uv.lock",
  "automation",
  "backups",
  "samples",
  "support_bundles/.keep",  # placeholder to avoid self-inclusion
  "src",
  "tests",
  "logs",
  "storage_raw",
  "storage_normalized"
)

foreach ($rel in $collectList) {
  $src = Join-Path $projRoot $rel
  $dst = Join-Path $bundleDir $rel
  try {
    if (Test-Path -LiteralPath $src) {
      if ((Get-Item -LiteralPath $src).PSIsContainer) {
        Copy-Item -LiteralPath $src -Destination $dst -Recurse -Force -ErrorAction SilentlyContinue
      } else {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dst) | Out-Null
        Copy-Item -LiteralPath $src -Destination $dst -Force -ErrorAction SilentlyContinue
      }
    }
  } catch {
    $errPath = Join-Path $metaDir "copy_errors.txt"
    $prev = if (Test-Path $errPath) { Get-Content $errPath -Raw } else { "" }
    Write-TextSafe $errPath ("Failed to copy $rel`n$($_.Exception.Message)`n---`n$prev")
  }
}

# -----------------------------
# 4) Python / pip diagnostics
# -----------------------------
$pyCandidates = @(
  (Join-Path $projRoot ".venv\Scripts\python.exe"),
  (Join-Path $projRoot "venv\Scripts\python.exe"),
  "py.exe","python.exe","python3.exe"
)
$python = Get-FirstExistingPath $pyCandidates
try {
  if ($python) {
    Write-TextSafe (Join-Path $metaDir "python_version.txt") (& $python --version 2>&1 | Out-String)
    Write-TextSafe (Join-Path $metaDir "pip_list.txt") (& $python -m pip list 2>&1 | Out-String)
    Write-TextSafe (Join-Path $metaDir "pip_freeze.txt") (& $python -m pip freeze 2>&1 | Out-String)
  } else {
    Write-TextSafe (Join-Path $metaDir "python_warning.txt") "No Python found (.venv, venv, or on PATH). Skipping Python/pip diagnostics."
  }
} catch {}

# -----------------------------
# 5) Quick DB diagnostics (SQLite) + Missing tables
# -----------------------------
$dbCandidates = @(
  (Join-Path $projRoot "cryptotaxcalc.db"),
  (Join-Path $projRoot "data.db"),
  (Join-Path $projRoot "src\cryptotaxcalc\cryptotaxcalc.db")
)
$dbPath = Get-FirstExistingPath $dbCandidates
$dbMetaDir = Join-Path $bundleDir "_db"
New-Item -ItemType Directory -Force -Path $dbMetaDir | Out-Null

try {
  if ($python -and $dbPath) {
@"
import sqlite3, json, os, time
db = r'''$dbPath'''
outdir = r'''$dbMetaDir'''
os.makedirs(outdir, exist_ok=True)
rep = {}
try:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    # schema
    cur.execute("SELECT name, type, sql FROM sqlite_master WHERE type in ('table','index') AND sql NOT NULL")
    rep["schema"] = [dict(r) for r in cur.fetchall()]
    # quick table counts
    tables = [r["name"] for r in rep["schema"] if r["type"]=="table"]
    counts = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
            counts[t] = cur.fetchone()["n"]
        except Exception as e:
            counts[t] = f"err: {e}"
    rep["counts"] = counts
    # tails (best-effort)
    def tail(q):
        try:
            cur.execute(q)
            return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            return {"error": str(e), "query": q}
    rep["fx_batches_tail"]   = tail("SELECT * FROM fx_batches ORDER BY id DESC LIMIT 10")
    rep["fx_rates_tail"]     = tail("SELECT * FROM fx_rates ORDER BY id DESC LIMIT 10")
    rep["transactions_tail"] = tail("SELECT * FROM transactions ORDER BY ROWID DESC LIMIT 10")
    rep["transactions_stats"]= tail("SELECT COUNT(*) AS n, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM transactions")
except Exception as e:
    rep["db_error"] = str(e)
open(os.path.join(outdir, "db_diag.json"), "w", encoding="utf-8").write(json.dumps(rep, indent=2, ensure_ascii=False))
"@ | Out-File -FilePath (Join-Path $dbMetaDir "db_diag.py") -Encoding UTF8
    & $python (Join-Path $dbMetaDir "db_diag.py") | Out-Null
  } else {
    Write-TextSafe (Join-Path $dbMetaDir "db_diag_skipped.txt") "Skipped DB diag: dbPath=$dbPath ; python=$python"
  }
} catch {
  Write-TextSafe (Join-Path $dbMetaDir "db_diag_error.txt") $_.Exception.Message
}

# Missing tables check
try {
  $dbDiagJsonPath = Join-Path $dbMetaDir 'db_diag.json'
  if (Test-Path -LiteralPath $dbDiagJsonPath) {
    $dbdiag = Get-Content -LiteralPath $dbDiagJsonPath -Raw -ErrorAction Stop | ConvertFrom-Json
    $presentTables = @()
    if ($dbdiag.schema) {
      foreach ($s in $dbdiag.schema) {
        if ($s.type -eq 'table' -and $s.name) { $presentTables += [string]$s.name }
      }
    }
    $missing = $ExpectedTables | Where-Object { $_ -notin $presentTables }
    ($ExpectedTables -join "`r`n") | Out-File -FilePath (Join-Path $dbMetaDir 'expected_tables.txt') -Encoding UTF8 -Force
    if ($missing -and $missing.Count -gt 0) {
      ("Missing tables:`r`n" + ($missing -join "`r`n")) | Out-File -FilePath (Join-Path $dbMetaDir 'missing_tables.txt') -Encoding UTF8 -Force
    } else {
      "OK: All expected tables are present." | Out-File -FilePath (Join-Path $dbMetaDir 'missing_tables.txt') -Encoding UTF8 -Force
    }
  } else {
    "db_diag.json not found; cannot compute missing tables." | Out-File -FilePath (Join-Path $dbMetaDir 'missing_tables.txt') -Encoding UTF8 -Force
  }
} catch {
  ("Error computing missing tables: " + $_.Exception.Message) | Out-File -FilePath (Join-Path $dbMetaDir 'missing_tables_error.txt') -Encoding UTF8 -Force
}

# -----------------------------
# 6) API health/version
# -----------------------------
try {
  $apiDir = Join-Path $bundleDir "_api"
  New-Item -ItemType Directory -Force -Path $apiDir | Out-Null

  foreach ($ep in @("/health","/version")) {
    try {
      $uri = "$ApiBase$ep"
      $r = Invoke-WebRequest -Uri $uri -Method GET -UseBasicParsing -TimeoutSec 10
      Write-TextSafe (Join-Path $apiDir ("GET" + $ep.Replace('/','_') + ".json")) ($r.Content)
      Write-TextSafe (Join-Path $apiDir ("GET" + $ep.Replace('/','_') + ".status.txt")) ("HTTP " + $r.StatusCode)
    } catch {
      Write-TextSafe (Join-Path $apiDir ("GET" + $ep.Replace('/','_') + ".error.txt")) $_.Exception.Message
    }
  }
} catch {}

# -----------------------------
# 7) Logs: tail last N lines
# -----------------------------
try {
  $logDirs = @(
    (Join-Path $projRoot "automation\logs"),
    (Join-Path $projRoot "logs")
  )
  $outLogDir = Join-Path $bundleDir "_logs"
  New-Item -ItemType Directory -Force -Path $outLogDir | Out-Null

  foreach ($ld in $logDirs) {
    if (Test-Path -LiteralPath $ld) {
      $files = Get-ChildItem -LiteralPath $ld -File -Recurse -ErrorAction SilentlyContinue
      foreach ($f in $files) {
        try {
          $dest = Join-Path $outLogDir ($f.FullName.Substring($ld.Length).TrimStart('\','/'))
          New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dest) | Out-Null
          if ($TailLines -gt 0) {
            Get-Content -LiteralPath $f.FullName -Tail $TailLines -ErrorAction SilentlyContinue | Out-File -FilePath $dest -Encoding UTF8 -Force
          } else {
            Copy-Item -LiteralPath $f.FullName -Destination $dest -Force
          }
        } catch {}
      }
    }
  }
} catch {}

# -----------------------------
# 8) File inventory + hashes
# -----------------------------
try {
  $invPath = Join-Path $metaDir "inventory.csv"
  "Path,Length,LastWriteTime,SHA256" | Out-File -FilePath $invPath -Encoding UTF8 -Force
  $files = Get-ChildItem -LiteralPath $bundleDir -Recurse -File -ErrorAction SilentlyContinue
  foreach ($f in $files) {
    try {
      $hash = (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256 -ErrorAction SilentlyContinue).Hash
      "$($f.FullName),$($f.Length),$($f.LastWriteTime.ToString('s')),$hash" | Out-File -FilePath $invPath -Append -Encoding UTF8
    } catch {}
  }
} catch {}

# -----------------------------
# 9) Build manifest
# -----------------------------
try {
  $manifest = [ordered]@{
    created_at   = (Get-Date).ToString("s")
    project_root = $projRoot
    bundle_dir   = $bundleDir
    zip_target   = $newZip
    api_base     = $ApiBase
    tail_lines   = $TailLines
    ps_edition   = $PSVersionTable.PSEdition
    ps_version   = $PSVersionTable.PSVersion.ToString()
    expected_tables = $ExpectedTables
  }
  ($manifest | ConvertTo-Json -Depth 5) | Out-File -FilePath (Join-Path $bundleDir "manifest.json") -Encoding UTF8 -Force
} catch {}

# -----------------------------
# 9.5) Debug snapshot (quick summary)
# -----------------------------
try {
  $debugSnapshot = [ordered]@{
    timestamp       = (Get-Date).ToString("s")
    project_root    = $projRoot
    ps_version      = $PSVersionTable.PSVersion.ToString()
    ps_edition      = $PSVersionTable.PSEdition
    os              = (Get-CimInstance Win32_OperatingSystem).Caption
    user            = $env:USERNAME
    python_path     = $python
    python_version  = (if ($python) { (& $python --version 2>$null) } else { "N/A" })
    db_path         = $dbPath
    db_exists       = (if ($dbPath) { Test-Path -LiteralPath $dbPath } else { $false })
    bundle_dir      = $bundleDir
    zip_target      = $newZip
    tail_lines      = $TailLines
    api_base        = $ApiBase
    env_debug_mode  = (if (Test-Path "$projRoot\.env") {
                         (Select-String -Path "$projRoot\.env" -Pattern "DEBUG_MODE\s*=\s*(.+)" -ErrorAction SilentlyContinue).Matches.Groups[1].Value
                       } else { "not set" })
    git_branch      = (try { (git rev-parse --abbrev-ref HEAD 2>$null) } catch { "N/A" })
    git_commit      = (try { (git rev-parse HEAD 2>$null) } catch { "N/A" })
  }
  $debugPath = Join-Path $metaDir "debug_snapshot.json"
  ($debugSnapshot | ConvertTo-Json -Depth 5) | Out-File -FilePath $debugPath -Encoding UTF8 -Force
} catch {
  Write-TextSafe (Join-Path $metaDir "debug_snapshot_error.txt") $_.Exception.Message
}

# -----------------------------
# 10) STOP transcript BEFORE zipping (release file lock)
# -----------------------------
try { Stop-Transcript | Out-Null } catch {}

# -----------------------------
# 11) Create the ZIP (robust) + retention + cleanup
# -----------------------------
$zipOk = $false
try {
  $zipOk = Compress-Bundle -SourceDir $bundleDir -ZipPath $newZip
} catch {
  Write-TextSafe (Join-Path $metaDir "zip_error.txt") $_.Exception.Message
  $zipOk = $false
}

# keep last 5 bundles
try {
  Get-ChildItem -LiteralPath $bundleRoot -Filter "support_bundle_*.zip" -File |
    Sort-Object LastWriteTime -Descending |
    Select-Object -Skip 5 |
    Remove-Item -Force -ErrorAction SilentlyContinue
} catch {}

# cleanup temp (best-effort)
try { Remove-Item -LiteralPath $bundleDir -Recurse -Force -ErrorAction SilentlyContinue } catch {}

if ($zipOk) {
  Write-Host "✅ Created: $newZip"
  exit 0
} else {
  Write-Host "❌ Failed to create zip. See _meta\zip_error.txt and build_transcript.txt (if present)."
  exit 2
}

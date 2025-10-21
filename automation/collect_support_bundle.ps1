# automation\collect_support_bundle.ps1
# Purpose: Collect a clean, timestamped support bundle ZIP inside /support_bundles
# Compatible with Windows PowerShell 5 and PowerShell 7

param(
  [string]$ApiBase = "http://127.0.0.1:8000",
  [int]$TailLines = 400
)

$ErrorActionPreference = "Continue"

# 1) Paths setup
$projRoot = Split-Path -Parent $PSScriptRoot
$stamp = (Get-Date).ToString("yyyy-MM-dd_HH-mm-ss")
$bundleRoot = Join-Path $projRoot "support_bundles"
New-Item -ItemType Directory -Force -Path $bundleRoot | Out-Null

$bundleDir = Join-Path $bundleRoot "temp_support_bundle_$stamp"
$newZip = Join-Path $bundleRoot "support_bundle_$stamp.zip"

New-Item -ItemType Directory -Force -Path $bundleDir | Out-Null

# 2) Collect key files
$files = @(
  "data.db",
  ".env",
  "requirements.txt",
  "automation\fx_ecb.csv",
  "automation\nightly_fx_task.xml",
  "automation\nightly_smoke_task.xml",
  "automation\run_smoke_and_email.py",
  "smoke_test.py",
  "src\cryptotaxcalc\app.py",
  "src\cryptotaxcalc\db.py",
  "src\cryptotaxcalc\fifo_engine.py",
  "src\cryptotaxcalc\fx_utils.py",
  "src\cryptotaxcalc\schemas.py",
  "src\cryptotaxcalc\models.py",
  "src\cryptotaxcalc\audit_utils.py",
  "src\cryptotaxcalc\audit_digest.py",
  "src\cryptotaxcalc\utils_files.py"
) | ForEach-Object { Join-Path $projRoot $_ }

foreach ($f in $files) {
  if (-not (Test-Path $f)) { continue }

  $abs = (Resolve-Path $f).Path
  if ($abs.StartsWith($projRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    $rel = $abs.Substring($projRoot.Length).TrimStart('\','/')
  } else {
    $rel = Join-Path "_external" (Split-Path $abs -Leaf)
  }

  $dest = Join-Path $bundleDir $rel
  $destDir = Split-Path $dest -Parent
  if (-not [string]::IsNullOrWhiteSpace($destDir)) {
    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
  }

  Copy-Item -LiteralPath $abs -Destination $dest -Force
}

# 3) Sanitize .env
$envPath = Join-Path $bundleDir ".env"
if (Test-Path $envPath) {
  (Get-Content $envPath) |
    ForEach-Object {
      if ($_ -match '^\s*([A-Za-z0-9_]+)\s*=\s*(.*)$') {
        "$($matches[1])=***REDACTED***"
      } else { $_ }
    } | Set-Content $envPath -Encoding UTF8
}

# 4) Versions and environment
try {
  & "$projRoot\.venv\Scripts\python.exe" -V 2>&1 | Out-File -FilePath (Join-Path $bundleDir "python_version.txt") -Encoding UTF8
  & "$projRoot\.venv\Scripts\python.exe" -m pip freeze 2>&1 | Out-File -FilePath (Join-Path $bundleDir "pip_freeze.txt") -Encoding UTF8
} catch {}

# 5) Quick DB diagnostics
$sqlite = "$projRoot\.venv\Scripts\python.exe"
$diagPy = @"
import sqlite3, json, sys, os
db = r'$projRoot\\data.db'
outdir = r'$bundleDir'
if os.path.exists(db):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    def dump(q, name):
        try:
            cur.execute(q)
            data = [dict(r) for r in cur.fetchall()]
            open(os.path.join(outdir, name), 'w', encoding='utf-8').write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            open(os.path.join(outdir, name), 'w', encoding='utf-8').write(str(e))
    open(os.path.join(outdir, 'db_schema.sql'),'w',encoding='utf-8').write('\\n'.join([r[0] for r in conn.execute('SELECT sql FROM sqlite_master WHERE type in (\"table\",\"index\") AND sql NOT NULL')]))
    dump('SELECT * FROM fx_batches ORDER BY id DESC LIMIT 5','fx_batches.json')
    dump('SELECT date, usd_per_eur, batch_id FROM fx_rates ORDER BY date DESC LIMIT 5','fx_rates_tail.json')
    dump('SELECT id, started_at, finished_at, jurisdiction, rule_version, lot_method, fx_set_id FROM calc_runs ORDER BY id DESC LIMIT 5','calc_runs_tail.json')
    dump('SELECT id, run_id, timestamp, asset, proceeds, cost_basis, gain FROM realized_events ORDER BY id DESC LIMIT 10','realized_tail.json')
    dump('SELECT COUNT(*) AS n, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM transactions','transactions_stats.json')
"@
$diagPath = Join-Path $bundleDir "db_diag.py"
$diagPy | Out-File -FilePath $diagPath -Encoding UTF8
try { & "$sqlite" "$diagPath" | Out-Null } catch {}

# 6) API health/version
try {
  Invoke-WebRequest -UseBasicParsing "$ApiBase/health" -TimeoutSec 10 | Select-Object -ExpandProperty Content | Out-File (Join-Path $bundleDir "health.json")
} catch {}
try {
  Invoke-WebRequest -UseBasicParsing "$ApiBase/version" -TimeoutSec 10 | Select-Object -ExpandProperty Content | Out-File (Join-Path $bundleDir "version.json")
} catch {}

# 7) Zip everything
if (Test-Path $newZip) { Remove-Item $newZip -Force }
Compress-Archive -Path "$bundleDir\*" -DestinationPath $newZip -Force

# 8) Auto-clean older bundles (keep 5 most recent)
$maxBundles = 5
Get-ChildItem $bundleRoot -Filter "support_bundle_*.zip" |
  Sort-Object LastWriteTime -Descending |
  Select-Object -Skip $maxBundles |
  Remove-Item -Force -ErrorAction SilentlyContinue

# 9) Remove temporary folder
Remove-Item -Path $bundleDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "`n✅ Support bundle created:`n$newZip`n"

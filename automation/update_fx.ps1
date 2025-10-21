# update_fx.ps1 (robust ZIP + TLS1.2 + validation)
$base = "C:\Users\picci\Desktop\CryptoTaxCalc\automation"
$zip  = Join-Path $base "eurofxref-hist.zip"
$csv  = Join-Path $base "eurofxref-hist.csv"   # extracted name inside the zip
$out  = Join-Path $base "fx_ecb.csv"

# 0) Ensure folder
if (!(Test-Path $base)) { New-Item -ItemType Directory -Path $base | Out-Null }

# 1) Force TLS 1.2 (ECB requires modern TLS)
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# 2) Download official ZIP (more reliable than CSV endpoint)
$ecbZipUrl = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
Invoke-WebRequest -Uri $ecbZipUrl -OutFile $zip -UseBasicParsing -Headers @{ "User-Agent" = "Mozilla/5.0" }

# 3) Extract csv (overwrite if exists)
if (Test-Path $csv) { Remove-Item $csv -Force }
Expand-Archive -Path $zip -DestinationPath $base -Force

# 4) Parse CSV reliably
$rows = Import-Csv -Path $csv

# 5) Validate schema
$headers = $rows[0].PSObject.Properties.Name
if (-not ($headers -contains 'Date' -and $headers -contains 'USD')) {
  Write-Error "ECB CSV schema not recognized (missing Date/USD). Aborting."
  exit 1
}

# 6) Validate recency (latest business day)
$dates = foreach ($r in $rows) { try { [datetime]$r.Date } catch { $null } }
$maxDate = ($dates | Where-Object { $_ } | Measure-Object -Maximum).Maximum
if ($maxDate -lt (Get-Date).AddDays(-10)) {
  Write-Error "ECB data looks stale (max date: $($maxDate.ToString('yyyy-MM-dd'))). Aborting."
  exit 1
}

# 7) Keep only rows with USD and normalize → date, usd_per_eur
$clean = $rows |
  Where-Object { $_.USD -and $_.USD.Trim() -ne "" } |
  Select-Object @{Name='date';Expression={ (Get-Date $_.Date).ToString('yyyy-MM-dd') }},
                @{Name='usd_per_eur';Expression={ $_.USD }} |
  Sort-Object date

# 8) Export (prefer no BOM; fallback to UTF8 with BOM is fine—server strips BOM)
try { $clean | Export-Csv -NoTypeInformation -Encoding utf8NoBOM -Path $out }
catch { $clean | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $out }

# 9) Upload to FastAPI
curl.exe -s -F "file=@$out;type=text/csv" http://127.0.0.1:8000/fx/upload

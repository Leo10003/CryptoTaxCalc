# run_smoke_wrapper.ps1 (hardened)
$ErrorActionPreference = 'Stop'

# ABSOLUTE paths (no reliance on working dir)
$ProjectRoot = 'C:\Users\picci\Desktop\CryptoTaxCalc'
$Automation  = Join-Path $ProjectRoot 'automation'
$LogsDir     = Join-Path $ProjectRoot 'automation\logs'
$VenvPython  = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$PyScript    = Join-Path $Automation 'run_smoke_and_email.py'
$WrapperLog  = Join-Path $LogsDir "task_wrapper.log"
$EnvFile     = Join-Path $ProjectRoot '.env'

# Make sure logs dir exists
if (-not (Test-Path $LogsDir)) { New-Item -ItemType Directory -Path $LogsDir | Out-Null }

# Start log with time stamp
$ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
"[$ts] wrapper start" | Out-File -FilePath $WrapperLog -Encoding UTF8

# Sanity checks
foreach ($p in @($ProjectRoot, $Automation, $VenvPython, $PyScript)) {
  if (-not (Test-Path $p)) {
    $ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
    "[$ts] MISSING: $p" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8
    exit 2
  }
}

# (Optional) surface .env existence for troubleshooting
if (Test-Path $EnvFile) {
  $ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
  "[$ts] .env found" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8
} else {
  $ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
  "[$ts] .env NOT found (Python script loads it internally if needed)" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8
}

# Run the python smoke runner
$ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
"[$ts] launching python: `"$VenvPython`" `"$PyScript`"" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $VenvPython
$psi.Arguments = "`"$PyScript`""
$psi.WorkingDirectory = $ProjectRoot
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true

$p = New-Object System.Diagnostics.Process
$p.StartInfo = $psi
$null = $p.Start()

# Pipe output to log
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()

$ts = (Get-Date).ToUniversalTime().ToString("s") + "Z"
"[$ts] python exit=$($p.ExitCode)" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8
if ($stdout) { "STDOUT:`r`n$stdout" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8 }
if ($stderr) { "STDERR:`r`n$stderr" | Out-File -FilePath $WrapperLog -Append -Encoding UTF8 }

exit $p.ExitCode

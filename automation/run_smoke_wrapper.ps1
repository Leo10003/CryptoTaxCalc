# automation/run_smoke_wrapper.ps1
# Runs the Python smoke runner and makes sure .env variables are injected into the child process.
# Writes a log to automation/task_wrapper.log that Task Scheduler can show in "Last Run Result".

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Log([string]$msg) {
    $ts = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    Add-Content -LiteralPath $Global:LogPath -Value "[$ts] $msg" -Encoding UTF8
}

# --- paths --------------------------------------------------------------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Resolve-Path (Join-Path $ScriptDir '..')
$Global:LogPath = Join-Path $ScriptDir 'logs\task_wrapper.log'
$EnvFile   = Join-Path $RepoRoot '.env'
$PyExe     = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Runner    = Join-Path $ScriptDir 'run_smoke_and_email.py'

# --- start --------------------------------------------------------------
New-Item -ItemType File -Path $Global:LogPath -Force | Out-Null
Write-Log 'wrapper start'

# --- sanity -------------------------------------------------------------
if (-not (Test-Path -LiteralPath $Runner)) {
    Write-Log "ERROR: runner not found at $Runner"
    exit 2
}

# --- load .env into *process* environment for child Python -------------
if (Test-Path -LiteralPath $EnvFile) {
    Write-Log '.env found'
    # Read UTF-8 (no BOM) just in case
    foreach ($line in Get-Content -LiteralPath $EnvFile -Encoding UTF8) {
        if ($line -match '^\s*#') { continue }
        if ($line -match '^\s*$') { continue }
        if ($line -match '^\s*([^#=]+?)\s*=\s*(.*)$') {
            $name = $matches[1].Trim()
            $val  = $matches[2].Trim()

            # strip surrounding quotes if present
            if ($val.StartsWith('"') -and $val.EndsWith('"')) {
                $val = $val.Trim('"')
            } elseif ($val.StartsWith("'") -and $val.EndsWith("'")) {
                $val = $val.Trim("'")
            }

            # Make sure child process sees it:
            [System.Environment]::SetEnvironmentVariable($name, $val, 'Process')
        }
    }
} else {
    Write-Log '.env NOT found'
}

# quick telemetry for troubleshooting
$tok  = $env:TELEGRAM_BOT_TOKEN; if (-not $tok) { $tok = $env:TELEGRAM_TOKEN }
$chat = $env:TELEGRAM_CHAT_ID;   if (-not $chat) { $chat = $env:TELEGRAM_CHATID }
if ($tok -and $chat) {
    Write-Log "Telegram env looks set (token len=$($tok.Length), chat=$chat)"
} else {
    Write-Log "Telegram missing (token? $([bool]$tok), chat? $([bool]$chat))"
}

# --- choose python ------------------------------------------------------
if (-not (Test-Path -LiteralPath $PyExe)) { $PyExe = 'python' }

Write-Log "launching python: `"$PyExe`" `"$Runner`""

# --- run python and capture output -------------------------------------
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName               = $PyExe
$psi.Arguments              = "`"$Runner`""
$psi.WorkingDirectory       = $RepoRoot
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true
$psi.UseShellExecute        = $false
$psi.CreateNoWindow         = $true

$p = [System.Diagnostics.Process]::Start($psi)
$stdout = $p.StandardOutput.ReadToEnd()
$stderr = $p.StandardError.ReadToEnd()
$p.WaitForExit()
$exitCode = $p.ExitCode

Write-Log "python exit=$exitCode"
if ($stdout) { Write-Log "STDOUT:`n$stdout" }
if ($stderr) { Write-Log "STDERR:`n$stderr" }

exit $exitCode

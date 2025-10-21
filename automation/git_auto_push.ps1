param(
  [string]$RepoPath = "C:\Users\picci\Desktop\CryptoTaxCalc",
  [string]$Branch = "main",
  [string]$Remote = "origin"
)

$ErrorActionPreference = "Stop"

# 1) Go to repo
Set-Location $RepoPath

# 2) Ensure we are on the right branch
try {
  git rev-parse --is-inside-work-tree | Out-Null
} catch {
  Write-Host "Not a git repo at $RepoPath" -ForegroundColor Red
  exit 1
}

# 3) Pull latest (avoid diverging histories)
git fetch $Remote $Branch
# Try fast-forward if possible (ignore if nothing to update)
git merge --ff-only "$Remote/$Branch" 2>$null | Out-Null

# 4) Check for changes
$changes = git status --porcelain
if ([string]::IsNullOrWhiteSpace($changes)) {
  Write-Host "No changes to commit."
  exit 0
}

# 5) Add, commit, push
git add -A
$stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ssK")
git commit -m "Auto-sync: $stamp"
git push $Remote $Branch

Write-Host "Auto-push complete at $stamp."

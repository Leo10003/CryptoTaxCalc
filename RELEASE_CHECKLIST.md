# RELEASE_CHECKLIST.md

This document is the single, end-to-end procedure for producing a release of CryptoTaxCalc.
It is written for the workflow where:
- GitHub is used as code storage/backup (especially when switching PCs).
- Releases are created by pushing semver tags (e.g., v1.0.2).
- The deployment artifact is a workflow-generated bundle zip that MUST include at repo root:
  - src/
  - templates/
  - static/

================================================================================
0) Definitions (know what you are shipping)
================================================================================

There are two kinds of artifacts you may see on GitHub:

A) Source archive (GitHub-generated)
   Examples:
   - CryptoTaxCalc-1.0.2.zip
   - CryptoTaxCalc-1.0.2.tar.gz
   Purpose:
   - Full repo snapshot for development continuity (best for moving to another PC).

B) Bundle zip (workflow-generated)
   Example:
   - CryptoTaxCalc_v1.0.2_bundle.zip (+ .sha256)
   Purpose:
   - Deployment/runtime package.
   Must contain at repo root:
   - src/
   - templates/
   - static/

================================================================================
1) Preconditions (before you even think about tagging)
================================================================================

1.1 Ensure you are on the correct branch/commit to release
- If your canonical code is on a backup branch (recommended for “GitHub as storage”), that is fine.
- The tag MUST be created on the commit you intend to ship.

Check current branch + commit:
~~~bash
git status -sb
git rev-parse --short HEAD
git show --name-only --oneline -1
~~~

1.2 Ensure the release workflow exists in THIS commit
The workflow only runs if it exists in the tagged commit.

Confirm file exists:
- .github/workflows/release.yml

Confirm it is included in the current commit view:
~~~bash
git show --name-only --oneline -1 | cat
~~~

You should see:
- .github/workflows/release.yml

1.3 Ensure root assets exist (paths must remain at repo root)
Confirm these directories exist at repo root:
- templates/
- static/
- src/

~~~bash
ls -la
~~~
(Or on Windows PowerShell)
~~~powershell
Get-ChildItem -Force
~~~

================================================================================
2) Run the quality gate (tests)
================================================================================

2.1 Run all tests excluding smoke
~~~bash
pytest -q -m "not smoke"
~~~

2.2 Run smoke tests
~~~bash
pytest -q -m smoke --maxfail=1
~~~

If either fails: stop. Fix before releasing.

================================================================================
3) Hygiene gate (code-only storage; prevent leaks/bloat)
================================================================================

This repo must not ship or track local/runtime/sensitive data:
- .env and variants
- storage_raw/
- backups/
- artifacts/
- dist/, build/
- *.egg-info/
- cryptotaxcalc.db and WAL/SHM

3.1 Confirm these are NOT tracked
~~~bash
git ls-files storage_raw backups artifacts dist build | cat
~~~
Expected output: nothing.

3.2 Confirm your working tree is not accidentally staging runtime data
~~~bash
git status
~~~

If you see DB/storage artifacts staged, fix before continuing:
(Example untrack commands)
~~~bash
git rm -r --cached storage_raw 2>/dev/null || true
git rm -r --cached backups 2>/dev/null || true
git rm -r --cached artifacts 2>/dev/null || true
git rm -r --cached dist build 2>/dev/null || true
git rm -r --cached "*.egg-info" 2>/dev/null || true
git rm --cached cryptotaxcalc.db 2>/dev/null || true
~~~

3.3 Confirm .gitignore is present and includes runtime exclusions
~~~bash
cat .gitignore
~~~

================================================================================
4) Choose the version tag (semver) safely
================================================================================

4.1 List existing tags
~~~bash
git tag -l | sort -V
~~~

4.2 Choose a new version
Rule:
- Do NOT reuse an existing tag name.
- Prefer a new tag (vX.Y.Z) rather than rewriting an old one.

If the tag already exists remotely, pick the next version (e.g., v1.0.3).

================================================================================
5) Tag and push (this triggers the release workflow)
================================================================================

5.1 Create the tag locally
~~~bash
git tag vX.Y.Z
~~~

5.2 Push the tag
~~~bash
git push origin vX.Y.Z
~~~

If Git says the tag already exists on remote:
- Do NOT force it.
- Choose a new tag version and push that instead.

================================================================================
6) Verify GitHub Actions ran and succeeded
================================================================================

6.1 On GitHub: go to Actions and find the run triggered by your tag.
- The release workflow must be green.

6.2 If the workflow did NOT run:
Common causes:
- The tag points to a commit that does not contain .github/workflows/release.yml
- You tagged the wrong branch/commit
- The workflow trigger patterns do not match your tag name

Fix by tagging a commit that includes the workflow.

================================================================================
7) Verify Release assets (bundle zip correctness)
================================================================================

7.1 Download from GitHub Releases:
- CryptoTaxCalc_vX.Y.Z_bundle.zip
- CryptoTaxCalc_vX.Y.Z_bundle.zip.sha256

7.2 Verify checksum (Linux/macOS)
~~~bash
sha256sum -c CryptoTaxCalc_vX.Y.Z_bundle.zip.sha256
~~~

7.3 Verify checksum (Windows PowerShell)
~~~powershell
Get-FileHash .\CryptoTaxCalc_vX.Y.Z_bundle.zip -Algorithm SHA256
Get-Content .\CryptoTaxCalc_vX.Y.Z_bundle.zip.sha256
~~~
The hash must match.

7.4 Verify bundle contents include root folders
Linux/macOS:
~~~bash
unzip -l CryptoTaxCalc_vX.Y.Z_bundle.zip | head -n 50
~~~

Windows PowerShell (quick extract check):
~~~powershell
$dest = ".\_bundle_check"
Remove-Item -Recurse -Force $dest -ErrorAction SilentlyContinue
Expand-Archive -Force ".\CryptoTaxCalc_vX.Y.Z_bundle.zip" $dest
Get-ChildItem $dest -Force
~~~

You MUST see:
- $dest\src\
- $dest\templates\
- $dest\static\

If any are missing: stop. Fix workflow/bundle and re-release with a new tag.

================================================================================
8) Confirm “Latest” release behavior (avoid user confusion)
================================================================================

Your release workflow should ensure:
- Only the highest semver tag is marked as “Latest”.

If “Latest” looks wrong in the GitHub UI:
- Create and push a NEW higher tag on the correct commit (recommended).
- Avoid rewriting tags (tag immutability preserves trust).

================================================================================
9) Post-release housekeeping
================================================================================

9.1 Create a code-only backup branch (recommended)
This matches the “GitHub as storage” philosophy without touching main.

~~~powershell
git switch -c backup/$(Get-Date -Format "yyyy-MM-dd")_post_release
git add .github src tests templates static automation tools docs `
  pyproject.toml requirements.txt README.md COLLAB_RULES.md pytest.ini .gitignore
git commit -m "Backup snapshot $(Get-Date -Format "yyyy-MM-dd HH:mm")" --no-verify
git push -u origin HEAD
~~~

9.2 Document the release notes (optional but recommended)
- Add a short changelog entry in README or a CHANGELOG.md.

================================================================================
10) Troubleshooting (quick fixes, non-destructive)
================================================================================

10.1 If Git is “stuck” (index.lock)
~~~powershell
Get-Process git -ErrorAction SilentlyContinue | Stop-Process -Force
Remove-Item -Force .git\index.lock -ErrorAction SilentlyContinue
~~~

10.2 If Git says “cannot switch branch while rebasing”
Use rebase quit (non-destructive):
~~~bash
git rebase --quit
~~~

10.3 PowerShell stash references
Always quote stash references:
- "stash@{0}"
Not:
- stash@{0}

================================================================================
END
================================================================================

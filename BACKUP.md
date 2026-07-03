# Backup workflow (GitHub as storage)

This project treats GitHub as a code-only backup so you can switch PCs or recover from local corruption.

## Golden rules
- Do not push local databases, WAL/SHM files, `storage_raw/`, or build outputs.
- Do not use `rebase --abort`, `reset --hard`, or `clean -fd` when you have uncommitted work.
- Prefer “backup branches” over rewriting `main`.

## Fast backup (PowerShell)
From repo root:

```powershell
# Create a dedicated backup branch (name it by date)
git switch -c backup/$(Get-Date -Format "yyyy-MM-dd")_local

# Stage only code + assets (explicit paths; avoids accidental sensitive data)
git add .github src tests templates static automation tools docs `
  pyproject.toml requirements.txt README.md COLLAB_RULES.md pytest.ini .gitignore

# Commit (skip hooks if needed)
git commit -m "Backup snapshot $(Get-Date -Format "yyyy-MM-dd HH:mm")" --no-verify

# Push backup branch (does NOT touch main)
git push -u origin HEAD

## PowerShell safety note
When referencing stashes in PowerShell, always quote the ref, e.g. `"stash@{0}"`, otherwise `{}` can be mis-parsed.


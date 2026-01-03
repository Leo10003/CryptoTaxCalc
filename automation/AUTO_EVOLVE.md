# AUTO_EVOLVE.md — Support Bundle Evolution Policy

## Goal
Ensure every new failure or anomaly automatically leads to better diagnostics on the next run — without requiring a full redesign.

## Policy Summary
Whenever a new class of failure appears (e.g., timeout, crash, missing artifact, data corruption), the `collect_support_bundle.py` script should be extended to produce a new, small artifact that captures its root cause automatically in future runs.

### Golden Rule
> **Every failure that isn't already self-explanatory must leave a breadcrumb that makes it self-explanatory next time.**

---

## Diagnostic Artifact Guidelines

| Category | Folder | File naming | Example |
|-----------|---------|-------------|----------|
| Environment & Runtime | `_meta/` | `runtime.json`, `env.txt` | Captures host and interpreter |
| DB Schema/Health | `_db/` | `missing_tables.txt`, `schema.json`, `preview_*.csv` | Summarize DB issues |
| API State | `_api/` | `api_diag_skipped.txt`, `GET_health.json` | Show API reachability |
| Rules | `_rules/` | `active_rules.json` | Record which rule modules loaded |
| Zipping | `_meta/zip_*` | `zip_error.txt`, `zip_truncated.txt` | Capture compression failures |
| Evolution Policy | `_meta/EVOLVE_RULE.json` | — | Declares the self-extension policy |

---

## Performance and Safety Rules

1. **Never block the API.**  
   All diagnostics must run within soft time limits:
   - `pip` diagnostics skipped in API mode  
   - file reads ≤ 2 MB  
   - per-step budget ≤ 2 seconds  

2. **No sensitive data.**  
   Mask or truncate values like:
   - wallet addresses, hashes, or private keys  
   - user email/PII  

3. **No critical side effects.**  
   Bundle generation must never modify DBs, repos, or runtime state.

---

## When to Add a New Artifact

You should extend the collector if you see:
- A failure not explained by existing artifacts.  
- A repeated timeout or hang.  
- Missing context about environment, DB, or source files.  
- Any bug that required manual reproduction steps to understand.

Each new artifact should be:
- ≤ 50 KB  
- JSON, TXT, or CSV  
- Placed under an existing `_meta`, `_db`, `_rules`, or `_api` folder.

---

## Example Workflow
1. A new error appears in `/admin/bundle` (`Script failed: Unknown database table`).
2. Developer inspects bundle → `_db/missing_tables.txt` empty → new helper `db_expected_schema()` added.
3. Next bundle includes `_db/expected_vs_actual.json` for automatic root-cause visibility.

---

## Maintenance

- **Reviewed monthly:** merge all ad-hoc debug helpers into stable functions.  
- **Versioned via** `_meta/EVOLVE_RULE.json`.  
- **Every commit adding diagnostics must mention:**  
  > “Extends collector for new failure: [type].”

---

_This document ensures the bundle collector grows with your system — quietly, safely, and predictably._

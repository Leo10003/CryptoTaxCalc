# COLLAB_RULES.md

A living contract for how we build and maintain **CryptoTaxCalc** together.  
Whenever a new rule is added, this file will be re-issued in full.

---

## 1) Don’t guess APIs or filenames
Only use functions, modules, env vars, and endpoints that exist in the repo or were explicitly introduced in this chat. If something’s missing, say so and propose options.

## 2) Match names exactly
Keep imports, module names, function names, table/column names, and route paths **exactly** consistent. If a rename is needed, provide a repo-wide diff plan.

## 3) No invented code
Never fabricate helper functions, settings, or constants. If a helper is needed, define it first (with filename and placement), then use it.

## 4) Prefer additive diffs
Show minimal, focused changes. Avoid large refactors unless asked. Provide before/after snippets when possible.

## 5) Validate endpoints
Every new/changed endpoint must include a quick curl/requests example and expected HTTP status codes.

## 6) Preserve contracts
Backwards-compatibility matters. Changing schemas, responses, or DB shape requires a clear migration + version bump + changelog note.

## 7) Explicit error handling
Return structured errors (status + code + message). Log the root cause server-side. Don’t leak secrets or stack traces to clients.

## 8) Deterministic hashing
Any audit or digest hash must be stable across runs. Specify inputs, serialization order, and normalization rules.

## 9) No hidden global state
Pass dependencies explicitly. If cached state is needed, document scope and invalidation.

## 10) Idempotent migrations
DB migrations must be safe to re-run. Include up/down or guards. Never destroy data without an opt-in flag.

## 11) SQLite pragmas & integrity
Use WAL mode for concurrency, foreign_keys=ON, and reasonable busy_timeout. Document these settings.

## 12) DB lock resilience
Wrap critical writes in retry w/ backoff. Surface a clean 409/423-style error if contention persists.

## 13) UTC everywhere
Store timestamps in UTC ISO-8601. Convert only at the UI/report layer.

## 14) Pagination by default
List endpoints accept `page` & `page_size` and return `{page, page_size, total, items}`.

## 15) Audit trails
Log calculation runs, inputs, outputs, and digests. Provide `/audit/run/{id}` to compare stored vs recomputed.

## 16) Rate-limit safety
External calls (FX, email, etc.) respect limits, timeouts, and retries with jitter. Cache where sensible.

## 17) Idempotent schedulers
Nightly tasks must be safe to re-run; no duplicate inserts. Use upsert/merge or unique constraints.

## 18) Cross-OS paths
Use `pathlib` and avoid hardcoded separators. Document Windows-specific steps when needed.

## 19) CLI = API parity
If it exists in the API, provide a thin CLI (or vice versa) that calls the same code paths.

## 20) Smoke tests
Maintain a fast `smoke_test.py` that hits `/health`, a read endpoint, and a write+read flow. Fails the run on any non-2xx.

## 21) Support bundles
Provide a script/endpoint to collect logs, DB, configs, and versions into a timestamped zip under `support_bundles/`.

## 22) Secrets management
No secrets in code or logs. Use `.env`, environment variables, or OS secrets. Document required keys.

## 23) Package structure conventions
`src/cryptotaxcalc/` houses importable code. Keep APIs in `api/`, core logic in `core/`, helpers in `utils/`, ORM in `models/`.

## 24) Versioning & changelog
Every user-visible change bumps version and updates `CHANGELOG.md` with brief notes and migration steps.

## 25) Contracts doc
Keep a `contracts.md` describing request/response schemas, DB tables, and invariants. Update when interfaces evolve.

## 26) File organization & structural clarity
Group by purpose, enforce single responsibility per file, maintain consistent names, avoid orphan files, and keep generated/temporary artifacts out of the repo root. Example:

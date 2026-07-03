# CryptoTaxCalc

CryptoTaxCalc is a **local-first crypto tax calculation platform** built on **FastAPI** with a modern, trust-oriented web UI.  
It is designed for two modes:

1. **Workspace (real-user flow):** import CSV exports → calculate → review → export (PDF/CSV/ZIP).
2. **Demo Mode (investor/offline showcase):** controlled dataset resets, dashboard KPIs, self-checks, and diagnostics export.

The guiding product principle is **trust through transparency**: every calculation is reproducible, every import is traceable, and the UI uses “guardrails” to prevent users from misreading partial/filtered results as filing-ready.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Core URLs (UI + API)](#core-urls-ui--api)
- [How the Product Works (Concepts)](#how-the-product-works-concepts)
- [Workspace: End-to-End User Flow](#workspace-end-to-end-user-flow)
- [CSV Support & Import](#csv-support--import)
- [FX Rates & Daily Prices](#fx-rates--daily-prices)
- [Calculation (v2) & Configuration](#calculation-v2--configuration)
- [History, Runs & Exports](#history-runs--exports)
- [Auditability & Verification](#auditability--verification)
- [Admin & Security Posture](#admin--security-posture)
- [Demo Mode (Investor Showcase)](#demo-mode-investor-showcase)
- [Diagnostics / Support Bundle](#diagnostics--support-bundle)
- [CI / Quality Gates](#ci--quality-gates)
- [Troubleshooting](#troubleshooting)
- [Legal / Compliance Notes](#legal--compliance-notes)

---

## Quick Start

### Requirements
- Python **3.11+** recommended.
- Works with SQLite by default (no external DB required).

### Create venv + install deps

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS / Linux:
source .venv/bin/activate

pip install -U pip
pip install -r requirements.txt
```

> If you use the packaging / dev extras defined in `pyproject.toml`, you can also do:
>
> ```bash
> pip install -e ".[dev]"
> ```

### Run the server

```bash
uvicorn cryptotaxcalc.app:app --reload --host 127.0.0.1 --port 8000
```

---

## Core URLs (UI + API)

### UI
- **Landing:** `http://127.0.0.1:8000/`
- **Workspace:** `http://127.0.0.1:8000/workspace`
- **Workspace Results:** `http://127.0.0.1:8000/workspace/results?run_id=<RUN_DB_ID>`
- **CSV Formats Catalog (human-friendly):** `http://127.0.0.1:8000/csv/formats`
- **Run History (HTML):** `http://127.0.0.1:8000/history?format=html`

### API Docs
- **OpenAPI / Swagger:** `http://127.0.0.1:8000/docs`

### Health & Status
- `GET /health`
- `GET /version`
- `GET /status`

---

## How the Product Works (Concepts)

### 1) Transactions vs. Raw Uploads (Traceability)
- **Raw uploads** are stored with SHA-256 and file metadata (helps debugging and supports compliance audits).
- **Transactions** are normalized rows used by the calculation engine.
- This separation is intentional: users can prove what they uploaded, and engineers can reproduce parsing outcomes.

### 2) Runs & Realized Events (Reproducibility)
- Each calculation produces a **run** stored in `calc_runs`.
- The engine produces **realized_events** (per disposal event) used for:
  - totals (gain, taxable gain, exempt gain),
  - charts,
  - exports (CSV/PDF),
  - subset filtering without re-running FIFO.

### 3) Trust Guardrails in UI (Psychology)
- The Workspace Results UI distinguishes between:
  - **Live totals** (fully recalculated for the current scope), and
  - **Preview/partial states** (cached or partially loaded views).
- This prevents the common user error: **treating partial/filtered data as filing-ready**.

---

## Workspace: End-to-End User Flow

### Step 1 — Upload & detect CSV formats
Recommended approach for multi-file imports:

- `POST /csv/detect/multiple`  
  Reads only headers (fast). Returns supported/unsupported detection results.

### Step 2 — Preview (optional)
- `POST /upload/csv`  
  Parses and returns a preview (no DB writes).  
  Preview reads are capped by `MAX_PREVIEW_BYTES` to avoid OOM.

### Step 3 — Import (recommended)
- `POST /import/multiple`  
  Upload multiple CSVs in one request. Stores raw file hashes + inserts normalized transactions.
- `POST /import/csv` (deprecated)  
  Wrapper around `/import/multiple` for single file.

### Step 4 — Calculate
- `POST /calculate/v2` (core calc endpoint)
- `POST /api/v1/runs` (API wrapper around `/calculate/v2`)

### Step 5 — Review results
- Open: `/workspace/results?run_id=<RUN_DB_ID>`

### Step 6 — Export
From the Results page, exports are generated from the stored run artifacts:
- PDF (full run): `/export/workspace_summary/<RUN_DB_ID>.pdf`
- PDF (subset scope): `/export/workspace_summary/<RUN_DB_ID>/subset.pdf?year=YYYY&asset=BTC&local_area=...`
- CSV (events): `/history/run/<RUN_DB_ID>/events.csv`
- ZIP (manifest bundle): `/history/<RUN_DB_ID>/download`

---

## CSV Support & Import

### Supported formats catalog (UI-friendly)
- `GET /csv/formats`  
  Shows the current catalog (headers, hints, match logic).

### Supported formats (API)
- `GET /csv/sources`  
  Returns supported format list for the wizard.
- `GET /csv/sources/catalog`  
  Returns match rules (headers + filename hints).

### Detect multiple CSVs quickly
- `POST /csv/detect/multiple`

**Behavior**
- Reads ~128KB sample per file.
- Detects delimiter + quotechar.
- Captures unknown structures into a triage store (so you can implement high-impact parsers).

### Import multiple files (recommended)
- `POST /import/multiple?reset=0|1`

**What it does**
- Stores original file (hash + path) as a “raw event” artifact.
- Parses using the parser registry (source-aware where possible).
- Inserts normalized transactions with duplicate detection.
- If `reset=true`, clears transactions first (useful for controlled demos).

### Preview upload (bounded)
- `POST /upload/csv`

**Why preview exists (psychology)**
- Users feel safer when they can confirm what the system understood before committing data.

---

## FX Rates & Daily Prices

### FX status (diagnostics)
- `GET /fx/status?base=USD&quote=EUR`

Returns:
- total rows in `fx_rates`,
- pair rows,
- latest date/rate for that pair.

### Upload FX (USD↔EUR) — Admin-only
- `POST /fx/upload`  
  Requires `X-Admin-Token` (see [Admin & Security Posture](#admin--security-posture)).

**CSV headers**
- `date` (YYYY-MM-DD)
- `usd_per_eur` (USD per 1 EUR)

Stored internally as:
- base=`USD`, quote=`EUR`, rate=`EUR per 1 USD`

### Upload daily prices (for third-asset fee valuation) — Admin-only
- `GET /prices/template.csv` (download template)
- `POST /prices/upload` (upload)

**CSV headers**
- `date` (YYYY-MM-DD)
- `base` (asset symbol, e.g. ETH, BNB)
- `quote` (EUR or USD-like: USD/USDT/USDC/BUSD)
- `rate` (quote per 1 base)

**Important**
- USD-like prices are converted to EUR using `fx_rates USD/EUR`.
- If you upload USD-like prices, make sure FX rates are available for those dates.

---

## Calculation (v2) & Configuration

### Main calculation endpoint
- `POST /calculate/v2`

This endpoint:
- clears FX cache for consistency,
- ensures tables exist,
- creates/uses an FX batch id,
- runs FIFO engine,
- stores summary + realized events + digests.

### Calculate request fields (important)
`CalculateV2Request` supports:

- `jurisdiction` (e.g., `HR`, `IT`)
- `rule_version` (string label, used in UI audit chips)
- `lot_method` (default: FIFO)
- `fx_source` (label stored in manifests)
- `holding_exemption_days` (affects exempt vs taxable)
- `it_threshold_eur` (Italy threshold behavior)
- `round_dp` (rounding control)
- `strict_fx` (reject missing FX instead of fallback)
- `include_tax_helpers`
- `include_audit_appendix`
- `tax_year`

### API wrapper
- `POST /api/v1/runs`
- `GET /api/v1/runs`
- `GET /api/v1/runs/{run_id}`
- `GET /api/v1/runs/{run_id}/tax`
- `GET /api/v1/runs/{run_id}/events?limit=100&offset=0&year=YYYY&asset=BTC`

---

## History, Runs & Exports

### History index (JSON or HTML)
- `GET /history?format=json` (default)
- `GET /history?format=html` (renders `history.html`)

### Download run bundle (ZIP)
- `GET /history/{run_id}/download`

Produces a zip containing at least:
- `manifest.json` (compact run manifest)

### Download realized events CSV
- `GET /history/run/{run_id}/events.csv`
- Also available as utility export:
  - `GET /export/events_csv?run_id=latest|<RUN_DB_ID>`

### Workspace PDF exports
- `GET /export/workspace_summary/{run_db_id}.pdf`
- `GET /export/workspace_summary/{run_db_id}/subset.pdf?year=YYYY&asset=BTC&local_area=...`

### Utility exports (legacy / quick ops)
- `GET /export/summary.pdf`
- `GET /export/summary.csv`
- `GET /export/calculate.pdf`
- `GET /export/calculate.csv`
- `GET /export/db` (Admin-only DB snapshot)

---

## Auditability & Verification

### Run digest + manifest (audit proof)
- `GET /audit/run/{run_id}`  
  Returns:
  - stored digests (input/output/manifest hashes),
  - recomputed digests,
  - whether they match,
  - the manifest.

### Lightweight verify
- `GET /audit/verify/{run_id}`  
  Returns boolean verification outcome + reason.

### Audit log stream
- `GET /audit/history?limit=50`

---

## Admin & Security Posture

CryptoTaxCalc defaults to a **secure-by-default posture** in production:

- Admin endpoints are **disabled by default in prod**.
- Admin endpoints are **localhost-only** unless explicitly enabled.
- Tokens are **header-first**; query-string tokens are discouraged (leak risk).

### Key environment variables (security)
- `CTC_ENV=development|production`
- `ENABLE_ADMIN_ENDPOINTS=1|0` (prod default: 0)
- `ENABLE_ADMIN_SCRIPTS=1|0` (off by default)
- `ADMIN_TOKEN=<strong-secret>` (must be set in prod)
- `BUNDLE_TOKEN=<optional-separate-token>` (allows bundling without using ADMIN_TOKEN)
- `ADMIN_HEADER_ONLY=1|0` (default: 1)
- `ADMIN_ALLOW_REMOTE=1|0` (default: 0; keep 0 in production unless behind a secure private network)
- `ALLOW_QUERY_TOKENS=1|0` (default: 0)
- `CTC_DISABLE_DOTENV=1` (recommended for packaged builds / prod services)

### Admin authentication headers
Prefer:
- `X-Admin-Token: <token>`  
or:
- `Authorization: Bearer <token>`

### Admin endpoints (selected)
- CSV triage:
  - `GET /admin/csv/unsupported`
  - `GET /admin/csv/unsupported/ui`
  - `POST /admin/csv/unsupported/remove`
- FX upload:
  - `POST /fx/upload`
- Prices upload:
  - `POST /prices/upload`
- Export bundle builder:
  - `POST /export/bundle`
- Support bundle builder:
  - `POST /admin/bundle` (requires admin scripts enabled)
- Maintenance:
  - `POST /maintenance/prune_fx`
  - `POST /maintenance/vacuum`

### Why header-only matters (psychology + ops)
Keeping secrets out of URLs prevents accidental leakage via:
- browser history,
- reverse proxy logs,
- screenshots during support calls.

This reduces incident risk and increases operator confidence.

---

## Demo Mode (Investor Showcase)

Demo routes are mounted only when Demo Mode is enabled in your build (see `demo_assets.is_demo_mode_enabled()`).

### Demo dashboard
- `GET /demo/dashboard`

### Demo health & build metadata
- `GET /demo/build_info`
- `GET /demo/self_check`
- `GET /demo/logo`

### Demo diagnostics export
- `GET /demo/diagnostics/export`  
Downloads a diagnostics zip (logs, assets, manifests where available).

### Demo reset
- `POST /demo/reset`  
Resets the demo database/seeds (only works when demo mode is enabled).

### Demo manifest and recent runs (dashboard helpers)
- `GET /demo/manifest`
- `GET /demo/runs/recent?limit=6`
- `POST /demo/load`  
Loads demo dataset without deleting run history (prod hardening prevents public resets unless explicitly allowed).

---

## Diagnostics / Support Bundle

### CLI support bundle
Creates a full “resume work anywhere” archive:

```bash
python -m cryptotaxcalc.exporter --support
```

### API support bundle (admin scripts)
- `POST /admin/bundle`  
Requires:
- `ENABLE_ADMIN_ENDPOINTS=1`
- `ENABLE_ADMIN_SCRIPTS=1`
- valid `ADMIN_TOKEN` or `BUNDLE_TOKEN`

---

## CI / Quality Gates

The repo includes CI steps focused on:
- **secret scanning** (gitleaks),
- linting / formatting,
- tests.

Recommended local commands:

```bash
pytest -q
pytest -q -m smoke
```

---

## Troubleshooting

### “FX setup failed” / missing rates
- Check `GET /fx/status?base=USD&quote=EUR`
- Upload FX via `POST /fx/upload` (admin)
- If using USD-like daily prices, upload FX first.

### CSV not recognized
- Visit `/csv/formats`
- Use `/csv/detect/multiple`
- If unsupported, capture appears in `/admin/csv/unsupported/ui` (admin)

### Large CSV preview fails
- `/upload/csv` is capped by `MAX_PREVIEW_BYTES`.
- Use `/import/multiple` for large files (stores raw upload + streams parsing).

---

## Legal / Compliance Notes

- **Informational only — not tax advice.**
- Local-first by default: no cloud sync unless you build it.
- Demo mode uses controlled seeds; do not use demo seeds for real filing.

---

# app.py
"""
Main FastAPI application.

This file wires together:
- the web server (FastAPI + Uvicorn)
- our CSV parsing service
- the database session and models
- clean, documented endpoints

Endpoints kept for continuity:
  GET  /health           → liveness check
  GET  /version          → app version metadata
  POST /upload/csv       → parse CSV and PREVIEW (no DB writes)
  POST /import/csv       → parse CSV and SAVE to DB
  GET  /transactions     → list recent saved transactions

  Command to start the server: uvicorn app:app --reload
"""

import os, shutil, tempfile, csv, io, json, csv as _csv, sys, glob, time, subprocess
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Response, Header, Request
from .schemas import CSVPreviewResponse, ImportCSVResponse, Transaction
from .db import SessionLocal, engine
from .models import Base, TransactionRow, FxRate
from decimal import Decimal, InvalidOperation
from sqlalchemy.exc import IntegrityError
from datetime import datetime, date, date as _date, timezone
from csv import DictReader
from io import StringIO, BytesIO
from sqlalchemy import text, and_, func
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from .utils_files import persist_uploaded_file
from datetime import datetime as _dt
from .csv_normalizer import parse_csv
from .fifo_engine import compute_fifo
from .fx_utils import usd_to_eur, get_rate_for_date, get_or_create_current_fx_batch_id
from .audit_digest import build_run_manifest, compute_digests
from .audit_utils import audit
from .utils_files import persist_uploaded_file
from pathlib import Path
from .__about__ import __title__, __version__

# ReportLab (pure-Python PDF generation)
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

import hashlib  # built-in Python library for secure hashes
from .db import init_db
init_db()

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Compute project root:  .../CryptoTaxCalc
# (app.py lives in .../CryptoTaxCalc/src/cryptotaxcalc/app.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../CryptoTaxCalc
AUTOMATION = PROJECT_ROOT / "automation"
GIT_SCRIPT = AUTOMATION / "git_auto_push.ps1"
LOG_DIR = AUTOMATION / "logs"

# Load .env from the project root (optional but recommended)
try:
    from dotenv import load_dotenv  # pip install python-dotenv (already common in your setup)
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    # Safe to ignore if python-dotenv isn't installed; ENV variables still work.
    pass

# Admin token used by /admin/git-sync endpoint
# Prefer to set this in .env (see below). The default is only for dev!
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "dev-token-change-me")

# Load .env early so BUNDLE_TOKEN is available
try:
    from dotenv import load_dotenv
    # project_root/.env  (src/cryptotaxcalc/app.py -> parents[2] == project root)
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
except Exception:
    # If python-dotenv isn't installed or .env missing, we just skip;
    # endpoint will detect missing token and return 500 with a safe message.
    pass

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def compute_tx_hash(tx: Transaction) -> str:
    """
    Compute a deterministic SHA-256 hash for a transaction based on key fields.
    This allows us to detect duplicates across imports even if the same CSV
    is uploaded again.

    We use SHA-256 because it’s collision-resistant and available in Python’s
    standard library (no extra dependency).

    The string we hash is built from a few important fields joined together with '|'.
    Even tiny differences (e.g., fee=1 vs fee=1.0) create a completely different hash.
    """
    # Build a reproducible text line from transaction fields
    base_string = (
        f"{tx.timestamp.isoformat()}|"
        f"{tx.type}|"
        f"{tx.base_asset}|"
        f"{tx.base_amount}|"
        f"{tx.quote_asset}|"
        f"{tx.quote_amount}|"
        f"{tx.fee_asset}|"
        f"{tx.fee_amount}|"
        f"{tx.exchange}|"
        f"{tx.memo}"
        f"{tx.fair_value}"
    )
    # Compute the hash value and return it as a 64-character hexadecimal string
    return hashlib.sha256(base_string.encode("utf-8")).hexdigest()

def dec_to_str(x: Decimal) -> str:
    try:
        q = x.quantize(Decimal("0.00000001"))  # 8 decimal places
    except Exception:
        return "0"
    s = format(q, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"

def _is_sqlite_file(path: str) -> bool:
    """Check by magic header."""
    try:
        with open(path, "rb") as f:
            header = f.read(16)
        return header.startswith(b"SQLite format 3\x00")
    except Exception:
        return False

def _integrity_ok(db_path: str) -> bool:
    """Run SQLite PRAGMA integrity_check on the given file path."""
    try:
        # Use a temporary engine bound to the file we want to check
        from sqlalchemy import create_engine
        tmp_engine = create_engine(f"sqlite:///{db_path}")
        with tmp_engine.connect() as conn:
            res = conn.execute(text("PRAGMA integrity_check;")).scalar()
        tmp_engine.dispose()
        return res == "ok"
    except Exception:
        return False

def _build_summary_pdf(
    title: str,
    year: int | None,
    by_quote: dict[str, dict[str, Decimal]],
    totals: dict[str, Decimal],
    eur_totals: dict[str, Decimal],
    top_events: list,  # list of realized event objects
) -> bytes:
    """
    Builds a compact PDF report with:
      - Title + optional year
      - By-quote summary table
      - Totals (native quote) and totals in EUR
      - Optional 'Top events' preview to sanity-check numbers

    Returns raw PDF bytes.
    """
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36
    )
    styles = getSampleStyleSheet()
    elements = []

    # ADD THIS helper (wraps cell text so it won’t overlap)
    def P(txt: str):
        # basic HTML escaping so Paragraph does not choke
        txt = (txt or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
        return Paragraph(txt, styles["Normal"])
    
    # ---- Title ----
    cap = f"{title}"
    if year is not None:
        cap += f" – {year}"
    elements.append(Paragraph(cap, styles["Title"]))
    elements.append(Spacer(1, 10))

    # ---- By-Quote Summary Table ----
    data_bq = [["Quote", "Proceeds", "Cost Basis", "Gain"]]
    for q, agg in by_quote.items():
        data_bq.append([
            (q or "(none)"),
            dec_to_str(agg["proceeds"]),
            dec_to_str(agg["cost_basis"]),
            dec_to_str(agg["gain"]),
        ])

    if len(data_bq) == 1:
        data_bq.append(["(no data)", "0", "0", "0"])

    tbl_bq = Table(data_bq, hAlign="LEFT")
    tbl_bq.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.black),
        ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
        ("ALIGN", (0,0), (0,-1), "LEFT"),
        ("BOTTOMPADDING", (0,0), (-1,0), 6),
        ("TOPPADDING", (0,0), (-1,0), 6),
    ]))
    elements.append(Paragraph("Riepilogo per Valuta di Controvalore / By Quote Asset", styles["Heading2"]))
    elements.append(tbl_bq)
    elements.append(Spacer(1, 12))

    # ---- Totals (native quote) ----
    data_tot = [["Section", "Proceeds", "Cost Basis", "Gain"]]
    data_tot.append([
        "Totals (native quotes)",
        dec_to_str(totals["proceeds"]),
        dec_to_str(totals["cost_basis"]),
        dec_to_str(totals["gain"])
    ])
    tbl_tot = Table(data_tot, hAlign="LEFT")
    tbl_tot.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
        ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
    ]))
    elements.append(tbl_tot)
    elements.append(Spacer(1, 8))

    # ---- EUR Totals ----
    data_eur = [["Section", "Proceeds (EUR)", "Cost Basis (EUR)", "Gain (EUR)"]]
    data_eur.append([
        "Totals (converted to EUR)",
        dec_to_str(eur_totals["proceeds"]),
        dec_to_str(eur_totals["cost_basis"]),
        dec_to_str(eur_totals["gain"])
    ])
    tbl_eur = Table(data_eur, hAlign="LEFT")
    tbl_eur.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
        ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("ALIGN", (1,1), (-1,-1), "RIGHT"),
    ]))
    elements.append(tbl_eur)
    elements.append(Spacer(1, 14))

    # ---- Top Events (preview) ----
    if top_events:
        elements.append(Paragraph("Anteprima Eventi Realizzati / Realized Events Preview", styles["Heading2"]))
        ev_head = ["Timestamp", "Asset", "Qty Sold", "Proceeds", "Cost Basis", "Gain", "Quote", "Fee"]
        data_ev = [ [P(h) for h in ev_head] ]

        for ev in top_events[:20]:  # keep list short for readability
            # compact, safe timestamp
            ts_compact = fmt_ts_display(ev.timestamp)

            data_ev.append([
                P(ts_compact),
                P(ev.asset or ""),
                P(dec_to_str(ev.qty_sold)),
                P(dec_to_str(ev.proceeds)),
                P(dec_to_str(ev.cost_basis)),
                P(dec_to_str(ev.gain)),
                P(ev.quote_asset or ""),
                P(dec_to_str(ev.fee_applied)),
            ])

        # Wider, safer column sizes to prevent collisions
        tbl_ev = Table(
            data_ev,
            hAlign="LEFT",
            colWidths=[90, 55, 55, 65, 65, 65, 45, 45]
        )
        tbl_ev.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#F0F0F0")),
            ("GRID", (0,0), (-1,-1), 0.25, colors.lightgrey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE", (0,1), (-1,-1), 9),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("ALIGN", (2,1), (5,-1), "RIGHT"),  # numeric columns aligned right
            ("LEFTPADDING", (0,0), (-1,-1), 4),
            ("RIGHTPADDING", (0,0), (-1,-1), 4),
            ("TOPPADDING", (0,0), (-1,-1), 2),
            ("BOTTOMPADDING", (0,0), (-1,-1), 2),
        ]))
        elements.append(tbl_ev)


    doc.build(elements)
    return buf.getvalue()

def fmt_ts_display(ts_iso: str) -> str:
    """
    Convert an ISO timestamp (e.g., '2024-03-15T08:45:00Z' or without 'Z')
    into 'YYYY-MM-DD HH:MM' for compact human display.
    """
    s = ts_iso.replace("Z", "")
    try:
        dt = _dt.fromisoformat(s)
    except Exception:
        return ts_iso  # fallback if unexpected format
    return dt.strftime("%Y-%m-%d %H:%M")

def _set_sqlite_pragmas():
    # Safer concurrency defaults for SQLite
    with engine.connect() as conn:
        # Write-Ahead Log: allows readers while one writer is active
        conn.execute(text("PRAGMA journal_mode=WAL;"))
        # Reasonable durability/perf for app usage
        conn.execute(text("PRAGMA synchronous=NORMAL;"))
        # How long SQLite should wait if the DB is busy (ms)
        conn.execute(text("PRAGMA busy_timeout=5000;"))

def _try_import():
    # try flat layout first
    try:
        from csv_normalizer import parse_csv  # noqa:F401
        from fifo_engine import compute_fifo  # noqa:F401
        from fx_utils import usd_to_eur, get_rate_for_date, get_or_create_current_fx_batch_id  # noqa:F401
        from .audit_digest import build_run_manifest, compute_digests  # noqa:F401
        from audit_utils import audit  # noqa:F401
        from utils_files import persist_uploaded_file  # noqa:F401
        return {
            "parse_csv": parse_csv,
            "compute_fifo": compute_fifo,
            "usd_to_eur": usd_to_eur,
            "get_rate_for_date": get_rate_for_date,
            "get_or_create_current_fx_batch_id": get_or_create_current_fx_batch_id,
            "build_run_manifest": build_run_manifest,
            "compute_digests": compute_digests,
            "audit": audit,
            "persist_uploaded_file": persist_uploaded_file,
        }
    except ImportError:
        # try package layout (app_core/*)
        from .csv_normalizer import parse_csv  # type: ignore
        from .fifo_engine import compute_fifo  # type: ignore
        from .fx_utils import usd_to_eur, get_rate_for_date, get_or_create_current_fx_batch_id  # type: ignore
        from .audit_digest import build_run_manifest, compute_digests  # type: ignore
        from .audit_utils import audit  # type: ignore
        from .utils_files import persist_uploaded_file  # type: ignore
        return {
            "parse_csv": parse_csv,
            "compute_fifo": compute_fifo,
            "usd_to_eur": usd_to_eur,
            "get_rate_for_date": get_rate_for_date,
            "get_or_create_current_fx_batch_id": get_or_create_current_fx_batch_id,
            "build_run_manifest": build_run_manifest,
            "compute_digests": compute_digests,
            "audit": audit,
            "persist_uploaded_file": persist_uploaded_file,
        }

def _d0() -> Decimal:
    """Return Decimal zero."""
    return Decimal("0")

def _as_str(d: Decimal | int | float | None) -> str:
    """Serialize numeric values safely as strings (for JSON)."""
    if d is None:
        return "0"
    if isinstance(d, Decimal):
        return format(d, 'f')
    try:
        return str(Decimal(str(d)))
    except Exception:
        return "0"

# --- helpers for summary endpoint ---

def _d0() -> Decimal:
    """Return Decimal zero."""
    return Decimal("0")

def _as_str(x) -> str:
    """Safe stringify numerics (Decimal/int/float/None) for JSON."""
    if x is None:
        return "0"
    if isinstance(x, Decimal):
        return format(x, "f")
    try:
        return str(Decimal(str(x)))
    except Exception:
        return "0"

def _parse_iso_ts(ts: str) -> datetime | None:
    """
    Robust timestamp parser:
    - Accepts 'YYYY-MM-DDTHH:MM:SS' and '...Z'
    - Returns naive datetime in local Python process (fine for grouping by year/month)
    """
    if not ts:
        return None
    t = ts.strip()
    if t.endswith("Z"):
        t = t[:-1]  # drop trailing Z to satisfy fromisoformat
    try:
        return datetime.fromisoformat(t)
    except Exception:
        # last resort: try only the date part "YYYY-MM-DD"
        try:
            return datetime(int(t[:4]), int(t[5:7]), int(t[8:10]))
        except Exception:
            return None

_locals = _try_import()
parse_csv = _locals["parse_csv"]
compute_fifo = _locals["compute_fifo"]
usd_to_eur = _locals["usd_to_eur"]
get_rate_for_date = _locals["get_rate_for_date"]
get_or_create_current_fx_batch_id = _locals["get_or_create_current_fx_batch_id"]
build_run_manifest = _locals["build_run_manifest"]
compute_digests = _locals["compute_digests"]
audit = _locals["audit"]
persist_uploaded_file = _locals["persist_uploaded_file"]
# ---- end robust imports ----

# --- Support-bundle helpers ---------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUPPORT_BUNDLES_DIR = PROJECT_ROOT / "support_bundles"
BUNDLE_SCRIPT = os.getenv("AUTOMATION_BUNDLE_PS", r"automation\collect_support_bundle.ps1")
BUNDLE_TOKEN = os.getenv("BUNDLE_TOKEN", "")

def _abs_script_path() -> str:
    p = Path(BUNDLE_SCRIPT)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return str(p.resolve())

def _latest_zip_path() -> str | None:
    zips = list((SUPPORT_BUNDLES_DIR).glob("support_bundle_*.zip"))
    if not zips:
        return None
    zips.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(zips[0])

def run_git_auto_push():
    script_path = PROJECT_ROOT / "automation" / "git_auto_push.ps1"
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    # Run PowerShell with encoding-safe output
    process = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script_path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(PROJECT_ROOT)
    )
    return {
        "return_code": process.returncode,
        "stdout": process.stdout,
        "stderr": process.stderr,
        "log_file": str((PROJECT_ROOT / "automation" / "logs" / f"git_auto_push_{__import__('datetime').datetime.now().strftime('%Y-%m-%d')}.log"))
    }

def _latest_log():
    if not LOG_DIR.exists():
        return None
    files = sorted(LOG_DIR.glob("git_auto_push_*.log"))
    return files[-1] if files else None

# -----------------------------------------------------------------------------
# Application factory & startup
# -----------------------------------------------------------------------------
app = FastAPI(
    title=__title__,
    version=__version__,
    description="Backend API for parsing crypto transactions and storing them safely.",
)

def _ensure_transactions_has_fair_value_column():
    with engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(transactions)")).fetchall()
        names = [c[1] for c in cols]
        if "fair_value" not in names:
            conn.execute(text("ALTER TABLE transactions ADD COLUMN fair_value VARCHAR(64)"))

def _ensure_fx_rates_has_batch_id():
    # Add fx_rates.batch_id if the DB was created before we introduced the column
    with engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(fx_rates)")).fetchall()
        names = [c[1] for c in cols]
        if "batch_id" not in names:
            conn.execute(text("ALTER TABLE fx_rates ADD COLUMN batch_id INTEGER"))

def _ensure_indexes():
    # create low-risk indexes if missing (idempotent)
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions(timestamp)"))
        # Optional (uncomment if you filter often by these):
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type)"))
        # conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_asset ON transactions(base_asset)"))

@app.on_event("startup")
def on_startup() -> None:
    """
    Runs when the server starts.
    - Ensures database tables exist (idempotent).
    """
    Base.metadata.create_all(bind=engine)
    _ensure_transactions_has_fair_value_column()
    _ensure_fx_rates_has_batch_id()
    _ensure_indexes()
    _set_sqlite_pragmas()

# -----------------------------------------------------------------------------
# Health + version endpoints (simple sanity checks)
# -----------------------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, str]:
    """Quick liveness check for monitoring or manual testing."""
    return {"status": "ok"}


@app.get("/version")
def version() -> Dict[str, str]:
    """Show the backend name and version (useful to confirm deployments)."""
    return {"name": "CryptoTaxCalc", "version": __version__}


# -----------------------------------------------------------------------------
# CSV endpoints
# -----------------------------------------------------------------------------
@app.post("/upload/csv", response_model=CSVPreviewResponse)
async def upload_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Accept a CSV upload, parse & validate it, and return a PREVIEW (no DB writes).

    Why preview? Users can see what's parsed and fix errors before saving.
    """
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    data = await file.read()
    if len(data) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    try:
        valid_rows, errors = parse_csv(data)
    except Exception as e:
        # In production, we'd log the exception and return a generic message.
        raise HTTPException(status_code=500, detail=f"Parser error: {e!s}")

    preview = [vr for vr in valid_rows[:5]]
    return {
        "filename": filename,
        "total_valid": len(valid_rows),
        "total_errors": len(errors),
        "preview_first_5": preview,
        "errors": errors[:5],
    }

@app.post("/import/csv", response_model=ImportCSVResponse)
async def import_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    DEPRECATED: Use /import/multiple instead. This wrapper calls the same logic.
    """
    # Reuse the same function you use inside /import/multiple for a single file.
    # NOTE: /import/multiple is async, so we await it.
    result = await import_multiple([file])

    # Add a gentle deprecation warning header.
    return JSONResponse(result, headers={"Warning": '299 - "Deprecated; use /import/multiple"'})

    """
    Accept a CSV upload, parse & validate it, and SAVE valid rows to the DB.
    Also:
      - store original file SHA-256 + path in raw_events
      - link each derived transaction to raw_event_id
      - deduplicate by transaction hash
    """
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    data = await file.read()
    if len(data) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    # Persist the original file once (provenance)
    blob_path, digest = persist_uploaded_file(file, data)
    received_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    mime = file.content_type or "application/octet-stream"

    # Insert a raw_events row
    with engine.begin() as conn:
        res = conn.execute(
            text("""
                INSERT INTO raw_events (source_filename, file_sha256, mime_type, importer, received_at, notes, blob_path)
                VALUES (:f, :h, :m, :imp, :ts, :n, :p)
            """),
            dict(f=filename, h=digest, m=mime, imp="api/upload", ts=received_at, n=None, p=blob_path)
        )
        raw_event_id = res.lastrowid

    audit("local-user", "import:file", "raw_events", raw_event_id, {"filename": filename, "sha256": digest})

    # Parse the CSV
    try:
        valid_rows, errors = parse_csv(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parser error: {e!s}")

    inserted = 0
    skipped_duplicates = 0
    with SessionLocal() as session:
        for tx in valid_rows:
            tx_hash = compute_tx_hash(tx)

            # Duplicate detection by hash
            existing = session.query(TransactionRow).filter_by(hash=tx_hash).first()
            if existing:
                skipped_duplicates += 1
                continue

            row = TransactionRow(
                hash=tx_hash,
                timestamp=tx.timestamp,
                type=tx.type,
                base_asset=tx.base_asset,
                base_amount=str(tx.base_amount),
                quote_asset=tx.quote_asset,
                quote_amount=(str(tx.quote_amount) if tx.quote_amount is not None else None),
                fee_asset=tx.fee_asset,
                fee_amount=(str(tx.fee_amount) if tx.fee_amount is not None else None),
                exchange=tx.exchange,
                memo=tx.memo,
                fair_value=(str(tx.fair_value) if getattr(tx, "fair_value", None) is not None else None),
                raw_event_id=raw_event_id,  # <-- link provenance
            )
            session.add(row)
            inserted += 1

        session.commit()

    return {
        "filename": filename,
        "inserted": inserted,
        "skipped_duplicates": skipped_duplicates,
        "skipped_errors": len(errors),
        "note": "Use GET /transactions to view saved rows."
    }

@app.post("/import/multiple")
async def import_multiple(files: List[UploadFile] = File(...)):
    """
    Accept multiple CSV files in one request.
    For each file:
      - Store original file in raw_events (SHA-256, path)
      - Parse & validate
      - Insert with duplicate detection by hash
      - Link transactions.raw_event_id to the raw_events row
    Returns: one report per file.
    """
    results = []

    for file in files:
        filename = file.filename or "(no-name)"
        inserted = 0
        skipped_duplicates = 0
        skipped_errors = 0

        try:
            contents = await file.read()
            if not contents:
                results.append({"filename": filename, "error": "Empty file"})
                continue

            # provenance per-file
            blob_path, digest = persist_uploaded_file(file, contents)
            received_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            mime = file.content_type or "application/octet-stream"

            with engine.begin() as conn:
                res = conn.execute(
                    text("""
                    INSERT INTO raw_events (source_filename, file_sha256, mime_type, importer, received_at, notes, blob_path)
                    VALUES (:f, :h, :m, :imp, :ts, :n, :p)
                    """),
                    dict(f=filename, h=digest, m=mime, imp="api/upload", ts=received_at, n=None, p=blob_path)
                )
                raw_event_id = res.lastrowid

            audit("local-user", "import:file", "raw_events", raw_event_id, {"filename": filename, "sha256": digest})

            # Parse
            valid_rows, parse_errors = parse_csv(contents)
            skipped_errors += len(parse_errors)

        except Exception as e:
            results.append({"filename": filename, "error": f"Failed to parse CSV: {str(e)}"})
            continue

        with SessionLocal() as session:
            for tx in valid_rows:
                tx_hash = compute_tx_hash(tx)

                existing = session.query(TransactionRow).filter_by(hash=tx_hash).first()
                if existing:
                    skipped_duplicates += 1
                    continue

                row = TransactionRow(
                    hash=tx_hash,
                    timestamp=tx.timestamp,
                    type=tx.type,
                    base_asset=tx.base_asset,
                    base_amount=str(tx.base_amount),
                    quote_asset=tx.quote_asset,
                    quote_amount=(str(tx.quote_amount) if tx.quote_amount is not None else None),
                    fee_asset=tx.fee_asset,
                    fee_amount=(str(tx.fee_amount) if tx.fee_amount is not None else None),
                    exchange=tx.exchange,
                    memo=tx.memo,
                    fair_value=(str(tx.fair_value) if getattr(tx, "fair_value", None) is not None else None),
                    raw_event_id=raw_event_id,  # <-- link provenance
                )
                session.add(row)
                inserted += 1

            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                results.append({
                    "filename": filename,
                    "error": "Database integrity error (possibly duplicate hash).",
                })
                continue

        results.append({
            "filename": filename,
            "inserted": inserted,
            "skipped_duplicates": skipped_duplicates,
            "skipped_errors": skipped_errors,
        })

    return {"results": results}


    """
    Return the most recent 100 transactions from the database.

    Later:
    - add pagination & filters (by date range, asset, type)
    - add authentication (only show the current user's data)
    """
    out: List[dict] = []
    with SessionLocal() as session:
        rows = session.query(TransactionRow).order_by(TransactionRow.id.desc()).limit(100).all()
        for r in rows:
            out.append({
                "id": r.id,
                "timestamp": r.timestamp.isoformat(),
                "type": r.type,
                "base_asset": r.base_asset,
                "base_amount": r.base_amount,
                "quote_asset": r.quote_asset,
                "quote_amount": r.quote_amount,
                "fee_asset": r.fee_asset,
                "fee_amount": r.fee_amount,
                "exchange": r.exchange,
                "memo": r.memo,
            })
    return out

@app.get("/transactions")
def list_transactions(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    asset: str | None = None,
    type: str | None = None,
    exchange: str | None = None,
    date_from: str | None = None,   # YYYY-MM-DD
    date_to: str | None = None,     # YYYY-MM-DD
    sort: str = Query("timestamp_desc", pattern="^(timestamp_asc|timestamp_desc)$"),
):
    """
    Paginated, filterable list of transactions.
    """
    with SessionLocal() as session:
        q = session.query(TransactionRow)
        conds = []

        if asset:
            conds.append(TransactionRow.base_asset == asset.upper())
        if type:
            conds.append(TransactionRow.type == type.lower())
        if exchange:
            conds.append(TransactionRow.exchange == exchange)

        # Date filtering (inclusive)
        def parse_d(d: str) -> _dt:
            return _dt.strptime(d, "%Y-%m-%d")

        if date_from:
            df = parse_d(date_from)
            conds.append(TransactionRow.timestamp >= df)
        if date_to:
            dt = parse_d(date_to)
            conds.append(TransactionRow.timestamp <= dt)

        if conds:
            q = q.filter(and_(*conds))

        total = q.count()

        if sort == "timestamp_asc":
            q = q.order_by(TransactionRow.timestamp.asc(), TransactionRow.id.asc())
        else:
            q = q.order_by(TransactionRow.timestamp.desc(), TransactionRow.id.desc())

        items = q.offset((page - 1) * page_size).limit(page_size).all()

        # Serialize minimal fields (unchanged keys to avoid breaking clients)
        data = []
        for r in items:
            data.append({
                "id": r.id,
                "timestamp": r.timestamp.isoformat(timespec="seconds"),
                "type": r.type,
                "base_asset": r.base_asset,
                "base_amount": str(r.base_amount),
                "quote_asset": r.quote_asset,
                "quote_amount": (str(r.quote_amount) if r.quote_amount is not None else None),
                "fee_asset": r.fee_asset,
                "fee_amount": (str(r.fee_amount) if r.fee_amount is not None else None),
                "exchange": r.exchange,
                "memo": r.memo,
            })

        return {
            "meta": {"page": page, "page_size": page_size, "total": total},
            "items": data,
        }

@app.get("/calculate")
def calculate_fifo() -> Dict[str, Any]:
    """
    Run the FIFO engine on all stored transactions and return:
      - realized events (each sale: proceeds, cost basis, gain)
      - summary totals by quote asset
      - warnings explaining assumptions/data gaps
    Additionally:
      - create a calc_runs row (rule_version, fx_set_id, params snapshot)
      - persist realized_events for audit
      - include run_id in the response
    """
    from models import TransactionRow
    from db import SessionLocal

    # Load transactions oldest-first
    tx_models: List[Transaction] = []
    with SessionLocal() as session:
        rows = session.query(TransactionRow)\
            .order_by(TransactionRow.timestamp.asc(), TransactionRow.id.asc())\
            .all()
        for r in rows:
            tx_models.append(Transaction(
                timestamp=r.timestamp,
                type=r.type,
                base_asset=r.base_asset,
                base_amount=Decimal(str(r.base_amount)),
                quote_asset=r.quote_asset,
                quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
                fee_asset=r.fee_asset,
                fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
                exchange=r.exchange,
                memo=r.memo,
                fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
            ))

    # Create calc_runs row (freeze metadata)
    started_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rule_version = "2025.01.fifo.v1"
    jurisdiction = "HR"
    lot_method = "FIFO"

    from .fx_utils import get_or_create_current_fx_batch_id
    fx_set_id = get_or_create_current_fx_batch_id()

    params = {
        "rounding": "bankers",
        "tz_policy": "UTC",
        "fee_policy": "quote_fee_reduces_proceeds"
    }

    with engine.begin() as conn:
        run_id = conn.execute(
            text("""
            INSERT INTO calc_runs (started_at, jurisdiction, rule_version, lot_method, fx_set_id, params_json)
            VALUES (:sa, :j, :rv, :lm, :fx, :pj)
            """),
            dict(sa=started_at, j=jurisdiction, rv=rule_version, lm=lot_method, fx=fx_set_id, pj=json.dumps(params))
        ).lastrowid

    # Compute FIFO
    events, summary, warnings = compute_fifo(tx_models)

    # EUR totals (same as your original logic)
    eur_totals = {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
    eur_notes: List[str] = []
    with SessionLocal() as session:
        for ev in events:
            q = ev.quote_asset.upper() if ev.quote_asset else ""
            if q == "EUR":
                eur_totals["proceeds"] += ev.proceeds
                eur_totals["cost_basis"] += ev.cost_basis
                eur_totals["gain"] += ev.gain
            elif q in {"USD", "USDT"}:
                ev_date = datetime.fromisoformat(ev.timestamp).date()
                usd_per_eur = get_rate_for_date(session, ev_date)
                if usd_per_eur is None:
                    eur_notes.append(f"No EURUSD rate for {ev_date}; skipping conversion for event at {ev.timestamp}.")
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, usd_per_eur)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, usd_per_eur)
                eur_totals["gain"] += usd_to_eur(ev.gain, usd_per_eur)
            else:
                eur_notes.append(f"Unsupported quote asset {q} for event at {ev.timestamp}; no EUR conversion.")

    eur_summary = {
        "totals_eur": {
            "proceeds": dec_to_str(eur_totals["proceeds"]),
            "cost_basis": dec_to_str(eur_totals["cost_basis"]),
            "gain": dec_to_str(eur_totals["gain"]),
        },
        "notes": eur_notes[:10]
    }

    # Persist realized events for this run
    with engine.begin() as conn:
        for ev in events:
            conn.execute(
                text("""
                INSERT INTO realized_events
                (run_id, tx_id, timestamp, asset, qty_sold, proceeds, cost_basis, gain, quote_asset, fee_applied, matches_json)
                VALUES (:rid, :tx, :ts, :asset, :qty, :p, :cb, :g, :qa, :fee, :mj)
                """),
                dict(
                    rid=run_id,
                    tx=None,  # if you later track tx_id → set it here
                    ts=ev.timestamp,
                    asset=ev.asset,
                    qty=str(ev.qty_sold),
                    p=str(ev.proceeds),
                    cb=str(ev.cost_basis),
                    g=str(ev.gain),
                    qa=ev.quote_asset,
                    fee=str(ev.fee_applied),
                    mj=json.dumps([
                        {"from_qty": str(m.from_qty), "lot_cost_per_unit": str(m.lot_cost_per_unit), "lot_cost_total": str(m.lot_cost_total)}
                        for m in ev.matches
                    ])
                )
            )
        finished_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        conn.execute(text("UPDATE calc_runs SET finished_at=:fa WHERE id=:rid"), dict(fa=finished_at, rid=run_id))

    # --- NEW: build manifest + compute digests + persist in run_digests
    from .audit_digest import build_run_manifest, compute_digests
    manifest = build_run_manifest(int(run_id))
    digests = compute_digests(manifest)

    created_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # IMPORTANT: open a fresh transaction/connection; the prior `conn` is closed
    with engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO run_digests (run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at)
            VALUES (:rid, :ih, :oh, :mh, :mj, :ts)
            ON CONFLICT(run_id) DO UPDATE SET
                input_hash=excluded.input_hash,
                output_hash=excluded.output_hash,
                manifest_hash=excluded.manifest_hash,
                manifest_json=excluded.manifest_json,
                created_at=excluded.created_at
            """),
            dict(
                rid=int(run_id),
                ih=digests["input_hash"],
                oh=digests["output_hash"],
                mh=digests["manifest_hash"],
                mj=json.dumps(manifest),
                ts=created_at,
            ),
        )

    audit("local-user", "calc:run", "calc_runs", run_id, {"rule_version": rule_version, "fx_set_id": fx_set_id})

    # Serialize events for JSON
    def ev_to_dict(ev) -> Dict[str, Any]:
        return {
            "timestamp": ev.timestamp,
            "asset": ev.asset,
            "qty_sold": dec_to_str(ev.qty_sold),
            "proceeds": dec_to_str(ev.proceeds),
            "cost_basis": dec_to_str(ev.cost_basis),
            "gain": dec_to_str(ev.gain),
            "quote_asset": ev.quote_asset,
            "fee_applied": dec_to_str(ev.fee_applied),
            "matches": [
                {"from_qty": dec_to_str(m.from_qty),
                 "lot_cost_per_unit": dec_to_str(m.lot_cost_per_unit),
                 "lot_cost_total": dec_to_str(m.lot_cost_total)}
                for m in ev.matches
            ],
        }

    return {
        "run_id": int(run_id),   # <-- include in response for stamping PDFs later
        "events": [ev_to_dict(e) for e in events],
        "summary": summary,
        "eur_summary": eur_summary,
        "warnings": warnings,
    }

@app.get("/report/summary")
def report_summary(
    year: int,
    asset: str | None = None,
    quote_asset: str | None = None,
):
    # 1) Load transactions from DB (oldest-first) and rebuild models
    with SessionLocal() as session:
        rows = session.query(TransactionRow).order_by(
            TransactionRow.timestamp.asc(), TransactionRow.id.asc()
        ).all()

        tx_models: list[Transaction] = []
        for r in rows:
            tx_models.append(Transaction(
                timestamp=r.timestamp,  # keep as datetime; compute_fifo handles it
                type=r.type,
                base_asset=r.base_asset,
                base_amount=Decimal(str(r.base_amount)),
                quote_asset=r.quote_asset,
                quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
                fee_asset=r.fee_asset,
                fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
                exchange=r.exchange,
                memo=r.memo,
                fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
            ))

        # 2) Compute FIFO (UNPACK the tuple!)
        all_events, _summary_unused, _warnings_unused = compute_fifo(tx_models)

        # 3) Filter events by year / optional filters
        events: list = []
        for e in all_events:
            dt = _parse_iso_ts(e.timestamp)
            if not dt:
                continue
            if dt.year != year:
                continue
            if asset and e.asset != asset:
                continue
            if quote_asset and e.quote_asset != quote_asset:
                continue
            events.append(e)

        # 4) If no events, return a clean empty summary
        if not events:
            return {
                "year": year,
                "by_month": {},
                "summary_by_quote": {},
                "summary_by_asset": {},
                "eur_summary": {
                    "totals_eur": {"proceeds": "0", "cost_basis": "0", "gain": "0"},
                    "notes": [f"No realized events found for {year} with current filters."]
                },
                "total_warnings": 0,
                "warnings": []
            }

        # 5) Aggregate by quote asset
        by_quote: dict[str, dict[str, Decimal]] = {}
        for e in events:
            q = (e.quote_asset or "UNKNOWN").upper()
            agg = by_quote.setdefault(q, {"proceeds": _d0(), "cost_basis": _d0(), "gain": _d0()})
            agg["proceeds"] += e.proceeds
            agg["cost_basis"] += e.cost_basis
            agg["gain"] += e.gain
        summary_by_quote = {q: {k: _as_str(v) for k, v in sums.items()} for q, sums in by_quote.items()}

        # 6) Aggregate by base asset
        by_base: dict[str, dict[str, Decimal]] = {}
        for e in events:
            b = e.asset
            agg = by_base.setdefault(b, {"proceeds": _d0(), "cost_basis": _d0(), "gain": _d0()})
            agg["proceeds"] += e.proceeds
            agg["cost_basis"] += e.cost_basis
            agg["gain"] += e.gain
        summary_by_asset = {a: {k: _as_str(v) for k, v in sums.items()} for a, sums in by_base.items()}

        # 7) Aggregate by month (YYYY-MM)
        per_month: dict[str, dict[str, Decimal]] = {}
        for e in events:
            dt = _parse_iso_ts(e.timestamp)
            if not dt:
                continue
            mkey = f"{dt.year:04d}-{dt.month:02d}"
            agg = per_month.setdefault(mkey, {"proceeds": _d0(), "cost_basis": _d0(), "gain": _d0()})
            agg["proceeds"] += e.proceeds
            agg["cost_basis"] += e.cost_basis
            agg["gain"] += e.gain
        by_month = {m: {k: _as_str(v) for k, v in sums.items()} for m, sums in per_month.items()}

        # 8) EUR totals using stored FX (supports USD/USDT; EUR passthrough)
        notes: list[str] = []
        proceeds_eur = _d0()
        basis_eur = _d0()
        gain_eur = _d0()

        for e in events:
            dt = _parse_iso_ts(e.timestamp)
            if not dt:
                continue
            q = (e.quote_asset or "").upper()
            if q == "EUR":
                proceeds_eur += e.proceeds
                basis_eur    += e.cost_basis
                gain_eur     += e.gain
            elif q in ("USD", "USDT"):
                rate = get_rate_for_date(session, dt.date())  # <-- pass session here
                if rate and rate != 0:
                    proceeds_eur += (e.proceeds / rate)
                    basis_eur    += (e.cost_basis / rate)
                    gain_eur     += (e.gain / rate)
                else:
                    notes.append(f"No ECB USD-per-EUR rate for {dt.date()}; skipped conversion for an event.")
            else:
                notes.append(f"EUR conversion for quote '{q}' not implemented; skipped an event.")

    eur_summary = {
        "totals_eur": {
            "proceeds": _as_str(proceeds_eur),
            "cost_basis": _as_str(basis_eur),
            "gain": _as_str(gain_eur),
        },
        "notes": notes,
    }

    # 9) Return robust summary
    return {
        "year": year,
        "by_month": by_month,
        "summary_by_quote": summary_by_quote,
        "summary_by_asset": summary_by_asset,
        "eur_summary": eur_summary,
        "total_warnings": len(notes),
        "warnings": notes[:10],
    }

@app.post("/fx/upload")
async def fx_upload(file: UploadFile = File(...)) -> dict:
    """
    Upload a CSV of daily EURUSD rates.
    Required headers: date, usd_per_eur
      - date format: YYYY-MM-DD
      - usd_per_eur: decimal number (USD per 1 EUR)
    Creates a new fx_batches row and tags the imported/updated dates with batch_id.
    """
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    raw = await file.read()
    text_csv = raw.decode("utf-8-sig", errors="replace")
    reader = DictReader(StringIO(text_csv))

    required = {"date", "usd_per_eur"}
    if not reader.fieldnames or required - set(h.strip().lower() for h in reader.fieldnames):
        raise HTTPException(status_code=400, detail="CSV must include: date, usd_per_eur")

    header_map = {h.strip().lower(): h for h in (reader.fieldnames or [])}

    inserted, updated, errors = 0, 0, 0
    touched_dates: set[date] = set()

    # Create a fresh FX batch for this upload
    now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with SessionLocal() as session:
        # insert batch
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            dict(t=now_iso, s="ECB CSV", h=None)
        ).lastrowid

        for row in reader:
            try:
                raw_date = row[header_map["date"]].strip()
                raw_rate = row[header_map["usd_per_eur"]].strip()
                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                eurusd = Decimal(raw_rate)
            except Exception:
                errors += 1
                continue

            existing = session.get(FxRate, d)
            if existing:
                existing.usd_per_eur = str(eurusd)
                existing.batch_id = bid
                updated += 1
            else:
                session.add(FxRate(date=d, usd_per_eur=str(eurusd), batch_id=bid))
                inserted += 1

            touched_dates.add(d)

        session.commit()

        # --- NEW: compute deterministic hash of this batch's rates and store in fx_batches.rates_hash
        rows_for_hash = session.execute(
            text("SELECT date, usd_per_eur FROM fx_rates WHERE batch_id = :b ORDER BY date"),
            dict(b=bid)
        ).fetchall()

        import hashlib
        h = hashlib.sha256()
        for r in rows_for_hash:
            # Use a fixed line format: YYYY-MM-DD|<rate>\n
            line = f"{r[0]}|{r[1]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            dict(rh=rates_hash, bid=bid)
        )
        session.commit()

    return {"inserted": inserted, "updated": updated, "errors": errors, "batch_id": int(bid)}

@app.post("/maintenance/prune_fx")
def prune_fx(keep_years: int = Query(5, ge=1, le=20)) -> Dict[str, Any]:
    """
    Keep only the last N years of FX rates (rolling window).
    Safe: does not touch transactions.
    """
    today = _date.today()
    cutoff_year = today.year - keep_years
    cutoff = _date(cutoff_year, 1, 1)  # keep from Jan 1 of cutoff_year onward

    deleted = 0
    with SessionLocal() as session:
        # Count before
        total_before = session.query(FxRate).count()
        session.query(FxRate).filter(FxRate.date < cutoff).delete(synchronize_session=False)
        session.commit()
        total_after = session.query(FxRate).count()
        deleted = total_before - total_after

    return {"status": "ok", "kept_from": str(cutoff), "deleted_rows": deleted}

@app.post("/maintenance/vacuum")
def vacuum_sqlite() -> Dict[str, Any]:
    """
    Run VACUUM to reclaim file space in SQLite after large deletes.
    """
    with engine.connect() as conn:
        conn.execute(text("VACUUM"))
    return {"status": "ok"}

@app.get("/export/db", summary="Download current database backup")
def export_database():
    """
    Returns the current SQLite database file for backup purposes.
    Safe, read-only endpoint.
    """
    file_path = "data.db"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Database not found")

    # Optional: create a timestamped copy before serving
    import shutil, datetime
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_name = f"data_backup_{timestamp}.db"
    backup_path = os.path.join("backups", backup_name)

    # Ensure backups directory exists
    os.makedirs("backups", exist_ok=True)
    shutil.copy2(file_path, backup_path)

    # Return the original file as a download
    return FileResponse(
        path=file_path,
        filename=backup_name,
        media_type="application/x-sqlite3"
    )

@app.post("/import/db",summary="Restore the SQLite database from an uploaded .db file (with safety checks)")
async def import_database(
    file: UploadFile = File(...),
    confirm: str = Query("", description="Must be 'I_UNDERSTAND' to proceed")
):
    """
    Restores the application's SQLite database from an uploaded file.
    Safety features:
    - Requires explicit confirm='I_UNDERSTAND'
    - Validates SQLite magic header
    - Backs up current data.db before replacing
    - Disposes engine to release file locks
    - Verifies PRAGMA integrity_check after swap; rolls back on failure
    """
    if confirm != "I_UNDERSTAND":
        raise HTTPException(
            status_code=400,
            detail="Confirmation missing. Add ?confirm=I_UNDERSTAND to proceed (this will overwrite the current DB)."
        )

    # 0) Ensure backups dir exists
    os.makedirs("backups", exist_ok=True)

    # 1) Save uploaded file to a temp path
    suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp_dir = tempfile.mkdtemp(prefix="restore_")
    tmp_upload = os.path.join(tmp_dir, f"uploaded_{suffix}.db")

    try:
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        with open(tmp_upload, "wb") as f:
            f.write(raw)

        # 2) Validate it's really a SQLite DB
        if not _is_sqlite_file(tmp_upload):
            raise HTTPException(status_code=400, detail="Uploaded file is not a valid SQLite database.")

        # 3) Optional pre-check integrity of the uploaded DB
        if not _integrity_ok(tmp_upload):
            raise HTTPException(status_code=400, detail="Uploaded database failed integrity_check.")

        # 4) Backup current DB
        src_db = "data.db"
        if not os.path.exists(src_db):
            # Allow restore even if current DB does not exist
            backup_path = None
        else:
            backup_name = f"data_before_restore_{suffix}.db"
            backup_path = os.path.join("backups", backup_name)
            shutil.copy2(src_db, backup_path)

        # 5) Dispose global engine so SQLite file is not locked
        try:
            engine.dispose()
        except Exception:
            pass  # best effort

        # 6) Replace data.db with the uploaded file
        shutil.copy2(tmp_upload, src_db)

        # 7) Post-swap integrity check (on the active DB path)
        if not _integrity_ok(src_db):
            # Roll back if broken
            if backup_path and os.path.exists(backup_path):
                shutil.copy2(backup_path, src_db)
            raise HTTPException(status_code=500, detail="Restored DB failed integrity_check; original DB has been restored.")

        # 8) Success
        return JSONResponse({
            "status": "ok",
            "message": "Database restored successfully.",
            "backup": (backup_path or "none"),
            "restored_from": file.filename
        })

    finally:
        # Cleanup temp directory
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass

@app.get("/export/calculate.csv", summary="Download realized events (FIFO) as CSV")
def export_calculate_csv() -> Response:
    # Reuse the same data source as /calculate
    with SessionLocal() as session:
        rows = session.query(TransactionRow).order_by(TransactionRow.timestamp.asc()).all()

    tx_models: list[Transaction] = []
    for r in rows:
        tx_models.append(Transaction(
            timestamp=r.timestamp,
            type=r.type,
            base_asset=r.base_asset,
            base_amount=Decimal(str(r.base_amount)),
            quote_asset=r.quote_asset,
            quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
            fee_asset=r.fee_asset,
            fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
            exchange=r.exchange,
            memo=r.memo,
            fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
        ))

    events, summary, warnings = compute_fifo(tx_models)

    # Build CSV in-memory
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow([
        "timestamp","asset","qty_sold","proceeds","cost_basis","gain",
        "quote_asset","fee_applied","matches_count"
    ])

    for ev in events:
        writer.writerow([
            ev.timestamp,
            ev.asset,
            dec_to_str(ev.qty_sold),
            dec_to_str(ev.proceeds),
            dec_to_str(ev.cost_basis),
            dec_to_str(ev.gain),
            ev.quote_asset or "",
            dec_to_str(ev.fee_applied),
            len(ev.matches or []),
        ])

    csv_bytes = buf.getvalue().encode("utf-8")
    headers = {
        "Content-Disposition": 'attachment; filename="calculate_events.csv"'
    }
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8", headers=headers)

@app.get("/export/summary.csv", summary="Download summary (optionally by year) as CSV")
def export_summary_csv(year: int | None = None) -> Response:
    # Pull transactions
    with SessionLocal() as session:
        q = session.query(TransactionRow).order_by(TransactionRow.timestamp.asc())
        rows = q.all()

    # Rebuild models
    tx_models: list[Transaction] = []
    for r in rows:
        tx_models.append(Transaction(
            timestamp=r.timestamp,
            type=r.type,
            base_asset=r.base_asset,
            base_amount=Decimal(str(r.base_amount)),
            quote_asset=r.quote_asset,
            quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
            fee_asset=r.fee_asset,
            fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
            exchange=r.exchange,
            memo=r.memo,
            fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
        ))

    # Compute FIFO once
    events, summary, warnings = compute_fifo(tx_models)

    # Optional year filter
    if year is not None:
        events = [e for e in events if datetime.fromisoformat(e.timestamp).year == year]

    # Aggregate by quote asset (like /report/summary)
    by_quote: dict[str, dict[str, Decimal]] = {}
    for ev in events:
        q = (ev.quote_asset or "").upper()
        agg = by_quote.setdefault(q, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg["proceeds"] += ev.proceeds
        agg["cost_basis"] += ev.cost_basis
        agg["gain"] += ev.gain

    # EUR conversion (same logic you use in /report/summary)
    eur_totals = {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
    with SessionLocal() as session:
        for ev in events:
            q = (ev.quote_asset or "").upper()
            if q == "EUR":
                eur_totals["proceeds"] += ev.proceeds
                eur_totals["cost_basis"] += ev.cost_basis
                eur_totals["gain"] += ev.gain
            elif q in {"USD", "USDT"}:
                ev_date = datetime.fromisoformat(ev.timestamp).date()
                usd_per_eur = get_rate_for_date(session, ev_date)
                if usd_per_eur is None:
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, usd_per_eur)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, usd_per_eur)
                eur_totals["gain"] += usd_to_eur(ev.gain, usd_per_eur)

    # Build CSV
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["section","key","proceeds","cost_basis","gain"])

    # By quote asset
    for q, agg in by_quote.items():
        writer.writerow([
            "by_quote_asset",
            q or "(none)",
            dec_to_str(agg["proceeds"]),
            dec_to_str(agg["cost_basis"]),
            dec_to_str(agg["gain"]),
        ])

    # Totals (all quotes combined)
    tot_pro = sum((v["proceeds"] for v in by_quote.values()), Decimal("0"))
    tot_cb  = sum((v["cost_basis"] for v in by_quote.values()), Decimal("0"))
    tot_g   = sum((v["gain"] for v in by_quote.values()), Decimal("0"))

    writer.writerow(["totals","ALL",
        dec_to_str(tot_pro),
        dec_to_str(tot_cb),
        dec_to_str(tot_g)
    ])

    # EUR totals (converted)
    writer.writerow(["totals_eur","EUR",
        dec_to_str(eur_totals["proceeds"]),
        dec_to_str(eur_totals["cost_basis"]),
        dec_to_str(eur_totals["gain"])
    ])

    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"summary{('_' + str(year)) if year else ''}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8", headers=headers)

@app.get("/export/summary.pdf", summary="Download PDF summary (optionally filtered by year)")
def export_summary_pdf(year: int | None = None) -> StreamingResponse:
    # 1) Load all transactions (same as /report/summary)
    with SessionLocal() as session:
        rows = session.query(TransactionRow).order_by(TransactionRow.timestamp.asc()).all()

    tx_models: list[Transaction] = []
    for r in rows:
        tx_models.append(Transaction(
            timestamp=r.timestamp,
            type=r.type,
            base_asset=r.base_asset,
            base_amount=Decimal(str(r.base_amount)),
            quote_asset=r.quote_asset,
            quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
            fee_asset=r.fee_asset,
            fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
            exchange=r.exchange,
            memo=r.memo,
            fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
        ))

    # 2) Compute FIFO once
    events, summary, warnings = compute_fifo(tx_models)

    # 3) Optional filter by year (affects both tables and EUR conversion)
    if year is not None:
        events = [e for e in events if datetime.fromisoformat(e.timestamp).year == year]

    # 4) Rebuild by-quote aggregation (like /report/summary)
    by_quote: dict[str, dict[str, Decimal]] = {}
    for ev in events:
        q = (ev.quote_asset or "").upper()
        agg = by_quote.setdefault(q, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg["proceeds"] += ev.proceeds
        agg["cost_basis"] += ev.cost_basis
        agg["gain"] += ev.gain

    totals = {
        "proceeds": sum((v["proceeds"] for v in by_quote.values()), Decimal("0")),
        "cost_basis": sum((v["cost_basis"] for v in by_quote.values()), Decimal("0")),
        "gain": sum((v["gain"] for v in by_quote.values()), Decimal("0")),
    }

    # 5) Build EUR totals (same logic as /report/summary & /calculate)
    eur_totals = {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
    with SessionLocal() as session:
        for ev in events:
            q = (ev.quote_asset or "").upper()
            if q == "EUR":
                eur_totals["proceeds"] += ev.proceeds
                eur_totals["cost_basis"] += ev.cost_basis
                eur_totals["gain"] += ev.gain
            elif q in {"USD", "USDT"}:
                ev_date = datetime.fromisoformat(ev.timestamp).date()
                usd_per_eur = get_rate_for_date(session, ev_date)
                if usd_per_eur is None:
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, usd_per_eur)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, usd_per_eur)
                eur_totals["gain"] += usd_to_eur(ev.gain, usd_per_eur)

    # 6) Build the PDF bytes
    pdf_bytes = _build_summary_pdf(
        title="Crypto Tax – FIFO Summary",
        year=year,
        by_quote=by_quote,
        totals=totals,
        eur_totals=eur_totals,
        top_events=events
    )

    # 7) Stream it to the client
    filename = f"summary{('_' + str(year)) if year else ''}.pdf"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/export/calculate.pdf", summary="Download realized events (FIFO) as a PDF table")
def export_calculate_pdf() -> StreamingResponse:
    # Load data
    with SessionLocal() as session:
        rows = session.query(TransactionRow).order_by(TransactionRow.timestamp.asc()).all()

    tx_models: list[Transaction] = []
    for r in rows:
        tx_models.append(Transaction(
            timestamp=r.timestamp,
            type=r.type,
            base_asset=r.base_asset,
            base_amount=Decimal(str(r.base_amount)),
            quote_asset=r.quote_asset,
            quote_amount=(Decimal(str(r.quote_amount)) if r.quote_amount is not None else None),
            fee_asset=r.fee_asset,
            fee_amount=(Decimal(str(r.fee_amount)) if r.fee_amount is not None else None),
            exchange=r.exchange,
            memo=r.memo,
            fair_value=(Decimal(str(r.fair_value)) if getattr(r, "fair_value", None) else None),
        ))

    events, summary, warnings = compute_fifo(tx_models)

    # Build a compact PDF listing events only (reuse helper with empty summary)
    by_quote_dummy = {}
    totals_dummy = {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
    eur_dummy = {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
    pdf_bytes = _build_summary_pdf(
        title="Crypto Tax – Realized Events (FIFO)",
        year=None,
        by_quote=by_quote_dummy,
        totals=totals_dummy,
        eur_totals=eur_dummy,
        top_events=events
    )

    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="calculate_events.pdf"'}
    )


    # If we ever want deeper checks later (DB ping, FX cache), add them here.
    return {"status": "ok"}

@app.get("/export/events_csv")
def export_events_csv(run_id: str = "latest"):
    """
    Export realized events for a given run as CSV.
    Use run_id=latest to export the most recent run.
    """
    with engine.begin() as conn:
        if run_id == "latest":
            row = conn.execute(text("SELECT id FROM calc_runs ORDER BY id DESC LIMIT 1")).fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="No calculation runs found.")
            run_id_val = int(row[0])
        else:
            try:
                run_id_val = int(run_id)
            except:
                raise HTTPException(status_code=400, detail="Invalid run_id")

        rows = conn.execute(text("""
            SELECT timestamp, asset, qty_sold, proceeds, cost_basis, gain, quote_asset, fee_applied, matches_json
            FROM realized_events
            WHERE run_id = :rid
            ORDER BY id
        """), dict(rid=run_id_val)).fetchall()

    if not rows:
        raise HTTPException(status_code=400, detail=f"No realized events for run_id={run_id_val}")

    output = io.StringIO()
    w = _csv.writer(output)
    w.writerow(["timestamp","asset","qty_sold","proceeds","cost_basis","gain","quote_asset","fee_applied","matches_json"])
    for r in rows:
        w.writerow([
            r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]
        ])
    output.seek(0)

    filename = f"realized_events_run_{run_id_val}.csv"
    return StreamingResponse(
        iter([output.read()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/audit/run/{run_id}")
def audit_get_run(run_id: int):
    """
    Return the stored manifest + hashes for a run (and recompute to show parity).
    """
    from .audit_digest import build_run_manifest, compute_digests
    manifest = build_run_manifest(run_id)
    live = compute_digests(manifest)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT input_hash, output_hash, manifest_hash, created_at FROM run_digests WHERE run_id = :rid"),
            dict(rid=run_id)
        ).mappings().first()
    
    # Compare ONLY the three hashes (ignore created_at to avoid false negatives)
    stored = dict(row) if row else None

    match = False
    if stored:
        match = (
            stored.get("input_hash") == live.get("input_hash")
            and stored.get("output_hash") == live.get("output_hash")
            and stored.get("manifest_hash") == live.get("manifest_hash")
        )
    
    return {
        "run_id": run_id,
        "stored": stored,
        "recomputed": live,
        "matches": match,
        "manifest": manifest,  # include for transparency (you can omit in prod if large)
    }

@app.get("/audit/verify/{run_id}")
def audit_verify_run(run_id: int):
    """
    Recompute digest and compare with stored digests. Returns boolean + details.
    """
    from .audit_digest import build_run_manifest, compute_digests
    manifest = build_run_manifest(run_id)
    live = compute_digests(manifest)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT input_hash, output_hash, manifest_hash FROM run_digests WHERE run_id = :rid"),
            dict(rid=run_id)
        ).fetchone()

    if not row:
        return {"run_id": run_id, "verified": False, "reason": "No stored digest for this run.", "recomputed": live}

    stored = {"input_hash": row[0], "output_hash": row[1], "manifest_hash": row[2]}
    ok = (stored == live)
    return {"run_id": run_id, "verified": bool(ok), "stored": stored, "recomputed": live}

@app.post("/admin/bundle", tags=["admin"])
def create_support_bundle(
    request: Request,
    token: str | None = Query(default=None, description="Admin token (alternative to header)"),
    x_admin_token: str | None = Header(default=None, convert_underscores=False)
):
    # 1) Is server configured with a token?
    if not BUNDLE_TOKEN:
        # 500 so you know the server isn’t configured; we don’t accept any token here
        raise HTTPException(status_code=500, detail="Admin token not configured on server (BUNDLE_TOKEN missing).")

    # 2) Accept either header or query ?token=...
    supplied = x_admin_token or token or ""
    if supplied != BUNDLE_TOKEN:
        # 401 if token provided but wrong/empty
        raise HTTPException(status_code=401, detail="Unauthorized")

    script = _abs_script_path()
    if not os.path.exists(script):
        raise HTTPException(status_code=500, detail=f"Bundle script not found: {script}")

    # track previous newest zip
    before = _latest_zip_path()

    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", script,
    ]

    try:
        proc = subprocess.run(
        cmd,
        cwd=os.path.dirname(script),
        capture_output=True,
        text=True,
        encoding="utf-8",   # decode PowerShell output as UTF-8
        errors="ignore",    # skip invalid bytes safely
        timeout=300,
    )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Bundle creation timed out (300s)")

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    time.sleep(0.5)
    # parse path from stdout if present, else take newest zip in folder
    zip_path = None
    for line in stdout.splitlines():
        if "Support bundle created" in line and ".zip" in line:
            # grab the last “word” ending with .zip
            parts = [p for p in line.split() if p.lower().endswith(".zip")]
            if parts:
                zip_path = parts[-1]
                break
    if not zip_path:
        zip_path = _latest_zip_path()

    # optional download=true
    download = request.query_params.get("download")
    if proc.returncode != 0:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "script": script,
                "zip_path": zip_path,
                "stdout": stdout,
                "stderr": stderr,
                "return_code": proc.returncode,
            },
        )

    if not zip_path or not os.path.exists(zip_path):
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Bundle ZIP not found after script completed",
                "script": script,
                "stdout": stdout,
                "stderr": stderr,
            },
        )

    if isinstance(download, str) and download.lower() in ("1", "true", "yes"):
        return FileResponse(path=zip_path, filename=os.path.basename(zip_path), media_type="application/zip")

    return {
        "status": "ok",
        "zip_path": zip_path,
        "script": script,
        "stdout": stdout,
        "stderr": stderr,
        "return_code": proc.returncode,
    }

@app.post("/admin/git-sync", tags=["admin"])
def admin_git_sync(token: str = Query(..., description="Admin token")):
    # auth guard
    if token != os.getenv("ADMIN_TOKEN", "12345"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # run the script
    proc = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy", "Bypass",
            "-File", str(GIT_SCRIPT),
        ],
        cwd=str(PROJECT_ROOT),        # ensure we run in repo root
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="ignore",
    )

    log_path = _latest_log()
    log_tail = ""
    if log_path and log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="ignore")
            log_tail = text[-4000:]  # last ~4k chars
        except Exception:
            log_tail = "<could not read log>"

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "script": str(GIT_SCRIPT),
        "return_code": proc.returncode,
        "stdout": proc.stdout,     # often empty—use log_tail for real info
        "stderr": proc.stderr,
        "log_path": str(log_path) if log_path else None,
        "log_tail": log_tail,
    }
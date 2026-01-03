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

import os, shutil, tempfile, csv, io, json, csv as _csv, sys, time, subprocess, zipfile, traceback
from typing import Dict, Any, List, Literal, Optional, Iterator, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Response, Header, Request, Path as PathParam, Body, Depends, APIRouter
from decimal import Decimal, InvalidOperation
from sqlalchemy.exc import IntegrityError
from datetime import datetime, date, timezone, datetime as dt
from csv import DictReader
from io import StringIO, BytesIO
from sqlalchemy import text, and_, text as _sqltext, select
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse, HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime as _dt
from pathlib import Path as FSPath, Path
import uuid
from dataclasses import is_dataclass, asdict
from uuid import UUID, uuid4
from cryptotaxcalc.report_pdf import build_summary_pdf
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, Session as _OrmSession, Session as SASession
from contextlib import asynccontextmanager
import logging
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.engine import Engine

from .__about__ import __title__, __version__
from .csv_normalizer import CSVFormatError, parse_csv, parse_csv_with_meta, parse_csv_stream_with_meta
from .csv_source_registry import (
    detect_csv_source,
    list_supported_sources,
    list_supported_sources_catalog,
    list_unsupported_signatures,
    remove_unsupported_signature,
)
from .fifo_engine import compute_fifo
from .fx_utils import usd_to_eur, get_or_create_current_fx_batch_id, ensure_rate_or_default, ensure_fx_rates_schema, set_session_factory
from .audit_utils import audit
from .schemas import CSVPreviewResponse, ImportCSVResponse, Transaction, CalcConfig
from .db import SessionLocal, engine, init_db
from .models import Base, TransactionRow, FXRate as FxRate, CalcRun, RunDigest, AuditLog, RealizedEvent, RawEvent, RunInput
from .calc_runner import run_calculation, run_calculation_on_subset
from .exporter import build_export_zip, ExportOptions
from .db_migrations import migrate_fx_rates_add_id, migrate_fx_schema, ensure_fx_schema_v2
from .demo_mode import is_demo_mode_enabled

# --- Global logging integration ---
from cryptotaxcalc.logging_setup import (
    get_logger,
    _atomic_write_json,
    _now_iso_z,
    setup_logging, 
    integrate_uvicorn_logs,
)

import platform
import hashlib  # built-in Python library for secure hashes

from cryptotaxcalc.demo_mode import router as demo_router
from cryptotaxcalc.demo_builder import router as demo_build_router

# Resolve runtime roots:
# - PROJECT_ROOT: writable location (repo root in dev; EXE folder when frozen)
# - RESOURCE_ROOT: bundled assets (repo root in dev; sys._MEIPASS when frozen)
def _resolve_project_root() -> FSPath:
    if getattr(sys, "frozen", False):
        return FSPath(sys.executable).resolve().parent
    here = FSPath(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "pyproject.toml").exists() or (p / "requirements.txt").exists():
            return p.resolve()
    return here.parents[2]


def _resolve_resource_root(project_root: FSPath) -> FSPath:
    """
    Resolve where runtime resources live (templates/static/logo).

    We keep templates/ and static/ at the PROJECT ROOT (repo root).
    This function simply makes that robust across different run contexts
    (Docker, systemd, CI) without changing folder paths.

    Priority:
      1) Frozen builds: sys._MEIPASS
      2) Explicit override: CTC_RESOURCE_ROOT
      3) Repo root if it contains templates/ and static/
      4) Current working directory if it contains templates/ and static/
      5) Fallback: project_root
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return FSPath(meipass).resolve()

    env_root = (os.getenv("CTC_RESOURCE_ROOT") or "").strip()
    if env_root:
        return FSPath(env_root).resolve()

    if (project_root / "templates").exists() and (project_root / "static").exists():
        return project_root

    cwd = FSPath.cwd()
    if (cwd / "templates").exists() and (cwd / "static").exists():
        return cwd.resolve()

    return project_root


PROJECT_ROOT = _resolve_project_root()
RESOURCE_ROOT = _resolve_resource_root(PROJECT_ROOT)
if not (RESOURCE_ROOT / "templates").exists():
    get_logger("app").warning(
        f"Templates directory not found at {RESOURCE_ROOT / 'templates'}. "
        "Ensure templates/ exists at the project root or set CTC_RESOURCE_ROOT."
    )
if not (RESOURCE_ROOT / "static").exists():
    get_logger("app").warning(
        f"Static directory not found at {RESOURCE_ROOT / 'static'}. "
        "Ensure static/ exists at the project root or set CTC_RESOURCE_ROOT."
    )

# Resource-only folders (bundled into the EXE)
AUTOMATION = RESOURCE_ROOT / "automation"

# Writable/script/log folders (dev repo; or EXE folder)
GIT_SCRIPT = (PROJECT_ROOT / "automation" / "git_auto_push.ps1")
LOG_DIR = (PROJECT_ROOT / "automation" / "logs")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Async lifespan context used by FastAPI to handle startup/shutdown events.
    Performs idempotent DB checks, initializes logs, and records startup diagnostics.
    """
    log_dir = PROJECT_ROOT / "logs" / "app"
    log_dir.mkdir(parents=True, exist_ok=True)
    for p in sorted(log_dir.glob("*.log"))[:-10]:  # keep last 10 logs
        try:
            p.unlink()
        except Exception:
            pass
    logger = get_logger("app")
    start_ts = _now_iso_z()
    diag = {"timestamp": start_ts, "status": "starting"}

    try:
        setup_logging(enable_console=True)
        integrate_uvicorn_logs()
        init_db(engine)
        Base.metadata.create_all(bind=engine)

        # Verify that core tables exist before serving requests
        required_tables = {"transactions", "calc_runs", "realized_events", "fx_rates"}
        with engine.connect() as conn:
            existing = {r[0] for r in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))}
        missing = sorted(required_tables - existing)
        diag["missing_tables"] = missing
        if missing:
            logger.warning(f"Missing tables detected: {missing}")
        else:
            logger.info("All core tables verified present.")
        
        # Run existing startup sequence
        if hasattr(sys.modules[__name__], "on_startup"):
            on_startup()

        diag.update({
            "status": "ok",
            "python_version": platform.python_version(),
            "platform": platform.system(),
            "db_url": str(engine.url),
        })
        logger.info(f"Startup completed successfully at {start_ts}")

    except Exception as e:
        diag.update({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
        })
        logger.warning(f"Startup failed: {e}")
        meta_dir = PROJECT_ROOT / "support_bundles" / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        (meta_dir / "startup_error.txt").write_text(
            f"[{start_ts}] Startup failed:\n{traceback.format_exc()}",
            encoding="utf-8"
        )
        raise
    finally:
        # Always write JSON diagnostics
        try:
            log_dir = PROJECT_ROOT / "logs" / "app"
            log_dir.mkdir(parents=True, exist_ok=True)
            _atomic_write_json(log_dir / "startup.json", diag)
        except Exception:
            pass

    yield

    try:
        deps = {
            "timestamp": _now_iso_z(),
            "graph": {
                "app.py": ["csv_normalizer.py","calc_runner.py","fifo_engine.py","fx_utils.py","exporter.py","report_pdf.py","audit_digest.py","audit_utils.py","models.py","schemas.py","db.py"],
                "calc_runner.py": ["fifo_engine.py","schemas.py","models.py","rules.hr","rules.it"],
                "rules.hr": ["rules.base","models.py"],
                "rules.it": ["rules.base","models.py"],
                "rules.base": ["models.py","schemas.py"],
                "csv_normalizer.py": ["schemas.py"],
                "fifo_engine.py": [],
                "fx_utils.py": ["db.py"],
                "audit_utils.py": ["db.py"],
                "audit_digest.py": ["db.py"],
                "exporter.py": ["logging_setup.py"],
                "report_pdf.py": ["logging_setup.py"],
                "models.py": [],
                "schemas.py": [],
                "db.py": ["logging_setup.py"],
            }
        }
        (PROJECT_ROOT / "logs" / "consistency").mkdir(parents=True, exist_ok=True)
        _atomic_write_json(PROJECT_ROOT / "logs" / "consistency" / "deps.json", deps)
    except Exception:
        pass
    
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


# Load .env early so runtime configuration (tokens, flags) is available (best-effort).
# In production deployments, prefer supplying environment variables via the process manager
# rather than relying on a local .env file.
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=str((PROJECT_ROOT / ".env").resolve()), override=False)
except Exception:
    pass


def _truthy_env(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


# Runtime environment mode (used to harden dangerous endpoints in production).
CTC_ENV = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
IS_PROD = CTC_ENV in {"prod", "production"}

# Security feature flags (prod defaults are restrictive).
ENABLE_ADMIN_ENDPOINTS = _truthy_env(os.getenv("ENABLE_ADMIN_ENDPOINTS")) if IS_PROD else True
ENABLE_ADMIN_SCRIPTS = _truthy_env(os.getenv("ENABLE_ADMIN_SCRIPTS")) if IS_PROD else True
ALLOW_QUERY_TOKENS = _truthy_env(os.getenv("ALLOW_QUERY_TOKENS")) if IS_PROD else True

# Admin tokens (MUST be set explicitly in production; defaults are dev-only).
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "dev-token-change-me").strip()
BUNDLE_TOKEN = (os.getenv("BUNDLE_TOKEN") or "").strip()

# Upload safety caps (prevent preview OOM and accidental huge uploads).
MAX_PREVIEW_BYTES = int(os.getenv("MAX_PREVIEW_BYTES") or str(5 * 1024 * 1024))    # 5MB
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES") or str(50 * 1024 * 1024))     # 50MB

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def migrate_fx_rates_schema(engine: Engine) -> None:
    """
    Backward-compatible wrapper.

    Single source of truth: db_migrations.ensure_fx_schema().
    """
    from .db_migrations import ensure_fx_schema
    ensure_fx_schema(engine)


def _ensure_calc_runs_has_digest_columns():
    """Add digest columns to calc_runs if they are missing (SQLite-safe, idempotent)."""
    with engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(calc_runs)")).fetchall()
        existing = {c[1] for c in cols}  # column names

        if "input_hash" not in existing:
            conn.execute(text("ALTER TABLE calc_runs ADD COLUMN input_hash TEXT"))
        if "output_hash" not in existing:
            conn.execute(text("ALTER TABLE calc_runs ADD COLUMN output_hash TEXT"))
        if "manifest_hash" not in existing:
            conn.execute(text("ALTER TABLE calc_runs ADD COLUMN manifest_hash TEXT"))


def _ensure_calc_runs_has_tax_year_column():
    """Add tax_year column to calc_runs if it's missing (SQLite-safe, idempotent)."""
    with engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(calc_runs)")).fetchall()
        existing = {c[1] for c in cols}
        if "tax_year" not in existing:
            conn.execute(text("ALTER TABLE calc_runs ADD COLUMN tax_year INTEGER"))
            

def _ensure_calc_runs_has_summary_json():
    """Add summary_json column to calc_runs if it's missing (SQLite-safe, idempotent)."""
    with engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(calc_runs)")).fetchall()
        existing = {c[1] for c in cols}
        if "summary_json" not in existing:
            conn.execute(text("ALTER TABLE calc_runs ADD COLUMN summary_json TEXT"))


def _iso_utc(dt: datetime | None) -> str:
    """Return dt as ISO8601 Zulu (UTC) string; fallback to now if None."""
    if dt is None:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_manifest(session: Session, run_db_id: int, run_uuid: str) -> dict:
    """
    Build a manifest for a run (used by /api/v1/runs/{run_id}).

    Includes:
      - core run metadata
      - trust signals (strict_fx, warnings_count, FX fallback)
      - performance timings (timings_ms) when available
    """
    row = session.execute(
        select(
            CalcRun.id,
            CalcRun.started_at,
            CalcRun.finished_at,
            CalcRun.jurisdiction,
            CalcRun.rule_version,
            CalcRun.lot_method,
            CalcRun.fx_set_id,
            CalcRun.params_json,
            CalcRun.summary_json,
            CalcRun.run_id,
        ).where(CalcRun.id == run_db_id)
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"calc run not found: id={run_db_id}")

    # Prefer run_digests.created_at if present, else calc_runs.started_at
    digest_row = session.execute(
        select(RunDigest.created_at).where(RunDigest.run_id == run_db_id)
    ).first()

    if digest_row and digest_row[0]:
        try:
            created_at = _iso_utc(datetime.fromisoformat(str(digest_row[0]).replace("Z", "+00:00")))
        except Exception:
            created_at = str(digest_row[0])
    else:
        created_at = _iso_utc(row.started_at)

    manifest = {
        "id": int(row.id),
        "run_id": run_uuid,
        "created_at": created_at,
        "jurisdiction": row.jurisdiction,
        "rule_version": row.rule_version,
        "lot_method": row.lot_method,
        "fx_set_id": row.fx_set_id,
        "finished_at": _iso_utc(row.finished_at) if getattr(row, "finished_at", None) else None,
    }

    # Trust signal: strict_fx (from params_json)
    try:
        params = getattr(row, "params_json", None)
        if isinstance(params, str):
            params = json.loads(params)
        if isinstance(params, dict) and "strict_fx" in params:
            manifest["strict_fx"] = bool(params.get("strict_fx"))
    except Exception:
        pass

    # Trust signals + performance timings (from summary_json)
    try:
        summary = getattr(row, "summary_json", None)
        if isinstance(summary, str):
            summary = json.loads(summary)

        if isinstance(summary, dict):
            w = summary.get("warnings") or []
            if isinstance(w, list):
                manifest["warnings_count"] = len([x for x in w if x])

            fx_used = summary.get("fx_fallback_used", None)
            if isinstance(fx_used, bool):
                manifest["fx_fallback_used"] = fx_used

            fx_days = summary.get("fx_fallback_days_count", None)
            if fx_days is not None:
                try:
                    manifest["fx_fallback_days_count"] = int(fx_days)
                except Exception:
                    pass

            tm = summary.get("timings_ms", None)
            if isinstance(tm, dict):
                manifest["timings_ms"] = tm
    except Exception:
        pass

    # inputs snapshot count (run_inputs)
    try:
        n_inputs = session.execute(
            text("SELECT COUNT(*) FROM run_inputs WHERE run_id = :rid"),
            {"rid": run_db_id},
        ).scalar()
        manifest["inputs_snapshot_count"] = int(n_inputs or 0)
    except Exception:
        pass

    # items_count (realized events)
    try:
        items_count = session.execute(
            text("SELECT COUNT(*) FROM realized_events WHERE run_id = :rid"),
            {"rid": run_db_id},
        ).scalar() or 0
        manifest["items_count"] = int(items_count)
    except Exception:
        pass

    # outputs_hash (from run_digests)
    try:
        out = session.execute(
            select(RunDigest.output_hash)
            .where(RunDigest.run_id == run_db_id)
            .order_by(RunDigest.id.desc())
            .limit(1)
        ).first()
        if out and out[0]:
            manifest["outputs_hash"] = str(out[0])
    except Exception:
        pass

    return {k: v for k, v in manifest.items() if v is not None}


def get_session() -> Iterator[SASession]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _try_import(modname: str):
    try:
        return __import__(modname)
    except Exception:
        return None


def _which(exe: str) -> Optional[str]:
    from shutil import which as _which
    return _which(exe)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def _admin_not_found() -> None:
    # 404 reduces endpoint discovery in production
    raise HTTPException(status_code=404, detail="Not found")


def _resolve_supplied_token(
    *,
    x_admin_token: str | None,
    x_token: str | None,
    authorization: str | None,
    query_token: str | None,
) -> str:
    bearer = _extract_bearer_token(authorization)
    if bearer:
        return bearer
    if x_admin_token:
        return x_admin_token
    if x_token:
        return x_token

    # Query-string tokens leak via browser history, reverse-proxy logs and error traces.
    # We keep them only for developer convenience and disable them by default in production.
    if query_token and (not IS_PROD or ALLOW_QUERY_TOKENS):
        return query_token

    return ""


def require_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use X-Admin-Token header"),
) -> None:
    # Feature flag gate (production-safe default: disabled unless explicitly enabled).
    if IS_PROD and not ENABLE_ADMIN_ENDPOINTS:
        _admin_not_found()

    supplied = _resolve_supplied_token(
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        query_token=token,
    )
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Hard fail if prod is misconfigured (prevents shipping with the dev token).
    if IS_PROD and (not ADMIN_TOKEN or ADMIN_TOKEN == "dev-token-change-me"):
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not configured for production")

    if supplied != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin_scripts(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use X-Admin-Token header"),
) -> None:
    require_admin(
        request=request,
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        token=token,
    )
    if IS_PROD and not ENABLE_ADMIN_SCRIPTS:
        _admin_not_found()


def require_bundle_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use X-Admin-Token header"),
) -> None:
    # Bundling runs local scripts and touches sensitive artifacts. Treat as scripts.
    if IS_PROD and not ENABLE_ADMIN_ENDPOINTS:
        _admin_not_found()
    if IS_PROD and not ENABLE_ADMIN_SCRIPTS:
        _admin_not_found()

    supplied = _resolve_supplied_token(
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        query_token=token,
    )
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    allowed = {t for t in (ADMIN_TOKEN, BUNDLE_TOKEN) if t}
    if IS_PROD and (not allowed or "dev-token-change-me" in allowed):
        raise HTTPException(status_code=500, detail="Admin token is not configured for production")

    if supplied not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _demo_allowed_here() -> bool:
    """
    Demo routes should exist only when DEMO_MODE is enabled.
    In production, they must also require ALLOW_DEMO_IN_PROD=1.

    Psychology: demo-only surfaces in production reduce perceived seriousness and trust.
    """
    if not is_demo_mode_enabled():
        return False
    if not IS_PROD:
        return True
    return _truthy_env(os.getenv("ALLOW_DEMO_IN_PROD"))


UPLOAD_BLOBS_DIR = PROJECT_ROOT / "storage_raw" / "uploads"
UPLOAD_BLOBS_DIR.mkdir(parents=True, exist_ok=True)

def _safe_upload_filename(name: str) -> str:
    # Keep only a safe basename; replace weird chars
    base = (name or "").strip()
    try:
        base = Path(base).name
    except Exception:
        pass
    if not base:
        base = "upload.csv"

    out = []
    for ch in base:
        if ch.isalnum() or ch in "._-":
            out.append(ch)
        else:
            out.append("_")
    cleaned = "".join(out).strip("_")
    return cleaned or "upload.csv"


async def _persist_uploaded_file_stream(
    upload: UploadFile,
    filename: str,
    max_bytes: int | None = None,
) -> Tuple[str, str, int]:
    """
    Stream an uploaded file to disk while computing SHA-256.

    Returns: (blob_path, sha256_hex, byte_len)
    """
    safe_name = _safe_upload_filename(filename or upload.filename or "upload.csv")
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    uid = uuid4().hex[:8]
    out_path = UPLOAD_BLOBS_DIR / f"{ts}_{uid}_{safe_name}"

    h = hashlib.sha256()
    total = 0

    # Hard cap to prevent accidental huge uploads (also protects server memory/disk).
    # Psychology: prevents timeouts/freeze that users interpret as data loss.
    limit = int(max_bytes) if max_bytes is not None else int(MAX_UPLOAD_BYTES)
    if limit <= 0:
        limit = int(MAX_UPLOAD_BYTES)

    await upload.seek(0)
    with open(out_path, "wb") as f:
        while True:
            chunk = await upload.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            f.write(chunk)
            h.update(chunk)
            total += len(chunk)
            if total > limit:
                # Clean up partial file to avoid filling disk with aborted uploads.
                try:
                    f.close()
                except Exception:
                    pass
                try:
                    out_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large (> {limit} bytes). Split the dataset into smaller CSVs.",
                )
    await upload.seek(0)

    return str(out_path), h.hexdigest(), int(total)


def _safe_json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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


# --- Support-bundle helpers ---------------------------------------------------

SUPPORT_BUNDLES_DIR = PROJECT_ROOT / "support_bundles"
# Prefer Python bundle script; keep legacy env var name for backward compatibility.
BUNDLE_SCRIPT = (
    os.getenv("AUTOMATION_BUNDLE_SCRIPT")
    or os.getenv("AUTOMATION_BUNDLE_PS")  # legacy name
    or r"automation\collect_support_bundle.py"
)


def _abs_script_path() -> str:
    p = FSPath(BUNDLE_SCRIPT)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return str(p.resolve())


def _latest_zip_path() -> str | None:
    zips = list((SUPPORT_BUNDLES_DIR).glob("support_bundle_*.zip"))
    if not zips:
        return None
    zips.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(zips[0])


def _latest_bundle_dir() -> Optional[str]:
    try:
        paths = [
            p for p in SUPPORT_BUNDLES_DIR.iterdir()
            if p.is_dir() and p.name.startswith("bundle_")
        ]
        if not paths:
            return None
        return str(max(paths, key=lambda p: p.stat().st_mtime))
    except Exception:
        return None


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


def _save_calc_run_json(payload: dict) -> str:
    """
    Writes one calc run to a JSON file and returns the run_id (filename stem).
    """
    run_id = payload.get("run_id") or str(uuid.uuid4())
    payload["run_id"] = run_id

    # Write a compact JSON to reduce footprint.
    out_path = CALC_HISTORY_DIR / f"{run_id}.json"
    out_path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=_json_default), encoding="utf-8")
    return run_id


def _list_calc_runs_meta() -> list[dict]:
    """
    Returns basic metadata for all stored calc runs (id, created_at, counts, etc.).
    """
    items = []
    for p in sorted(CALC_HISTORY_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        items.append({
            "run_id": data.get("run_id", p.stem),
            "created_at": data.get("created_at"),
            "events_count": (len(data.get("events", [])) if isinstance(data.get("events"), list) else None),
            "inputs_hash": data.get("inputs_hash"),
            "outputs_hash": data.get("outputs_hash"),
            "manifest": data.get("manifest"),
        })
    # newest first
    items.sort(key=lambda d: (d.get("created_at") or ""), reverse=True)
    return items


def _load_calc_run(run_id: str) -> dict | None:
    path = CALC_HISTORY_DIR / f"{run_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _delete_calc_run(run_id: str) -> bool:
    path = CALC_HISTORY_DIR / f"{run_id}.json"
    if path.exists():
        path.unlink()
        return True
    return False


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_jsonable(obj):
    # primitives
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    # Decimal → str (keeps precision), or float if you prefer
    if isinstance(obj, Decimal):
        return str(obj)

    # dates / datetimes → ISO
    if isinstance(obj, (date, datetime)):
        # ensure naive datetimes don’t crash
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    # dataclasses
    if is_dataclass(obj):
        return {k: _to_jsonable(v) for k, v in asdict(obj).items()}

    # dict-like
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}

    # list / tuple / set
    if isinstance(obj, (list, tuple, set)):
        return [_to_jsonable(x) for x in obj]

    # generic python objects (fall back to __dict__)
    if hasattr(obj, "__dict__"):
        return {k: _to_jsonable(v) for k, v in vars(obj).items()}

    # last resort: string
    return str(obj)


def _json_default(obj):
    # Use strings for exactness (no float rounding)
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, set):
        return list(obj)
    # Let json raise for anything unexpected (helps catch bugs)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

# --- JSON + hashing helpers (safe for Decimal, UUID, dataclasses, etc.) ---

def _hash_text(s: str) -> str:
    """Return hex SHA256 of an input string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _to_plain_data(obj):
    """
    Recursively convert objects to JSON-serializable structures:
    - Decimal -> float
    - set/tuple -> list
    - dataclass / pydantic / attr / arbitrary objects -> dict via __dict__
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Decimal):
        # keep 2 dp if you prefer fixed precision: float(Decimal) is fine for API outputs
        return float(obj)
    if isinstance(obj, dict):
        return {k: _to_plain_data(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_plain_data(v) for v in obj]
    # Try pydantic model
    if hasattr(obj, "model_dump"):
        return _to_plain_data(obj.model_dump())
    # Try dataclass-like / simple objects
    if hasattr(obj, "__dict__"):
        return _to_plain_data(vars(obj))
    # Fallback to string
    return str(obj)


def _decimal_to_float(o):
    if isinstance(o, Decimal):
        return float(o)
    return o


def json_dumps(data) -> str:
    # compact, ASCII-safe JSON with Decimal handling
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False, default=_decimal_to_float)


def _match_to_dict(ev, m) -> Dict[str, Any]:
    """
    Normalize a single lot match into a JSON-friendly dict, including
    acquisition and disposal timestamps for holding-period logic.
    """
    from datetime import datetime, date

    # Acquisition timestamp from the original lot (if present)
    acquired_raw = getattr(m, "acquired_at", None)
    acquired_at: Optional[str]
    if isinstance(acquired_raw, (datetime, date)):
        acquired_at = acquired_raw.isoformat()
    elif acquired_raw is not None:
        acquired_at = str(acquired_raw)
    else:
        acquired_at = None

    # Disposal timestamp = the realization's timestamp
    disposed_raw = getattr(ev, "timestamp", None)
    disposed_at: Optional[str]
    if isinstance(disposed_raw, (datetime, date)):
        disposed_at = disposed_raw.isoformat()
    elif disposed_raw is not None:
        disposed_at = str(disposed_raw)
    else:
        disposed_at = None

    return {
        "from_qty":          dec_to_str(getattr(m, "from_qty", Decimal("0"))),
        "lot_cost_per_unit": dec_to_str(getattr(m, "lot_cost_per_unit", Decimal("0"))),
        "lot_cost_total":    dec_to_str(getattr(m, "lot_cost_total", Decimal("0"))),
        "acquired_at":       acquired_at,
        "disposed_at":       disposed_at,
    }


def ev_to_dict(ev) -> Dict[str, Any]:
    # All numbers as strings -> avoids Decimal issues and keeps precision
    return {
        "timestamp": getattr(ev, "timestamp", None),
        "asset": getattr(ev, "asset", None),
        "qty_sold": dec_to_str(getattr(ev, "qty_sold", Decimal("0"))),
        "proceeds": dec_to_str(getattr(ev, "proceeds", Decimal("0"))),
        "cost_basis": dec_to_str(getattr(ev, "cost_basis", Decimal("0"))),
        "gain": dec_to_str(getattr(ev, "gain", Decimal("0"))),
        "quote_asset": getattr(ev, "quote_asset", None),
        "fee_applied": dec_to_str(getattr(ev, "fee_applied", Decimal("0"))),
        "matches": [
            _match_to_dict(ev, m)
            for m in getattr(ev, "matches", []) or []
        ],
    }


class _RealizedEventSampler:
    """
    Deterministic, audit-friendly sampler for realised events.

    Goals:
      - Stable output for the same run/scope (seeded)
      - Includes top gains and top losses
      - Reservoir sample provides broad coverage across time
      - Deduplicates identical rows (timestamp/asset/qty/proceeds/cost/gain/quote)
    """
    def __init__(self, seed_text: str, max_rows: int = 50, top_k: int = 6):
        import random
        self._max_rows = max(0, int(max_rows))
        self._top_k = max(0, int(top_k))

        seed_hex = hashlib.sha256((seed_text or "seed").encode("utf-8")).hexdigest()[:8]
        self._rng = random.Random(int(seed_hex, 16))

        self._n = 0
        self._seen: set[tuple[str, ...]] = set()
        self._reservoir: list[RealizedEvent] = []
        self._top_gains: list[tuple[float, int, RealizedEvent]] = []
        self._top_losses: list[tuple[float, int, RealizedEvent]] = []

    def _key(self, e: RealizedEvent) -> tuple[str, str, str, str, str, str, str, str]:
        def _q(v: Any, quantum: Decimal) -> str:
            try:
                return str(Decimal(str(v)).quantize(quantum))
            except Exception:
                return str(v or "")

        ts = str(getattr(e, "timestamp", "") or "")
        asset = str((getattr(e, "asset", "") or "")).upper()
        quote = str((getattr(e, "quote_asset", "") or "")).upper()

        qty = _q(getattr(e, "qty_sold", None), Decimal("0.00000001"))
        proceeds = _q(getattr(e, "proceeds", None), Decimal("0.01"))
        cost = _q(getattr(e, "cost_basis", None), Decimal("0.01"))
        gain = _q(getattr(e, "gain", None), Decimal("0.01"))
        fee = _q(getattr(e, "fee_applied", None), Decimal("0.01"))

        return (ts, asset, qty, proceeds, cost, gain, quote, fee)

    def offer(self, e: RealizedEvent) -> None:
        key = self._key(e)
        if key in self._seen:
            return
        self._seen.add(key)
        self._n += 1

        try:
            g = float(str(getattr(e, "gain", 0) or 0))
        except Exception:
            g = 0.0

        import heapq

        # Keep extremes
        if self._top_k > 0:
            if g > 0:
                heapq.heappush(self._top_gains, (g, self._n, e))
                if len(self._top_gains) > self._top_k:
                    heapq.heappop(self._top_gains)
            elif g < 0:
                score = -g  # larger score = bigger loss magnitude
                heapq.heappush(self._top_losses, (score, self._n, e))
                if len(self._top_losses) > self._top_k:
                    heapq.heappop(self._top_losses)

        # Reservoir sample for broad coverage
        if self._max_rows <= 0:
            return
        if len(self._reservoir) < self._max_rows:
            self._reservoir.append(e)
        else:
            j = self._rng.randint(0, self._n - 1)
            if j < self._max_rows:
                self._reservoir[j] = e

    def finalize(self) -> list[RealizedEvent]:
        picked: list[RealizedEvent] = []
        used: set[tuple[str, ...]] = set()

        def _add(e: RealizedEvent) -> None:
            k = self._key(e)
            if k in used:
                return
            used.add(k)
            picked.append(e)

        for _g, _i, e in sorted(self._top_gains, key=lambda t: t[0], reverse=True):
            _add(e)
        for _s, _i, e in sorted(self._top_losses, key=lambda t: t[0], reverse=True):
            _add(e)
        for e in self._reservoir:
            _add(e)

        picked = picked[: self._max_rows]

        try:
            picked.sort(key=lambda e: (str(getattr(e, "timestamp", "") or ""), int(getattr(e, "id", 0) or 0)))
        except Exception:
            pass

        return picked


def _get_client_ip(request: Request) -> str:
    """Extract client IP address from FastAPI Request."""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        # If behind a proxy, X-Forwarded-For may contain multiple IPs
        ip = x_forwarded_for.split(",")[0].strip()
    else:
        ip = request.client.host if request.client else "unknown"
    return ip


def _resolve_db_run_id(session: Session, run_id: str) -> int:
    """
    Resolve an external run identifier to the internal integer PK.

    Supports:
      - CalcRun.run_id (UUID/string)
      - CalcRun.id (integer as string, e.g. "42")
    """
    # 1) Try to resolve as external UUID / string run_id
    stmt = (
        select(CalcRun.id)
        .where(CalcRun.run_id == run_id)
        .order_by(CalcRun.id.desc())
        .limit(1)
    )
    rid = session.execute(stmt).scalar_one_or_none()
    if rid is not None:
        return int(rid)

    # 2) Fallback: try to interpret as numeric primary key
    try:
        pk = int(run_id)
    except Exception:
        raise HTTPException(status_code=404, detail=f"run_id not found: {run_id}")

    rid = session.execute(
        select(CalcRun.id).where(CalcRun.id == pk).limit(1)
    ).scalar_one_or_none()
    if rid is None:
        raise HTTPException(status_code=404, detail=f"run_id not found: {run_id}")
    return int(rid)


def _as_uuid_str(v) -> str | None:
    if v is None:
        return None
    try:
        return str(v)
    except Exception:
        return None
    

def _parse_uuid_maybe(s: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(s))
    except Exception:
        return None


class CalculateV2Request(BaseModel):
    jurisdiction: str = Field(default="HR")
    rule_version: str = Field(default="2025.1")
    tax_year: int = Field(default=2025)
    lot_method: Literal["FIFO"] = "FIFO"
    fx_source: Literal["HNB", "ECB"] = Field(default="HNB")
    holding_exemption_days: Optional[int] = Field(default=730)  # HR default; IT typically None
    it_threshold_eur: Optional[str] = Field(default=None)  # no default threshold for IT
    round_dp: int = Field(default=2)
    strict_fx: bool = Field(default=False)  # NEW: strict FX mode toggle
    include_tax_helpers: bool = Field(default=True)
    include_audit_appendix: bool = Field(default=True)


class CalculateV2Response(BaseModel):
    run_id: int
    summary: dict
    digests: Optional[dict] = None


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def _meta_dir() -> FSPath:
    # central place where we log export errors
    root = FSPath(__file__).resolve().parents[2]  # project root: <repo>/
    m = root / "support_bundles" / "_meta"
    m.mkdir(parents=True, exist_ok=True)
    return m


class ExportBundleRequest(BaseModel):
    include_history: bool = True
    include_db: bool = True
    include_logs: bool = True
    include_env_redacted: bool = True
    include_requirements: bool = True
    include_pyproject: bool = True
    include_git_meta: bool = True


_SQLITE_SUFFIXES = {".db", ".sqlite", ".sqlite3"}

def _find_sqlite_files(root: FSPath) -> list[FSPath]:
    """Return any SQLite-like files under `root`."""
    if not root.exists():
        return []
    return [
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in _SQLITE_SUFFIXES
    ]


def _inject_eur_summary(payload: dict) -> dict:
    """
    Ensure payload['summary']['eur_summary'] exists for exporters.
    If it's missing, derive it from summary.totals.{proceeds_eur,cost_eur,gain_eur}.
    Tolerant to both top-level and wrapped shapes.
    """
    if not isinstance(payload, dict):
        return payload

    summary = payload.get("summary", payload)
    if not isinstance(summary, dict):
        return payload

    # If eur_summary already present with totals_eur, do nothing
    es = summary.get("eur_summary")
    if isinstance(es, dict) and isinstance(es.get("totals_eur"), dict):
        return payload

    totals = summary.get("totals", {}) if isinstance(summary.get("totals"), dict) else {}
    def _D(v):
        try:
            return Decimal(str(v))
        except Exception:
            return Decimal("0")

    proceeds = _D(totals.get("proceeds_eur"))
    cost = _D(totals.get("cost_eur"))
    gain = _D(totals.get("gain_eur"))

    summary["eur_summary"] = {
        "totals_eur": {
            "proceeds": f"{proceeds.quantize(Decimal('0.01'))}",
            "cost_basis": f"{cost.quantize(Decimal('0.01'))}",
            "gain": f"{gain.quantize(Decimal('0.01'))}",
        },
        "notes": [
            "Derived from run totals (EUR).",
        ],
    }
    payload["summary"] = summary
    return payload


# -----------------------------------------------------------------------------
# Application factory & startup
# -----------------------------------------------------------------------------
app = FastAPI(
    title=__title__,
    version=__version__,
    lifespan=lifespan,
    description="Backend API for parsing crypto transactions and storing them safely.",
)


templates = Jinja2Templates(directory=str((RESOURCE_ROOT / "templates").resolve()))


# --- CORS (dev-friendly defaults; production requires explicit allowlist) ---
# Psychology: predictable security posture reduces perceived risk when users upload financial history.
_default_origins = [
    "http://127.0.0.1",
    "http://127.0.0.1:8000",
    "http://localhost",
    "http://localhost:8000",
]
# Preferred: CORS_ALLOW_ORIGINS="https://yourdomain.com,https://app.yourdomain.com"
# Backward-compat: DEMO_CORS_ORIGINS="http://127.0.0.1:3000,http://localhost:5173"
_env_origins = [
    o.strip()
    for o in (os.getenv("CORS_ALLOW_ORIGINS") or os.getenv("DEMO_CORS_ORIGINS") or "").split(",")
    if o.strip()
]

if IS_PROD:
    # In production, do not guess origins. If no allowlist is provided, disable CORS entirely.
    _allow_origins = _env_origins
else:
    _allow_origins = _env_origins or _default_origins


# Only add CORS middleware when we have an explicit allowlist.
# In production, empty allowlist means CORS is disabled (same-origin deployments do not need it).
if _allow_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add safe, low-friction security headers.

    Psychology: visible professionalism + reduced exploit surface improves user trust
    when handling sensitive financial history.
    """
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    return response


# --- Serve /logo as a static folder (White/Black_transparent.png live here) ---
from fastapi.staticfiles import StaticFiles

STATIC_DIR = (RESOURCE_ROOT / "static").resolve()
LOGO_DIR   = (RESOURCE_ROOT / "logo").resolve()


@app.get("/logo/icon", summary="Small mark icon for navbar (theme-aware)")
def logo_icon(theme: str = "dark"):
    """
    Returns the small circular mark. 
    theme=dark -> icon_white.png (for dark backgrounds)
    theme=light -> icon_black.png (for light backgrounds)
    """
    fname = "icon_white.png" if theme.lower() == "dark" else "icon_black.png"
    path = LOGO_DIR / fname
    if not path.exists():
        path = LOGO_DIR / "icon_white.png"
    return FileResponse(str(path), media_type="image/png")


# Mount /static for front-end assets (e.g., glow.js)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Mount /logo so /logo/White_transparent.png works
if LOGO_DIR.exists():
    app.mount("/logo", StaticFiles(directory=str(LOGO_DIR)), name="logo")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    import time
    logger = get_logger("app")
    start_time = time.time()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        process_time = round(time.time() - start_time, 3)
        logger.info(
            f"{request.method} {request.url.path} -> "
            f"{getattr(response, 'status_code', '?')} "
            f"in {process_time}s"
        )


router = APIRouter()


# --- History storage (file-based) ---
CALC_HISTORY_DIR = FSPath("storage_raw/calc_runs")
CALC_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


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
        # Transactions: import + filters
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions(timestamp)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_ts_id ON transactions(timestamp, id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_asset_ts ON transactions(base_asset, timestamp)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_type_ts ON transactions(type, timestamp)"))

        # Realized events: workspace filters and exports
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_realized_events_run_id ON realized_events(run_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_realized_events_run_asset_ts ON realized_events(run_id, asset, timestamp)"))

        # Runs: history filtering (jurisdiction/year)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_calc_runs_juris_year ON calc_runs(jurisdiction, tax_year)"))

        # Audit stability: run input snapshots
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_run_inputs_run ON run_inputs(run_id)"))


#"""@app.on_event("startup")
def on_startup() -> None:
    """
    Runs when the server starts.
    - Ensures database tables exist (idempotent).
    """
    init_db(engine)
    Base.metadata.create_all(bind=engine)
    
    # Ensure columns that may have been added later
    _ensure_transactions_has_fair_value_column()
    _ensure_calc_runs_has_digest_columns()
    _ensure_calc_runs_has_tax_year_column()
    _ensure_calc_runs_has_summary_json()

    # FX schema/migrations are enforced inside db.init_db(engine) (single source of truth).

    _ensure_indexes()
    _set_sqlite_pragmas()

    # Provide session factory to fx_utils for runtime helpers
    set_session_factory(SessionLocal)
        
    # Auto-bootstrap FX rates once from automation/fx_ecb.csv if fx_rates is empty
    _bootstrap_fx_from_csv_if_empty(engine)
        
    logger.info("Startup completed successfully at %s", datetime.now(timezone.utc).isoformat())

    # Call the schema assert you defined
    def _assert_schema():
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM sqlite_master WHERE type='table' AND name='calc_runs'")
            ).fetchone()
            if not exists:
                raise RuntimeError("Missing table 'calc_runs' — run DB init/migrations.")
    _assert_schema()


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


@app.get("/country_notes", summary="Jurisdiction notes (informational only)")
def country_notes(
    jurisdiction: str = Query("HR", pattern="^[A-Za-z]{2}$"),
) -> Dict[str, Any]:
    """
    Non-binding country notes used by the Workspace and exports.
    Returns a safe fallback for newly added jurisdictions.
    """
    j = (jurisdiction or "HR").strip().upper()

    base = {
        "icon_url": "/static/img/icons/country_notes.png",
        "disclaimer": (
            "These notes are informational only — not tax advice. "
            "Your obligations depend on local law and your personal circumstances."
        ),
        "jurisdiction": j,
    }

    if j == "HR":
        return {
            **base,
            "title": "Country Notes – Croatia",
            "subtitle": "High-level context only.",
            "bullets": [
                "Croatia: taxable crypto disposals are typically treated as capital gains (JOPPD context).",
                "Holding-period relief (e.g., 2+ years) can affect taxable vs exempt classification.",
                "This is a technical EUR summary; mapping into forms requires professional review.",
            ],
        }

    if j == "IT":
        return {
            **base,
            "title": "Country Notes – Italy",
            "subtitle": "High-level context only.",
            "bullets": [
                "Italy: crypto may involve Quadro RT (gains) and Quadro RW (holdings disclosure).",
                "Thresholds and obligations can depend on your situation; consult a commercialista.",
                "This is a technical EUR summary only; it does not replace Italian tax forms.",
            ],
        }

    if j == "XX":
        return {
            **base,
            "title": "Country Notes – XX",
            "subtitle": "Baseline / placeholder jurisdiction.",
            "bullets": [
                "XX is currently configured as a baseline rule for testing and jurisdiction onboarding.",
                "No country-specific exemptions or filing guidance are included yet.",
                "Treat outputs as a technical FIFO + FX summary only and validate obligations with a qualified professional.",
            ],
        }

    return {
        **base,
        "title": f"Country Notes – {j}",
        "subtitle": "High-level context only.",
        "bullets": [
            f"Country notes are not yet implemented for {j} in this build.",
            "This is a technical FIFO + FX summary only; consult local law and professional guidance for filing requirements.",
            "When adding a new jurisdiction, include fixtures + tests and then extend these notes for audit-friendly exports.",
        ],
    }


@app.get("/jurisdiction/status", summary="Jurisdiction readiness (informational only)")
def jurisdiction_status(
    jurisdiction: str = Query("HR", pattern="^[A-Za-z]{2}$"),
) -> Dict[str, Any]:
    """
    Returns a simple readiness status for the jurisdiction code.

    Readiness levels:
      - FULL: rule exists and country notes are specific (e.g., HR, IT)
      - BASELINE: rule exists but notes are generic/placeholder (e.g., XX or newly added jurisdictions)
      - MISSING: rule not registered
    """
    code = (jurisdiction or "HR").strip().upper()

    try:
        from cryptotaxcalc.rules.registry import supported_jurisdictions
        supported = supported_jurisdictions()
    except Exception:
        supported = []

    rule_registered = code in supported

    # Notes mode mirrors /country_notes logic:
    # HR/IT: specific; XX: baseline placeholder; others: generic fallback
    if code in {"HR", "IT"}:
        notes_mode = "specific"
    elif code == "XX":
        notes_mode = "baseline"
    else:
        notes_mode = "generic"

    if not rule_registered:
        readiness = "MISSING"
        message = "No rule module is registered for this jurisdiction in this build."
    else:
        if notes_mode == "specific":
            readiness = "FULL"
            message = "Rule module and jurisdiction notes are configured."
        else:
            readiness = "BASELINE"
            message = "Rule module exists, but jurisdiction guidance is minimal (technical summary only)."

    return {
        "jurisdiction": code,
        "readiness": readiness,
        "rule_registered": bool(rule_registered),
        "notes_mode": notes_mode,
        "supported": supported,
        "message": message,
    }


@app.get("/status")
def status():
    """
    Status endpoint used by the demo dashboard KPIs.
    Returns DB connectivity, demo flag, and latest run id if available.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        database = "connected"
    except Exception:
        database = "failed"

    # latest run id (int) if present
    try:
        with engine.begin() as conn:
            last_id = conn.execute(text("SELECT id FROM calc_runs ORDER BY id DESC LIMIT 1")).scalar()
    except Exception:
        last_id = None

    # demo flag: ask demo module if importable, else default False
    demo_enabled = False
    try:
        from cryptotaxcalc.demo_assets import is_demo_mode_enabled
        demo_enabled = bool(is_demo_mode_enabled())
    except Exception:
        pass

    return {
        "database": database,
        "demo_mode": demo_enabled,
        "last_run_id": last_id,
    }
    

@app.get("/fx/status", summary="Inspect rate data availability")
def fx_status(
    base: str = Query("USD", description="Base currency/asset (e.g., USD, BNB, ETH)"),
    quote: str = Query("EUR", description="Quote currency (e.g., EUR)"),
) -> Dict[str, Any]:
    """
    Lightweight diagnostics for fx_rates.

    Returns:
      - rows: total count in fx_rates (all pairs)
      - pair_rows: count for selected base/quote
      - latest_date: most recent date for the pair
      - latest_rate: rate for that date (quote per 1 base), if available
      - latest_batch_id: batch id for that latest row, if available
    """
    b = (base or "USD").strip().upper()
    q = (quote or "EUR").strip().upper()

    try:
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(1) FROM fx_rates")).scalar() or 0
            pair_total = conn.execute(
                text("SELECT COUNT(1) FROM fx_rates WHERE base=:b AND quote=:q"),
                {"b": b, "q": q},
            ).scalar() or 0
            row = conn.execute(
                text(
                    "SELECT date, rate, batch_id "
                    "FROM fx_rates "
                    "WHERE base=:b AND quote=:q "
                    "ORDER BY date DESC LIMIT 1"
                ),
                {"b": b, "q": q},
            ).fetchone()
    except Exception as e:
        return {
            "rows": 0,
            "pair_rows": 0,
            "has_data": False,
            "base": b,
            "quote": q,
            "latest_date": None,
            "latest_rate": None,
            "latest_batch_id": None,
            "error": str(e),
        }

    latest_date = None
    latest_rate = None
    latest_batch_id = None
    if row:
        latest_date, latest_rate, latest_batch_id = row[0], row[1], row[2]

    return {
        "rows": int(total),
        "pair_rows": int(pair_total),
        "has_data": bool(pair_total),
        "base": b,
        "quote": q,
        "latest_date": str(latest_date) if latest_date is not None else None,
        "latest_rate": float(latest_rate) if latest_rate is not None else None,
        "latest_batch_id": int(latest_batch_id) if latest_batch_id is not None else None,
    }

    
# -----------------------------------------------------------------------------
# CSV endpoints
# -----------------------------------------------------------------------------

@app.post("/demo/load", summary="Load demo dataset WITHOUT deleting run history")
def demo_load_dataset(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use X-Admin-Token header"),
):
    """
    Replace ONLY the dataset (transactions) with demo seeds.
    Do NOT delete calc_runs, run_digests, realized_events, or raw_events.
    This behaves identically to CSV upload but uses demo data.
    """
    if not _demo_allowed_here():
        _admin_not_found()

    # Production hardening: prevent public resets unless explicitly allowed.
    # Psychology: a stable demo experience builds trust; random resets feel like instability.
    if IS_PROD and not _truthy_env(os.getenv("DEMO_ALLOW_PUBLIC_RESET")):
        require_admin(
            request=request,
            x_admin_token=x_admin_token,
            x_token=x_token,
            authorization=authorization,
            token=token,
        )

    from cryptotaxcalc.demo_assets import _seed_rows  # use the REAL seeder you already have
    from cryptotaxcalc.models import TransactionRow

    with SessionLocal() as session:
        # 1) Delete ONLY the dataset
        session.query(TransactionRow).delete()
        session.commit()

        # 2) Insert demo rows using your existing deterministic seeding
        _seed_rows(session)  # THIS now seeds ADA/ETH buys + sells

        session.commit()

    return {"status": "ok", "message": "Demo dataset loaded without deleting run history."}


@app.post("/upload/csv", response_model=CSVPreviewResponse)
async def upload_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Accept a CSV upload, parse & validate it, and return a PREVIEW (no DB writes).

    Why preview? Users can see what's parsed and fix errors before saving.
    """
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    # Read a bounded amount to avoid OOM on preview.
    data = await file.read(MAX_PREVIEW_BYTES + 1)
    if len(data) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(data) > MAX_PREVIEW_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"File too large for preview (> {MAX_PREVIEW_BYTES} bytes). "
                "Use the Workspace import (multi-file) flow instead."
            ),
        )

    try:
        valid_rows, errors, meta = parse_csv_with_meta(data, filename=filename)
    except CSVFormatError as e:
        raise HTTPException(status_code=400, detail={"message": str(e), "csv_source": e.meta})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parser error: {e!s}")

    preview = [vr for vr in valid_rows[:5]]
    return {
        "filename": filename,
        "total_valid": len(valid_rows),
        "total_errors": len(errors),
        "preview_first_5": preview,
        "errors": errors[:5],
        **meta,
    }


def _csv_source_meta_to_dict(meta: Any) -> Dict[str, Any]:
    return {
        "recognized_source_id": getattr(meta, "source_id", None),
        "recognized_source_name": getattr(meta, "source_name", None),
        "recognized_source_status": getattr(meta, "status", None),
        "recognized_source_confidence": getattr(meta, "confidence", 0.0),
        "recognized_source_signature": getattr(meta, "signature", None),
    }


def _detect_csv_headers_from_sample(sample_bytes: bytes) -> Tuple[List[str], str | None, str | None]:
    text = sample_bytes.decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(text[:8192])
    except Exception:
        dialect = csv.excel

    rdr = csv.reader(io.StringIO(text), dialect=dialect)
    headers = next(rdr, [])
    headers = [str(h) for h in headers if h is not None]

    return headers, getattr(dialect, "delimiter", None), getattr(dialect, "quotechar", None)


@app.post("/csv/detect/multiple")
async def csv_detect_multiple(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    """
    Fast detection endpoint used by the Step-1 wizard UI.

    Reads only a small sample (header row) to determine:
      - supported / unsupported
      - recognized source name (if supported)
    Also records unknown structures into storage_raw/csv_sources/unsupported_structures.json
    """
    results: List[Dict[str, Any]] = []

    for f in files:
        filename = f.filename or "(no-name)"
        sample = await f.read(131072)  # 128KB is enough for headers + dialect sniff
        headers, delim, qchar = _detect_csv_headers_from_sample(sample)

        meta = detect_csv_source(headers=headers, filename=filename, delimiter=delim, quotechar=qchar)
        results.append({"filename": filename, **_csv_source_meta_to_dict(meta)})

    return {"results": results}


@app.get("/csv/sources")
def csv_sources() -> Dict[str, Any]:
    """
    Return supported CSV formats for the wizard UI.
    This avoids hardcoding supported formats in templates.
    """
    try:
        sources = list_supported_sources()
    except Exception:
        sources = []
    return {"sources": sources}


@app.get("/csv/sources/catalog")
def csv_sources_catalog() -> Dict[str, Any]:
    """
    Supported CSV formats with match details (headers + filename hints).
    Used by the wizard unsupported panel for “what to export”.
    """
    try:
        sources = list_supported_sources_catalog()
    except Exception:
        sources = []
    return {"sources": sources}


@app.get("/csv/formats", response_class=HTMLResponse, include_in_schema=False)
def csv_formats_page(request: Request):
    """
    User-facing catalog of supported CSV formats (required headers, optional headers, filename hints).
    """
    try:
        sources = list_supported_sources_catalog()
    except Exception:
        sources = []
    return templates.TemplateResponse(
        "csv_formats.html",
        {
            "request": request,
            "sources": sources,
        },
    )


@app.get("/admin/csv/unsupported", tags=["admin"])
def admin_csv_unsupported(
    limit: int = Query(200, ge=1, le=2000),
    _admin: None = Depends(require_admin),
) -> Dict[str, Any]:
    """
    Developer/ops endpoint: list unsupported CSV structures captured from users.
    """
    items = list_unsupported_signatures(limit=limit)
    return {"items": items, "limit": int(limit)}


@app.get(
    "/admin/csv/unsupported/ui",
    response_class=HTMLResponse,
    include_in_schema=False,
    tags=["admin"],
)
def admin_csv_unsupported_ui(
    request: Request,
    limit: int = Query(200, ge=1, le=2000),
    _admin: None = Depends(require_admin),
) -> HTMLResponse:
    """
    Admin UI: triage unsupported CSV structures captured from users.

    Note: If you rely on query-string tokens for this UI, you must enable them explicitly
    (ALLOW_QUERY_TOKENS=1) and enable admin endpoints.
    """
    items = list_unsupported_signatures(limit=limit)
    token = request.query_params.get("token")  # optional; used only to keep legacy UI links working
    return templates.TemplateResponse(
        "admin_csv_unsupported.html",
        {
            "request": request,
            "items": items,
            "limit": int(limit),
            "token": token,
        },
    )


class AdminRemoveUnsupportedSignatureRequest(BaseModel):
    signature: str


@app.post("/admin/csv/unsupported/remove", tags=["admin"])
def admin_csv_unsupported_remove(
    req: AdminRemoveUnsupportedSignatureRequest,
    _admin: None = Depends(require_admin),
) -> Dict[str, Any]:
    """
    Admin action: remove a signature from unsupported_structures.json
    (use after you implement the parser and add it to supported sources).
    """
    signature = (req.signature or "").strip()
    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    removed = remove_unsupported_signature(signature)
    return {"removed": bool(removed), "signature": signature}


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


@app.post("/import/multiple")
async def import_multiple(
    files: List[UploadFile] = File(...),
    reset: bool = Query(False, description="If true, clear existing transactions before import"),
):
    """
    Accept multiple CSV files in one request.
    For each file:
      - Store original file in raw_events (SHA-256, path)
      - Parse & validate
      - Insert with duplicate detection by hash
      - Link transactions.raw_event_id to the raw_events row

    When reset=true, the transactions table is cleared before import.
    """
    results = []
    global_min_year = None
    global_max_year = None

    # For workspace / paid flows we call /import/multiple?reset=1 so that
    # the new dataset replaces any previous uploads. In demo mode we keep
    # the same behavior as before.
    if reset or is_demo_mode_enabled():
        with SessionLocal() as session:
            session.query(TransactionRow).delete()
            session.commit()

    for file in files:
        filename = file.filename or "(no-name)"
        inserted = 0
        skipped_duplicates = 0
        skipped_errors = 0

        blob_path = None
        raw_event_id = None

        try:
            # Stream to disk (no full in-memory buffer) while computing SHA-256
            blob_path, digest, byte_len = await _persist_uploaded_file_stream(
                file,
                filename=filename,
                max_bytes=MAX_UPLOAD_BYTES,
            )
            if byte_len <= 0:
                results.append({"filename": filename, "error": "Empty file"})
                continue

            received_at = datetime.now().replace(microsecond=0).isoformat() + "Z"
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

            # Parse + detect source (streamed; avoids building a huge decoded string)
            await file.seek(0)
            text_stream = io.TextIOWrapper(file.file, encoding="utf-8-sig", errors="replace", newline="")
            try:
                valid_rows, parse_errors, csv_meta = parse_csv_stream_with_meta(text_stream, filename=filename)
            finally:
                # Prevent closing UploadFile's underlying handle
                try:
                    text_stream.detach()
                except Exception:
                    pass
            
            skipped_errors += len(parse_errors)
            
            # Detect year range in this file for smart defaults in the wizard
            file_min_year = None
            file_max_year = None
            for tx in valid_rows:
                try:
                    y = tx.timestamp.year
                except Exception:
                    continue
                if file_min_year is None or y < file_min_year:
                    file_min_year = y
                if file_max_year is None or y > file_max_year:
                    file_max_year = y

        except CSVFormatError as e:
            results.append({
                "filename": filename,
                "error": str(e),
                **(e.meta or {}),
            })
            continue

        except Exception as e:
            import traceback, hashlib
            # Create meta dir once
            meta_dir = (PROJECT_ROOT / "support_bundles" / "_meta")
            meta_dir.mkdir(parents=True, exist_ok=True)

            tb = traceback.format_exc()

            preview = b""
            try:
                if blob_path and os.path.exists(blob_path):
                    with open(blob_path, "rb") as f:
                        preview = f.read(1024)
            except Exception:
                preview = b""

            preview_digest = hashlib.sha256(preview).hexdigest()

            size_hint = 0
            try:
                if blob_path and os.path.exists(blob_path):
                    size_hint = os.path.getsize(blob_path)
            except Exception:
                size_hint = 0

            (meta_dir / "import_errors.log").write_text(
                f"[{_now_iso()}] file={filename} size={size_hint}\n"
                f"sha256_1kb={preview_digest}\n"
                f"{tb}\n"
                f"---\n",
                encoding="utf-8"
            )

            results.append({"filename": filename, "error": f"Failed to parse CSV: {e}"})
            continue

        with SessionLocal() as session:
            # Precompute tx hashes (fast; avoids per-row DB queries)
            tx_hashes: list[str] = []
            for tx in valid_rows:
                tx_hashes.append(compute_tx_hash(tx))

            # Load existing hashes from DB in SQLite-safe chunks (SQLite param limit)
            existing_hashes: set[str] = set()
            unique_hashes = sorted(set(tx_hashes))
            IN_CHUNK = 900  # keep under SQLite variable limit
            for i in range(0, len(unique_hashes), IN_CHUNK):
                chunk = unique_hashes[i:i + IN_CHUNK]
                if not chunk:
                    continue
                rows = session.execute(
                    select(TransactionRow.hash).where(TransactionRow.hash.in_(chunk))
                ).scalars().all()
                existing_hashes.update([h for h in rows if h])

            # Build rows to insert (also de-dupe within this upload)
            seen_in_file: set[str] = set()
            to_insert: list[dict] = []

            for tx, tx_hash in zip(valid_rows, tx_hashes):
                if tx_hash in existing_hashes or tx_hash in seen_in_file:
                    skipped_duplicates += 1
                    continue
                seen_in_file.add(tx_hash)

                to_insert.append({
                    "hash": tx_hash,
                    "timestamp": tx.timestamp,
                    "type": tx.type,
                    "base_asset": tx.base_asset,
                    "base_amount": str(tx.base_amount),
                    "quote_asset": tx.quote_asset,
                    "quote_amount": (str(tx.quote_amount) if tx.quote_amount is not None else None),
                    "fee_asset": tx.fee_asset,
                    "fee_amount": (str(tx.fee_amount) if tx.fee_amount is not None else None),
                    "exchange": tx.exchange,
                    "memo": tx.memo,
                    "fair_value": (str(tx.fair_value) if getattr(tx, "fair_value", None) is not None else None),
                    "raw_event_id": raw_event_id,
                })

            # Bulk insert in chunks (faster than bulk_save_objects; avoids ORM tracking)
            BULK_CHUNK = 1000
            for i in range(0, len(to_insert), BULK_CHUNK):
                session.bulk_insert_mappings(TransactionRow, to_insert[i:i + BULK_CHUNK])

            try:
                session.commit()
                inserted += len(to_insert)
            except IntegrityError:
                session.rollback()
                results.append({
                    "filename": filename,
                    "error": "Database integrity error (possibly duplicate hash).",
                })
                continue

        # Roll file-level years into global range
        if file_min_year is not None:
            if global_min_year is None or file_min_year < global_min_year:
                global_min_year = file_min_year
        if file_max_year is not None:
            if global_max_year is None or file_max_year > global_max_year:
                global_max_year = file_max_year

        results.append({
            "filename": filename,
            "inserted": inserted,
            "skipped_duplicates": skipped_duplicates,
            "skipped_errors": skipped_errors,
            "min_year": file_min_year,
            "max_year": file_max_year,
            **(csv_meta or {}),
        })

    return {
        "results": results,
        "meta": {
            "min_year": global_min_year,
            "max_year": global_max_year,
        },
    }


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
def calculate_fifo(request: Request) -> Dict[str, Any]:
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
    run_id = getattr(request.state, "run_id", None)
    if not run_id:
        run_id = str(uuid.uuid4())
        request.state.run_id = run_id

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

    _tx_inputs_view = [
        {
            "timestamp": str(getattr(t, "timestamp", "")),
            "base": getattr(t, "base_asset", None),
            "quote": getattr(t, "quote_asset", None),
            "side": getattr(t, "side", None),
            "quantity": str(getattr(t, "quantity", "")),
            "price": str(getattr(t, "price", "")),
            "fee": str(getattr(t, "fee", "")),
            "txid": getattr(t, "txid", None),
        }
        for t in tx_models
    ]
    inputs_hash = _hash_text(json.dumps(_tx_inputs_view, separators=(",", ":"), ensure_ascii=False))

    # Create calc_runs row (freeze metadata)
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rule_version = "2025.01.fifo.v1"
    jurisdiction = (request.query_params.get("jurisdiction") or "HR").upper()
    try:
        from cryptotaxcalc.rules.registry import get_rule
        get_rule(jurisdiction)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    lot_method = "FIFO"

    fx_set_id = get_or_create_current_fx_batch_id()

    params = {
        "rounding": "bankers",
        "tz_policy": "UTC",
        "fee_policy": "quote_fee_reduces_proceeds"
    }
    
    run_id_str = run_id

    with engine.begin() as conn:
        run_id_db = conn.execute(
            text("""
            INSERT INTO calc_runs (started_at, jurisdiction, rule_version, lot_method, fx_set_id, params_json, run_id)
            VALUES (:sa, :j, :rv, :lm, :fx, :pj, :rid)
            """),
            dict(
                sa=started_at, j=jurisdiction, rv=rule_version, lm=lot_method,
                fx=fx_set_id, pj=json.dumps(params, default=_json_default), rid=run_id_str
            )
        ).lastrowid

    int_run_id = int(run_id_db)        # integer PK
    request.state.run_id = run_id_str   # UUID for external references
    
    # --- Run FIFO and normalize outputs -----------------------------------------
    events_payload: list[dict] = []  # ensure it's always defined

    try:
        # compute realized events
        events, summary, warnings = compute_fifo(tx_models)

        # normalize events to plain dicts for stable hashing/JSON
        events_payload = [ev_to_dict(e) for e in events]

    except Exception as e:
        # mark the run as finished/errored and surface a clean 500
        import logging
        logging.getLogger("cryptotaxcalc.calc").exception("compute_fifo failed")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE calc_runs SET finished_at=:fa WHERE id=:rid"),
                dict(fa=datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z",
                rid=int_run_id)
            )
        raise HTTPException(status_code=500, detail=f"Calculation failed: {e}")

    # now it's safe to hash the outputs
    outputs_hash = _hash_text(json.dumps(events_payload, separators=(",", ":"), ensure_ascii=False))

    # persist a compact JSON snapshot of the run
    created_at = datetime.now().isoformat(timespec="seconds") + "Z"
    payload = {
        "run_id": run_id_str,
        "created_at": created_at,
        "inputs_hash": inputs_hash,
        "outputs_hash": outputs_hash,
        "manifest": {"params": {"lot_method": "FIFO"}},
        "events": events_payload,
    }
    _save_calc_run_json(payload)
    run_id = payload["run_id"]


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
                usd_per_eur = ensure_rate_or_default(session, ev_date)
                if usd_per_eur is None:
                    eur_notes.append(f"No EURUSD rate for {ev_date}; skipping conversion for event at {ev.timestamp}.")
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, ev_date, db=session)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, ev_date, db=session)
                eur_totals["gain"] += usd_to_eur(ev.gain, ev_date, db=session)
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
                    rid=int_run_id,
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
                    ], default=_json_default)
                )
            )
        finished_at = datetime.now().replace(microsecond=0).isoformat() + "Z"
        conn.execute(
            text("UPDATE calc_runs SET finished_at=:fa WHERE id=:rid"),
            dict(fa=finished_at, rid=int_run_id)
        )

    # --- NEW: build manifest + compute digests + persist in run_digests
    from .audit_digest import build_run_manifest, compute_digests
    manifest = build_run_manifest(int_run_id)  # run_id is a UUID string
    digests = compute_digests(manifest)

    created_at = datetime.now().replace(microsecond=0).isoformat() + "Z"

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
                rid=int_run_id,
                ih=digests["input_hash"],
                oh=digests["output_hash"],
                mh=digests["manifest_hash"],
                mj=json.dumps(manifest, default=_json_default),
                ts=created_at,
            ),
        )

    audit_meta = {
        "rule_version": rule_version,
        "fx_set_id": manifest["fx_batch"]["id"] if "fx_batch" in manifest else None,
        "run_id": run_id_str,
        "timestamp": started_at,
    }

    audit(
        actor="local-user",
        action="calc:run",
        target_type="calc_runs",
        target_id=int_run_id,
        details=audit_meta,
        ip=_get_client_ip(request),
    )

    # Persist in lightweight calc_audit table for quick lookup
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO calc_audit (run_id, actor, action, meta_json, created_at)
                VALUES (:rid, :actor, :action, :meta, :ts)
            """),
            dict(
                rid=int_run_id,
                actor="local-user",
                action="calc:run",
                meta=json.dumps(audit_meta, default=_json_default),
                ts=now_utc_iso(),
            ),
        )
    
    return {
        "run_id": run_id_str,
        "events": [ev_to_dict(e) for e in events],
        "summary": summary,
        "eur_summary": eur_summary,
        "warnings": warnings,
    }


@app.post("/calculate/v2", response_model=CalculateV2Response, tags=["calc"])
def calculate_v2(
    req: CalculateV2Request | None = Body(None),
    request: Request = None,
    db: Session = Depends(get_db),
    debug: bool = Query(False, description="Return extra diagnostics for debugging"),
    jurisdiction: str = Query("HR"),
):
    """
    Clean calculation endpoint:
      - Accepts either a JSON body OR no body (query param `jurisdiction`).
    """
    logger = logging.getLogger("cryptotaxcalc.calc")

    # If the dashboard calls us with no body, synthesize defaults:
    if req is None:
        req = CalculateV2Request(jurisdiction=jurisdiction)

    # Clear FX cache for consistent runs
    from cryptotaxcalc.fx_utils import clear_fx_cache
    clear_fx_cache()

    # Ensure tables exist (idempotent, cheap)
    Base.metadata.create_all(bind=db.get_bind())

    # Resolve / create FX batch id once for this run
    try:
        fx_batch_id = get_or_create_current_fx_batch_id()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"FX setup failed: {e}")

    # Build calc config from request (uses defaults if caller sent no body)
    cfg = CalcConfig(
        jurisdiction=(req.jurisdiction or "HR").upper(),
        rule_version=req.rule_version,
        lot_method=req.lot_method,
        fx_source=req.fx_source,
        holding_exemption_days=req.holding_exemption_days,
        it_threshold_eur=None if req.it_threshold_eur is None else req.it_threshold_eur,
        round_dp=req.round_dp,
        strict_fx=req.strict_fx,
        include_tax_helpers=req.include_tax_helpers,
        include_audit_appendix=req.include_audit_appendix,
    )

    # Validate jurisdiction (registry is the single source of truth)
    try:
        from cryptotaxcalc.rules.registry import get_rule
        get_rule(cfg.jurisdiction)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Create calc_runs record
    run = CalcRun(
        started_at=datetime.now(timezone.utc),
        jurisdiction=cfg.jurisdiction,
        rule_version=req.rule_version,     # ← unified: use req
        tax_year=req.tax_year,             # ← new field, correct
        lot_method=cfg.lot_method,
        fx_set_id=fx_batch_id,
        params_json=_safe_json_dumps(cfg.model_dump()),
        finished_at=None,
        run_id=str(uuid4()),
    )
    db.add(run); db.commit(); db.refresh(run)

    # Quick audit helper
    def _audit(action: str, details: dict | None = None):
        try:
            db.add(AuditLog(
                actor="local-user",
                action=action,
                target_type="calc_runs",
                target_id=run.id,
                details_json=_safe_json_dumps(details) if details else None,
                ip=(request.client.host if request and request.client else "n/a"),
                ts=datetime.now(timezone.utc),
            ))
            db.commit()
        except Exception:
            db.rollback()

    _audit("calc:start", {"fx_batch_id": fx_batch_id})

    try:
        # Run the actual calculation
        summary = run_calculation(db, run, cfg)
        # --- guard: some RunSummary versions don't expose `warnings`
        warnings = []
        if hasattr(summary, "warnings"):
            w = getattr(summary, "warnings") or []
            if isinstance(w, list):
                warnings = w
        run.finished_at = datetime.now(timezone.utc)
        db.add(run); db.commit()

        from .audit_digest import build_run_manifest, compute_digests
        manifest = build_run_manifest(run.id)
        digests = compute_digests(manifest)
        manifest_json = _safe_json_dumps(manifest)

        # Upsert into run_digests
        try:
            db.execute(text("""
                INSERT INTO run_digests (run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at)
                VALUES (:rid, :ih, :oh, :mh, :mj, :ts)
                ON CONFLICT(run_id) DO UPDATE SET
                    input_hash=excluded.input_hash,
                    output_hash=excluded.output_hash,
                    manifest_hash=excluded.manifest_hash,
                    manifest_json=excluded.manifest_json,
                    created_at=excluded.created_at
            """), dict(
                rid=run.id, ih=digests["input_hash"], oh=digests["output_hash"],
                mh=digests["manifest_hash"], mj=manifest_json, ts=datetime.now(timezone.utc),
            ))
            db.commit()
        except Exception:
            db.rollback()

        payload = CalculateV2Response(run_id=run.id, summary=summary.model_dump(), digests=digests)
        
        # Attach FX context and warnings for UI display
        payload.summary["fx_context"] = {
            "fx_batch_id": fx_batch_id,
            "fx_rate_used": getattr(summary, "fx_rate_used", None),
            "fx_fallback_used": getattr(summary, "fx_fallback_used", None),
            "fx_fallback_days_count": getattr(summary, "fx_fallback_days_count", None),
            "fx_fallback_days_sample": getattr(summary, "fx_fallback_days_sample", None),
            "jurisdiction": cfg.jurisdiction,
        }
        if warnings:
            payload.summary["warnings"] = summary.warnings
        
        if debug:
            payload.summary.setdefault("__debug", {})
            payload.summary["__debug"] = {
                "fx_batch_id": fx_batch_id,
                "cfg": cfg.model_dump(),
                "started_at": run.started_at,
                "finished_at": run.finished_at,
                "warnings": warnings,
            }
        return payload

    except HTTPException:
        logger.exception("Calculation failed with HTTPException (run_id=%s)", run.id)
        _audit("calc:error", {"detail": "HTTPException"}); raise
    except Exception as e:
        logger.exception("Calculation failed (run_id=%s).", getattr(run, "id", None))
        db.rollback(); _audit("calc:error", {"detail": str(e)})
        raise HTTPException(status_code=500, detail=f"Calculation failed: {e}")
    

@app.post("/api/v1/runs", response_model=CalculateV2Response, tags=["api", "calc"])
def api_create_run(
    req: CalculateV2Request | None = Body(None),
    request: Request = None,
    db: Session = Depends(get_db),
    debug: bool = Query(False, description="Return extra diagnostics for debugging"),
    jurisdiction: str = Query("HR"),
):
    """
    API-friendly wrapper around /calculate/v2.

    - Accepts the same JSON body as /calculate/v2 (CalculateV2Request)
      or falls back to `jurisdiction` query param when body is missing.
    - Creates a new calc run in the database.
    - Returns run_id (int PK), summary, and digests for integrations.
    """
    return calculate_v2(
        req=req,
        request=request,
        db=db,
        debug=debug,
        jurisdiction=jurisdiction,
    )


@app.get("/calc/run/{run_id}/events", response_class=JSONResponse, tags=["calc"])
def get_run_events(run_id: int, db: Session = Depends(get_db)):
    """
    Return realized events for a given calculation run as JSON-friendly dicts.
    All numbers (Decimal) are safely stringified.
    """
    rows = (
        db.query(RealizedEvent)
        .filter(RealizedEvent.run_id == run_id)
        .order_by(RealizedEvent.id.asc())
        .all()
    )

    events_payload = []
    for r in rows:
        events_payload.append({
            "timestamp": r.timestamp,
            "asset": r.asset,
            "side": "SELL",  # disposal events only
            "qty": str(r.qty_sold) if r.qty_sold is not None else None,
            "proceeds": str(r.proceeds) if r.proceeds is not None else None,
            "cost": str(r.cost_basis) if r.cost_basis is not None else None,
            "gain": str(r.gain) if r.gain is not None else None,
            "quote_asset": r.quote_asset,
            "fee_applied": str(r.fee_applied) if r.fee_applied is not None else None,
            "matches": json.loads(r.matches_json) if r.matches_json else None,
        })

    return JSONResponse({
        "run_id": run_id,
        "events": events_payload
    })


@app.get("/report/summary")
def report_summary(
    year: int,
    asset: str | None = None,
    quote_asset: str | None = None,
    show_tax_helpers: bool = True,
    show_audit_appendix: bool = True,
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
                rate = ensure_rate_or_default(session, dt.date())
                try:
                    proceeds_eur += (e.proceeds / rate)
                    basis_eur    += (e.cost_basis / rate)
                    gain_eur     += (e.gain / rate)
                except Exception:
                    notes.append(f"Bad EURUSD rate for {dt.date()}; skipped conversion for an event.")
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
async def fx_upload(file: UploadFile = File(...), _admin: None = Depends(require_admin)) -> dict:
    """
    Upload a CSV of daily EURUSD rates.
    Required headers: date, usd_per_eur
      - date format: YYYY-MM-DD
      - usd_per_eur: decimal number (USD per 1 EUR)
    We normalize and store as: base='USD', quote='EUR', rate=<EUR per 1 USD>.
    A new fx_batches row is created; imported rows get its batch_id.
    """
    # Basic validation
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    raw = await file.read()
    try:
        text_csv = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV (utf-8).")

    reader = DictReader(StringIO(text_csv))
    required = {"date", "usd_per_eur"}
    if not reader.fieldnames or required - {h.strip().lower() for h in reader.fieldnames}:
        raise HTTPException(status_code=400, detail="CSV must include headers: date, usd_per_eur")

    # Map canonical → actual header casing
    header_map = {h.strip().lower(): h for h in (reader.fieldnames or [])}

    inserted = 0
    updated = 0
    errors = 0

    now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    with SessionLocal() as session:
        # Make sure fx_rates has the columns this code relies on
        try:
            from .fx_utils import ensure_fx_rates_schema
            ensure_fx_rates_schema(session)
        except Exception:
            # Not fatal; continue and let the operations fail if schema is truly broken
            pass

        # Start a new batch
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": "ECB CSV", "h": None}
        ).lastrowid

        for row in reader:
            try:
                raw_date = (row.get(header_map["date"]) or "").strip()
                raw_rate = (row.get(header_map["usd_per_eur"]) or "").strip()

                if not raw_date or not raw_rate:
                    raise ValueError("Missing date or usd_per_eur")

                # Parse date
                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                d_iso = d.isoformat()

                # CSV gives USD per 1 EUR → normalize to EUR per 1 USD
                usd_per_eur = Decimal(raw_rate)  # may raise InvalidOperation
                if usd_per_eur <= 0:
                    raise ValueError("usd_per_eur must be > 0")

                rate_eur_per_usd = (Decimal("1") / usd_per_eur)

                # Upsert normalized row
                exists = session.execute(
                    text("SELECT 1 FROM fx_rates WHERE date = :d AND base='USD' AND quote='EUR'"),
                    {"d": d_iso}
                ).scalar()

                if exists:
                    session.execute(
                        text("""UPDATE fx_rates
                                SET rate = :r, batch_id = :b, base='USD', quote='EUR'
                                WHERE date = :d AND base='USD' AND quote='EUR'"""),
                        {"r": str(rate_eur_per_usd), "b": bid, "d": d_iso}
                    )
                    updated += 1
                else:
                    session.execute(
                        text("""INSERT INTO fx_rates (date, base, quote, rate, batch_id)
                                VALUES (:d, 'USD', 'EUR', :r, :b)"""),
                        {"d": d_iso, "r": str(rate_eur_per_usd), "b": bid}

                    )
                    inserted += 1

            except (InvalidOperation, ValueError):
                errors += 1
                continue
            except Exception:
                # Any unexpected row-level failure shouldn't kill the batch
                errors += 1
                continue

        session.commit()

        # Compute a deterministic hash of what was just imported (by date asc)
        rows_for_hash = session.execute(
            text("SELECT date, base, quote, rate FROM fx_rates WHERE batch_id = :b ORDER BY date"),
            {"b": bid}
        ).fetchall()

        h = hashlib.sha256()
        for r in rows_for_hash:
            # r = (date, base, quote, rate)
            line = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            {"rh": rates_hash, "bid": bid}
        )
        session.commit()

    return {"inserted": inserted, "updated": updated, "errors": errors, "batch_id": int(bid)}


@app.get("/prices/template.csv", summary="Download CSV template for daily price uploads", tags=["prices"])
def prices_template_csv() -> Response:
    """
    Template for /prices/upload.
    rate is QUOTE per 1 BASE (e.g., EUR per 1 BNB; USD per 1 ETH).
    """
    sample = (
        "date,base,quote,rate\n"
        "2025-01-01,BNB,EUR,250.12\n"
        "2025-01-01,ETH,USD,3450.50\n"
    )
    headers = {"Content-Disposition": 'attachment; filename="prices_template.csv"'}
    return Response(content=sample, media_type="text/csv; charset=utf-8", headers=headers)


@app.post("/prices/upload", summary="Upload daily prices for assets (used for third-asset fee valuation)", tags=["prices"])
async def prices_upload(
    file: UploadFile = File(...),
    source: str = Query("PRICE CSV", description="Label stored in fx_batches.source"),
    _admin: None = Depends(require_admin),
) -> dict:
    """
    Upload daily prices into fx_rates as base=<ASSET>, quote='EUR', rate=<EUR per 1 base>.

    CSV headers:
      - date: YYYY-MM-DD
      - base: asset symbol (e.g., BNB, ETH)
      - quote: EUR or USD-like (USD/USDT/USDC/BUSD). USD-like quotes are converted to EUR using fx_rates USD/EUR.
      - rate: quote per 1 base (e.g., EUR per 1 BNB; USD per 1 ETH)
    """
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    raw = await file.read()
    try:
        text_csv = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV (utf-8).")

    reader = DictReader(StringIO(text_csv))
    required = {"date", "base", "quote", "rate"}
    if not reader.fieldnames or required - {h.strip().lower() for h in reader.fieldnames}:
        raise HTTPException(status_code=400, detail="CSV must include headers: date, base, quote, rate")

    header_map = {h.strip().lower(): h for h in (reader.fieldnames or [])}

    inserted = 0
    updated = 0
    errors = 0
    fx_missing = 0
    fx_missing_days: set[str] = set()

    now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    USD_LIKE = {"USD", "USDT", "USDC", "BUSD"}

    with SessionLocal() as session:
        # Ensure fx_rates schema exists (idempotent)
        try:
            ensure_fx_rates_schema(session)
        except Exception:
            pass

        # Start a new batch for these rates/prices
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": (source or "PRICE CSV"), "h": None},
        ).lastrowid

        from datetime import timedelta

        for row in reader:
            try:
                raw_date = (row.get(header_map["date"]) or "").strip()
                raw_base = (row.get(header_map["base"]) or "").strip()
                raw_quote = (row.get(header_map["quote"]) or "").strip()
                raw_rate = (row.get(header_map["rate"]) or "").strip()

                if not raw_date or not raw_base or not raw_quote or not raw_rate:
                    raise ValueError("Missing required fields")

                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                d_iso = d.isoformat()

                base = raw_base.upper()
                quote = raw_quote.upper()

                rate_in_quote = Decimal(raw_rate)
                if rate_in_quote <= 0:
                    raise ValueError("rate must be > 0")

                # Normalize into EUR rates for downstream valuation.
                if quote == "EUR":
                    rate_eur = rate_in_quote
                elif quote in USD_LIKE:
                    # Convert quote (USD-like) -> EUR using USD->EUR rate for same day (allow up to 7-day lookback for weekends/holidays).
                    min_iso = (d - timedelta(days=7)).isoformat()
                    fx_row = session.execute(
                        text(
                            "SELECT rate FROM fx_rates "
                            "WHERE base='USD' AND quote='EUR' AND date <= :d AND date >= :min_d "
                            "ORDER BY date DESC LIMIT 1"
                        ),
                        {"d": d_iso, "min_d": min_iso},
                    ).first()
                    if not fx_row or fx_row[0] is None:
                        fx_missing += 1
                        fx_missing_days.add(d_iso)
                        raise ValueError("Missing USD->EUR FX rate for conversion")
                    eur_per_usd = Decimal(str(fx_row[0]))
                    rate_eur = (rate_in_quote * eur_per_usd)
                else:
                    raise ValueError(f"Unsupported quote '{quote}'. Use EUR or USD/USDT/USDC/BUSD.")

                # Upsert into fx_rates (base=<asset>, quote='EUR')
                exists = session.execute(
                    text("SELECT 1 FROM fx_rates WHERE date = :d AND base = :b AND quote = :q"),
                    {"d": d_iso, "b": base, "q": "EUR"},
                ).scalar()

                if exists:
                    session.execute(
                        text(
                            "UPDATE fx_rates SET rate = :r, batch_id = :bid "
                            "WHERE date = :d AND base = :b AND quote = :q"
                        ),
                        {"r": str(rate_eur), "bid": bid, "d": d_iso, "b": base, "q": "EUR"},
                    )
                    updated += 1
                else:
                    session.execute(
                        text(
                            "INSERT INTO fx_rates (date, base, quote, rate, batch_id) "
                            "VALUES (:d, :b, :q, :r, :bid)"
                        ),
                        {"d": d_iso, "b": base, "q": "EUR", "r": str(rate_eur), "bid": bid},
                    )
                    inserted += 1

            except (InvalidOperation, ValueError):
                errors += 1
                continue
            except Exception:
                errors += 1
                continue

        session.commit()

        # Hash the imported/updated rows for this batch
        rows_for_hash = session.execute(
            text("SELECT date, base, quote, rate FROM fx_rates WHERE batch_id = :b ORDER BY date, base, quote"),
            {"b": bid},
        ).fetchall()

        h = hashlib.sha256()
        for r in rows_for_hash:
            line = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            {"rh": rates_hash, "bid": bid},
        )
        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "errors": errors,
        "fx_missing": fx_missing,
        "fx_missing_days_count": len(fx_missing_days),
        "fx_missing_days_sample": sorted(list(fx_missing_days))[:10],
        "batch_id": int(bid),
    }


def _bootstrap_fx_from_csv_if_empty(engine: Engine) -> None:
    """
    One-shot FX bootstrap from automation/fx_ecb.csv.

    Behavior:
      - If fx_rates already has rows → do nothing
      - If automation/fx_ecb.csv is missing → do nothing
      - Otherwise: import it using the same semantics as /fx/upload
        (date, usd_per_eur → base='USD', quote='EUR', rate=EUR per 1 USD)
    """
    logger = get_logger("fx.bootstrap")

    # 1) Check if fx_rates already has data
    try:
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(1) FROM fx_rates")).scalar() or 0
        if total:
            logger.info("FX bootstrap skipped: fx_rates already has %s rows.", int(total))
            return
    except Exception as e:
        logger.warning("FX bootstrap: could not inspect fx_rates; skipping. %s", e)
        return

    # 2) Locate automation/fx_ecb.csv relative to project root
    csv_path = AUTOMATION / "fx_ecb.csv"
    if not csv_path.exists():
        logger.info("FX bootstrap: %s not found; nothing to import.", csv_path)
        return

    # 3) Read and parse CSV (same headers as /fx/upload)
    try:
        raw = csv_path.read_bytes()
        text_csv = raw.decode("utf-8-sig", errors="replace")
    except Exception as e:
        logger.warning("FX bootstrap: failed to read/ decode %s: %s", csv_path, e)
        return

    reader = DictReader(StringIO(text_csv))
    required = {"date", "usd_per_eur"}
    fieldnames = reader.fieldnames or []
    seen = {h.strip().lower() for h in fieldnames}
    if required - seen:
        logger.warning(
            "FX bootstrap: %s missing required headers %s; found %s. Skipping.",
            csv_path,
            required,
            fieldnames,
        )
        return

    header_map = {h.strip().lower(): h for h in fieldnames}

    inserted = 0
    updated = 0
    errors = 0
    now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    with SessionLocal() as session:
        # Ensure schema is ready (same helper fx_upload uses)
        try:
            from .fx_utils import ensure_fx_rates_schema
            ensure_fx_rates_schema(session)
        except Exception as e:
            logger.warning("FX bootstrap: ensure_fx_rates_schema failed (continuing): %s", e)

        # Start a new batch
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": "ECB CSV (bootstrap)", "h": None},
        ).lastrowid

        for row in reader:
            try:
                raw_date = (row.get(header_map["date"]) or "").strip()
                raw_rate = (row.get(header_map["usd_per_eur"]) or "").strip()

                if not raw_date or not raw_rate:
                    raise ValueError("Missing date or usd_per_eur")

                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                d_iso = d.isoformat()

                usd_per_eur = Decimal(raw_rate)
                if usd_per_eur <= 0:
                    raise ValueError("usd_per_eur must be > 0")

                # Same normalization as /fx/upload:
                # CSV gives USD per 1 EUR → store EUR per 1 USD
                rate_eur_per_usd = (Decimal("1") / usd_per_eur)

                exists = session.execute(
                    text(
                        "SELECT 1 FROM fx_rates "
                        "WHERE date = :d AND base='USD' AND quote='EUR'"
                    ),
                    {"d": d_iso},
                ).scalar()

                if exists:
                    session.execute(
                        text(
                            "UPDATE fx_rates "
                            "SET rate = :r, batch_id = :b, base='USD', quote='EUR' "
                            "WHERE date = :d AND base='USD' AND quote='EUR'"
                        ),
                        {"r": str(rate_eur_per_usd), "b": bid, "d": d_iso},
                    )
                    updated += 1
                else:
                    session.execute(
                        text(
                            "INSERT INTO fx_rates (date, base, quote, rate, batch_id) "
                            "VALUES (:d, 'USD', 'EUR', :r, :b)"
                        ),
                        {"d": d_iso, "r": str(rate_eur_per_usd), "b": bid},
                    )
                    inserted += 1

            except (InvalidOperation, ValueError):
                errors += 1
                continue
            except Exception:
                errors += 1
                continue

        session.commit()

        # Compute deterministic hash of imported batch (like /fx/upload)
        rows_for_hash = session.execute(
            text(
                "SELECT date, base, quote, rate "
                "FROM fx_rates WHERE batch_id = :b ORDER BY date"
            ),
            {"b": bid},
        ).fetchall()

        h = hashlib.sha256()
        for r in rows_for_hash:
            line = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            {"rh": rates_hash, "bid": bid},
        )
        session.commit()

    logger.info(
        "FX bootstrap: imported=%s updated=%s errors=%s from %s (batch_id=%s)",
        inserted,
        updated,
        errors,
        csv_path,
        bid,
    )


@app.post("/maintenance/prune_fx")
def prune_fx(keep_years: int = Query(5, ge=1, le=20), _admin: None = Depends(require_admin)) -> Dict[str, Any]:
    """
    Keep only the last N years of FX rates (rolling window).
    Safe: does not touch transactions.
    """
    today = dt.today()
    cutoff_year = today.year - keep_years
    cutoff = dt(cutoff_year, 1, 1)  # keep from Jan 1 of cutoff_year onward

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
def vacuum_sqlite(_admin: None = Depends(require_admin)) -> Dict[str, Any]:
    """
    Run VACUUM to reclaim file space in SQLite after large deletes.
    """
    with engine.connect() as conn:
        conn.execute(text("VACUUM"))
    return {"status": "ok"}


@app.get("/export/db", summary="Download current database backup")
def export_database(_admin: None = Depends(require_admin)):
    # Derive the active sqlite file from the SQLAlchemy engine
    db_name = getattr(engine.url, "database", None) or "cryptotaxcalc.db"
    db_path = db_name if os.path.isabs(db_name) else str((PROJECT_ROOT / db_name).resolve())

    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"Database not found at {db_path}")

    # Timestamped backup copy (kept on disk)
    import datetime
    backups_dir = PROJECT_ROOT / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_name = f"db_backup_{timestamp}.sqlite"
    backup_path = backups_dir / backup_name
    shutil.copy2(db_path, backup_path)

    # Stream the *live* DB as the download (filename is the backup copy name)
    return FileResponse(
        path=db_path,
        filename=backup_name,
        media_type="application/x-sqlite3"
    )


@app.post("/import/db", summary="Restore the SQLite database from an uploaded .db file (with safety checks)")
async def import_database(
    file: UploadFile = File(...),
    confirm: str = Query("", description="Must be 'I_UNDERSTAND' to proceed"),
    _admin: None = Depends(require_admin),
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
        db_name = getattr(engine.url, "database", None) or "cryptotaxcalc.db"
        src_db = db_name if os.path.isabs(db_name) else str((PROJECT_ROOT / db_name).resolve())
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
        
    # Aggregate by base asset
    by_asset: dict[str, dict[str, Decimal]] = {}
    for ev in events:
        a = (ev.asset or "").upper()
        agg = by_asset.setdefault(a, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg["proceeds"] += ev.proceeds
        agg["cost_basis"] += ev.cost_basis
        agg["gain"] += ev.gain

    # Aggregate by month (YYYY-MM)
    by_month: dict[str, dict[str, Decimal]] = {}
    for ev in events:
        try:
            dt_ev = datetime.fromisoformat(ev.timestamp)
        except Exception:
            continue
        mkey = f"{dt_ev.year:04d}-{dt_ev.month:02d}"
        agg = by_month.setdefault(mkey, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
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
                usd_per_eur = ensure_rate_or_default(session, ev_date)
                if usd_per_eur is None:
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, ev_date, db=session)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, ev_date, db=session)
                eur_totals["gain"] += usd_to_eur(ev.gain, ev_date, db=session)

    # Build CSV
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["section", "key", "proceeds", "cost_basis", "gain"])

    # By quote asset (sorted for stable order)
    for q, agg in sorted(by_quote.items()):
        writer.writerow([
            "by_quote_asset",
            q or "(none)",
            dec_to_str(agg["proceeds"]),
            dec_to_str(agg["cost_basis"]),
            dec_to_str(agg["gain"]),
        ])

    # By asset (base asset)
    for a, agg in sorted(by_asset.items()):
        writer.writerow([
            "by_asset",
            a or "(none)",
            dec_to_str(agg["proceeds"]),
            dec_to_str(agg["cost_basis"]),
            dec_to_str(agg["gain"]),
        ])

    # By month (YYYY-MM)
    for m, agg in sorted(by_month.items()):
        writer.writerow([
            "by_month",
            m,
            dec_to_str(agg["proceeds"]),
            dec_to_str(agg["cost_basis"]),
            dec_to_str(agg["gain"]),
        ])

    # Totals (all quotes combined)
    tot_pro = sum((v["proceeds"] for v in by_quote.values()), Decimal("0"))
    tot_cb  = sum((v["cost_basis"] for v in by_quote.values()), Decimal("0"))
    tot_g   = sum((v["gain"] for v in by_quote.values()), Decimal("0"))

    writer.writerow([
        "totals",
        "ALL",
        dec_to_str(tot_pro),
        dec_to_str(tot_cb),
        dec_to_str(tot_g),
    ])

    # EUR totals (converted)
    writer.writerow([
        "totals_eur",
        "EUR",
        dec_to_str(eur_totals["proceeds"]),
        dec_to_str(eur_totals["cost_basis"]),
        dec_to_str(eur_totals["gain"]),
    ])

    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"summary{('_' + str(year)) if year else ''}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=csv_bytes, media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/export/summary.pdf", summary="Download PDF summary (optionally filtered by year)")
def export_summary_pdf(
    year: int | None = None,
    include_tax_helpers: bool = Query(True),
    include_audit_appendix: bool = Query(True),
) -> StreamingResponse:
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
    
    # 4b) Aggregate by month (YYYY-MM) and by asset for charts & tables
    by_month: dict[str, dict[str, Decimal]] = {}
    by_asset: dict[str, dict[str, Decimal]] = {}

    for ev in events:
        # Month key: YYYY-MM
        try:
            dt_ev = datetime.fromisoformat(ev.timestamp)
        except Exception:
            continue
        mkey = f"{dt_ev.year:04d}-{dt_ev.month:02d}"
        agg_m = by_month.setdefault(mkey, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg_m["proceeds"] += ev.proceeds
        agg_m["cost_basis"] += ev.cost_basis
        agg_m["gain"] += ev.gain

        # Asset key
        akey = ev.asset
        agg_a = by_asset.setdefault(akey, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg_a["proceeds"] += ev.proceeds
        agg_a["cost_basis"] += ev.cost_basis
        agg_a["gain"] += ev.gain

    summary_by_month = {
        m: {k: dec_to_str(v) for k, v in sums.items()}
        for m, sums in by_month.items()
    }
    summary_by_asset = {
        a: {k: dec_to_str(v) for k, v in sums.items()}
        for a, sums in by_asset.items()
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
                usd_per_eur = ensure_rate_or_default(session, ev_date)
                if usd_per_eur is None:
                    continue
                eur_totals["proceeds"] += usd_to_eur(ev.proceeds, ev_date, db=session)
                eur_totals["cost_basis"] += usd_to_eur(ev.cost_basis, ev_date, db=session)
                eur_totals["gain"] += usd_to_eur(ev.gain, ev_date, db=session)

    # Find latest run UUID and its configuration (fallback to safe defaults)
    juris = None
    show_tax_helpers = True
    show_audit_appendix = True

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT run_id, jurisdiction, params_json, summary_json
                FROM calc_runs
                ORDER BY id DESC
                LIMIT 1
            """)
        ).mappings().first()

    if row:
        latest_run_id = row.get("run_id") or "n/a"
        raw_juris = row.get("jurisdiction") or ""
        juris = raw_juris.strip() or None

        params_raw = row.get("params_json")
        if params_raw:
            try:
                cfg = json.loads(params_raw)
                v = cfg.get("include_tax_helpers")
                if isinstance(v, bool):
                    show_tax_helpers = v
                v2 = cfg.get("include_audit_appendix")
                if isinstance(v2, bool):
                    show_audit_appendix = v2
            except Exception:
                # If parsing fails, we just keep the safe defaults (True/True)
                pass
    else:
        latest_run_id = "n/a"
        
    run_totals: dict[str, Any] = {}
    if row:
        raw_summary_json = row.get("summary_json")
        summary_obj = None
        if isinstance(raw_summary_json, dict):
            summary_obj = raw_summary_json
        elif raw_summary_json:
            try:
                summary_obj = json.loads(raw_summary_json)
            except Exception:
                summary_obj = None

        if isinstance(summary_obj, dict):
            maybe_totals = summary_obj.get("totals") or {}
            if isinstance(maybe_totals, dict):
                # e.g. proceeds_eur, cost_eur, gain_eur, taxable_gain_eur, exempt_gain_eur
                run_totals = {k: str(v) for k, v in maybe_totals.items()}

    # Build a small sample of events for the PDF table (convert to dicts!)
    top_events = [ev_to_dict(e) for e in events[:50]]

    # Convert EUR totals to strings for PDF safety
    eur_summary_payload = {
        "totals_eur": {
            "proceeds": dec_to_str(eur_totals["proceeds"]),
            "cost_basis": dec_to_str(eur_totals["cost_basis"]),
            "gain":      dec_to_str(eur_totals["gain"]),
        },
        "notes": (["See Data Quality Checks for run warnings."] if (warnings or []) else []),
    }


    # Build the PDF
    pdf_bytes = build_summary_pdf({
        "title": "Crypto Tax – FIFO Summary",
        "run_id": latest_run_id,
        "year": year,
        # Keys expected by report_pdf.py
        "summary_by_quote": {
            q: {
                "proceeds": str(v["proceeds"]),
                "cost_basis": str(v["cost_basis"]),
                "gain": str(v["gain"]),
            } for q, v in by_quote.items()
        },
        "summary_by_month": summary_by_month,
        "summary_by_asset": summary_by_asset,
        "eur_summary": eur_summary_payload,   # already in expected shape
        "top_events": top_events,
        "logo_path": str((PROJECT_ROOT / "logo" / "logo.png")),
        "jurisdiction": juris or "",
        "show_tax_helpers": include_tax_helpers,
        "show_audit_appendix": include_audit_appendix,
        "run_totals": run_totals,  # NEW: totals incl. taxable/exempt from latest run.summary_json
        "is_demo": True,           # this export is used by the demo dashboard
        "warnings": warnings or [],
    })

    # 7) Stream it to the client
    filename = f"summary{('_' + str(year)) if year else ''}.pdf"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
    
    
@app.get("/export/workspace_summary/{run_db_id}.pdf", summary="Export a Workspace summary PDF for a specific run.")
def export_workspace_summary(
    run_db_id: int,
    db: Session = Depends(get_db),
):
    """
    Generates a premium Workspace PDF report using the canonical engine results
    stored in calc_runs.summary_json and realized_events.

    This is the version used by the authenticated Workspace (non-demo).
    """

    # 1. Load CalcRun metadata
    run = db.query(CalcRun).filter(CalcRun.id == run_db_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_db_id} not found")

    # Parse summary_json
    raw_summary = getattr(run, "summary_json", None)
    summary = None
    if isinstance(raw_summary, dict):
        summary = raw_summary
    elif raw_summary:
        try:
            summary = json.loads(raw_summary)
        except Exception:
            summary = None

    if not summary:
        raise HTTPException(status_code=400, detail="Run has no summary_json — cannot export workspace PDF.")

    totals = summary.get("totals", {})
    warnings = summary.get("warnings", [])
    fx_ctx = summary.get("fx_context", {})

    # 2) Load realized events for this run
    #    - top_events: small, fixed sample for the PDF table
    #    - aggregates: computed from the full realised_events set (audit-correct)
    sampler = _RealizedEventSampler(
        seed_text=str(run.run_id or run.id),
        max_rows=50,
        top_k=6,
    )
    # top_events is built from sampler.finalize() after streaming all realized events
    top_events: list[dict[str, Any]] = []

    by_month: dict[str, dict[str, Decimal]] = {}
    by_asset: dict[str, dict[str, Decimal]] = {}
    by_quote: dict[str, dict[str, Decimal]] = {}

    proceeds_total = Decimal("0")
    cost_total = Decimal("0")
    gain_total = Decimal("0")

    events_count_total = 0
    min_d = ""
    max_d = ""

    all_rows = (
        db.query(RealizedEvent)
        .filter(RealizedEvent.run_id == run_db_id)
        .order_by(RealizedEvent.id.asc())
        .yield_per(2000)
    )

    for e in all_rows:
        events_count_total += 1
        sampler.offer(e)

        ts = str(getattr(e, "timestamp", "") or "")
        d = ts[:10] if len(ts) >= 10 else ""
        if d and len(d) == 10 and d[4] == "-" and d[7] == "-":
            if not min_d or d < min_d:
                min_d = d
            if not max_d or d > max_d:
                max_d = d

        p = Decimal(str(e.proceeds or "0"))
        cb = Decimal(str(e.cost_basis or "0"))
        g = Decimal(str(e.gain or "0"))

        proceeds_total += p
        cost_total += cb
        gain_total += g

        dt_ev = _parse_iso_ts(ts)
        if dt_ev:
            mkey = f"{dt_ev.year:04d}-{dt_ev.month:02d}"
            agg_m = by_month.setdefault(
                mkey,
                {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")},
            )
            agg_m["proceeds"] += p
            agg_m["cost_basis"] += cb
            agg_m["gain"] += g

        akey = (e.asset or "").upper()
        agg_a = by_asset.setdefault(
            akey,
            {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")},
        )
        agg_a["proceeds"] += p
        agg_a["cost_basis"] += cb
        agg_a["gain"] += g

        qkey = (e.quote_asset or "UNKNOWN").upper()
        agg_q = by_quote.setdefault(
            qkey,
            {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")},
        )
        agg_q["proceeds"] += p
        agg_q["cost_basis"] += cb
        agg_q["gain"] += g

    summary_by_month = {
        m: {k: dec_to_str(v) for k, v in sums.items()}
        for m, sums in by_month.items()
    }
    summary_by_asset = {
        a: {k: dec_to_str(v) for k, v in sums.items()}
        for a, sums in by_asset.items()
    }
    summary_by_quote = {
        q: {k: dec_to_str(v) for k, v in sums.items()}
        for q, sums in by_quote.items()
    }
    top_events = [ev_to_dict(e) for e in sampler.finalize()]

    # 4. Convert totals to EUR-summary block compatible with report_pdf.py
    eur_summary_payload = {
        "totals_eur": {
            "proceeds": dec_to_str(proceeds_total),
            "cost_basis": dec_to_str(cost_total),
            "gain": dec_to_str(gain_total),
        },
        "notes": (["See Data Quality Checks for run warnings."] if (warnings or []) else []),
    }

    # 5. Call build_summary_pdf
    pdf_bytes = build_summary_pdf({
        "title": "CryptoTaxCalc – Workspace Summary",
        "run_id": str(run.run_id or run.id),
        "year": None,  # full-run export (all years); use subset export for year-specific reports
        "scope_asset": "All assets",
        "scope_year": "All years",
        "period_start": min_d,
        "period_end": max_d,
        "events_count_total": events_count_total,
        "generated_at": _now_iso_z(),
        "jurisdiction": run.jurisdiction,
        "rule_version": run.rule_version,
        "tax_year": run.tax_year,
        "summary_by_quote": summary_by_quote,
        "summary_by_month": summary_by_month,
        "summary_by_asset": summary_by_asset,
        "eur_summary": eur_summary_payload,
        "top_events": top_events,  # already dicts
        "run_totals": totals,
        "show_tax_helpers": True,
        "show_audit_appendix": False,
        "logo_path": str((PROJECT_ROOT / "logo" / "logo.png")),
        "is_demo": False,     # important (workspace mode)
        "warnings": warnings or [],
        "show_yearly_tax_block": False,
    })

    filename = f"workspace_summary_run_{run_db_id}.pdf"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    )
    

@app.get(
    "/export/workspace_summary/{run_db_id}/subset.pdf",
    summary="Export a Workspace summary PDF for the current workspace filter.",
)
def export_workspace_summary_subset(
    run_db_id: int,
    year: int | None = Query(None, description="Optional tax-year filter (YYYY)"),
    asset: str | None = Query(None, description="Optional asset filter (e.g. BTC)"),
    db: Session = Depends(get_db),
):
    """
    Generates a Workspace PDF summary for a filtered subset of the dataset
    (matching the current Workspace filter: year + asset).

    IMPORTANT:
    - We DO NOT re-run the engine on a truncated history.
    - Filters slice realized_events for this run so long-term exemptions stay intact.
    """

    # 1) Load CalcRun metadata
    run = db.query(CalcRun).filter(CalcRun.id == run_db_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_db_id} not found")
    
    # Pull run-level warnings from summary_json so subset exports stay audit-consistent
    run_warnings: list[str] = []
    raw_summary = getattr(run, "summary_json", None)

    if isinstance(raw_summary, dict):
        run_warnings = raw_summary.get("warnings") or []
    elif raw_summary:
        try:
            js = json.loads(raw_summary)
            if isinstance(js, dict):
                run_warnings = js.get("warnings") or []
        except Exception:
            run_warnings = []

    if not isinstance(run_warnings, list):
        run_warnings = []

    # 2) Load and filter realized events for this run
    q = (
        db.query(RealizedEvent)
        .filter(RealizedEvent.run_id == run_db_id)
        .order_by(RealizedEvent.id.asc())
    )

    if year is not None:
        # RealizedEvent.timestamp is stored as ISO string -> filter with ISO string bounds
        start = f"{year:04d}-01-01"
        end = f"{year + 1:04d}-01-01"
        q = q.filter(RealizedEvent.timestamp >= start, RealizedEvent.timestamp < end)

    if asset:
        q = q.filter(RealizedEvent.asset == asset.upper())

    ev_rows = q.all()
    if not ev_rows:
        # Same behaviour as before: let the client show a “no data” message
        raise HTTPException(status_code=400, detail="No realized events found for this filter.")

    # 3) Aggregate EUR totals directly from realized_events
    dec0 = Decimal("0")
    proceeds = dec0
    cost = dec0

    for e in ev_rows:
        try:
            proceeds += Decimal(str(e.proceeds or "0"))
            cost += Decimal(str(e.cost_basis or "0"))
        except Exception:
            continue

    # 4) Build Summary by Month / Asset for tables + charts
    by_month: dict[str, dict[str, Decimal]] = {}
    by_asset: dict[str, dict[str, Decimal]] = {}
    by_quote: dict[str, dict[str, Decimal]] = {}

    min_d = ""
    max_d = ""

    for e in ev_rows:
        ts = str(getattr(e, "timestamp", "") or "")

        # Period range for this subset (used on the cover)
        d = ts[:10] if len(ts) >= 10 else ""
        if d and len(d) == 10 and d[4] == "-" and d[7] == "-":
            if not min_d or d < min_d:
                min_d = d
            if not max_d or d > max_d:
                max_d = d

        try:
            p = Decimal(str(e.proceeds or "0"))
            cb = Decimal(str(e.cost_basis or "0"))
            g = Decimal(str(e.gain or "0"))
        except Exception:
            p = dec0
            cb = dec0
            g = dec0

        # Month key (robust parsing; do not drop rows if timestamp includes 'Z')
        dt_ev = _parse_iso_ts(ts)
        if dt_ev:
            mkey = f"{dt_ev.year:04d}-{dt_ev.month:02d}"
            agg_m = by_month.setdefault(
                mkey, {"proceeds": dec0, "cost_basis": dec0, "gain": dec0}
            )
            agg_m["proceeds"] += p
            agg_m["cost_basis"] += cb
            agg_m["gain"] += g

        # Asset key
        akey = (e.asset or "").upper()
        agg_a = by_asset.setdefault(
            akey, {"proceeds": dec0, "cost_basis": dec0, "gain": dec0}
        )
        agg_a["proceeds"] += p
        agg_a["cost_basis"] += cb
        agg_a["gain"] += g

        # Quote asset key (THIS fixes the empty “Summary by Quote Asset” section)
        qkey = (e.quote_asset or "UNKNOWN").upper()
        agg_q = by_quote.setdefault(
            qkey, {"proceeds": dec0, "cost_basis": dec0, "gain": dec0}
        )
        agg_q["proceeds"] += p
        agg_q["cost_basis"] += cb
        agg_q["gain"] += g

    summary_by_month = {
        m: {k: dec_to_str(v) for k, v in sums.items()}
        for m, sums in by_month.items()
    }
    summary_by_asset = {
        a: {k: dec_to_str(v) for k, v in sums.items()}
        for a, sums in by_asset.items()
    }
    summary_by_quote = {
        q: {k: dec_to_str(v) for k, v in sums.items()}
        for q, sums in by_quote.items()
    }

    # 5) Compute taxable / exempt gain for this subset using HR/IT logic
    total_gain, taxable, exempt = _compute_subset_tax_split(run, ev_rows)

    subset_totals = {
        "proceeds_eur": dec_to_str(proceeds),
        "cost_eur": dec_to_str(cost),
        "gain_eur": dec_to_str(total_gain),
        "taxable_gain_eur": dec_to_str(taxable),
        "exempt_gain_eur": dec_to_str(exempt),
    }

    # 6) Build EUR summary payload compatible with report_pdf.py
    eur_summary_payload = {
        "totals_eur": {
            "proceeds": str(subset_totals.get("proceeds_eur", "0")),
            "cost_basis": str(subset_totals.get("cost_eur", "0")),
            "gain": str(subset_totals.get("gain_eur", "0")),
        },
        "notes": (["See Data Quality Checks for run warnings."] if (run_warnings or []) else []),
    }

    # 7) Limit events for PDF table (deterministic sample across this scope)
    asset_label = asset.upper() if asset else "All assets"
    year_label = str(year) if year is not None else "All years"

    sampler = _RealizedEventSampler(
        seed_text=f"{run.run_id or run.id}|{year_label}|{asset_label}",
        max_rows=50,
        top_k=6,
    )
    for e in ev_rows:
        sampler.offer(e)
    top_events = [ev_to_dict(e) for e in sampler.finalize()]

    # 8) Descriptive title reflecting the subset
    title_text = f"CryptoTaxCalc – Workspace Summary ({asset_label}, {year_label})"

    pdf_bytes = build_summary_pdf(
        {
            "title": title_text,
            "run_id": str(run.run_id or run.id),
            "scope_asset": asset_label,
            "scope_year": year_label,
            "period_start": min_d,
            "period_end": max_d,
            "events_count_total": len(ev_rows),
            "rule_version": run.rule_version,
            "tax_year": run.tax_year,
            "generated_at": _now_iso_z(),
            "show_yearly_tax_block": bool(year is not None),
            "year": year,
            "jurisdiction": run.jurisdiction,
            "summary_by_quote": summary_by_quote,
            "summary_by_month": summary_by_month,
            "summary_by_asset": summary_by_asset,
            "eur_summary": eur_summary_payload,
            "warnings": run_warnings or [],
            "top_events": top_events,
            "run_totals": subset_totals,
            "show_tax_helpers": True,
            "show_audit_appendix": False,
            "logo_path": str((PROJECT_ROOT / "logo" / "logo.png")),
            "is_demo": False,
        }
    )

    fname_asset = asset_label.replace(" ", "")
    fname_year = year_label.replace(" ", "")
    filename = f"workspace_summary_run_{run_db_id}_{fname_asset}_{fname_year}.pdf"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
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
    pdf_bytes = build_summary_pdf({
        "title": "Crypto Tax – Realized Events (FIFO)",
        "run_id": "n/a",
        "year": None,
        "summary_by_quote": {},       # no summary block for this export
        "summary_by_month": {},
        "summary_by_asset": {},
        "eur_summary": {
            "totals_eur": {"proceeds": "0", "cost_basis": "0", "gain": "0"},
            "notes": (["See Data Quality Checks for run warnings."] if (warnings or []) else []),
        },
        "warnings": warnings or [],
        "top_events": [ev_to_dict(e) for e in events],
    })

    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="calculate_events.pdf"'}
    )


@app.get("/export/events_csv")
def export_events_csv(run_id: str = "latest"):
    """
    Export realized events for a given run as CSV.
    Use run_id=latest to export the most recent run.

    CSV is deliberately human- and audit-friendly:
      - important columns first (timestamp, asset, qty, proceeds, cost, gain),
      - FX and run metadata included,
      - column names spelled out clearly.
    """
    with engine.begin() as conn:
        # Resolve internal calc_runs.id from external parameter
        if run_id == "latest":
            row = conn.execute(
                text("SELECT id FROM calc_runs ORDER BY id DESC LIMIT 1")
            ).fetchone()
            if not row:
                raise HTTPException(status_code=400, detail="No calculation runs found.")
            run_id_val = int(row[0])
        else:
            try:
                run_id_val = int(run_id)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid run_id")

        # Run metadata (for audit context)
        run_meta = conn.execute(
            text(
                """
                SELECT id, jurisdiction, tax_year, fx_set_id, run_id AS run_ref
                FROM calc_runs
                WHERE id = :rid
                """
            ),
            {"rid": run_id_val},
        ).mappings().first()

        if not run_meta:
            raise HTTPException(status_code=400, detail=f"Run metadata not found for id={run_id_val}")

         # Events for this run
        rows = conn.execute(
            text(
                """
                SELECT
                    timestamp,
                    asset,
                    qty_sold,
                    proceeds,
                    cost_basis,
                    gain,
                    quote_asset,
                    fee_applied,
                    matches_json
                FROM realized_events
                WHERE run_id = :rid
                ORDER BY id
                """
            ),
            {"rid": run_id_val},
        ).mappings().all()

    if not rows:
        raise HTTPException(status_code=400, detail=f"No realized events for run_id={run_id_val}")

    output = io.StringIO()
    w = _csv.writer(output)

    # Professional, self-explanatory headers (only fields we actually have)
    w.writerow([
        "timestamp",
        "asset",
        "qty_sold",
        "proceeds_eur",
        "cost_basis_eur",
        "gain_eur",
        "quote_asset",
        "fee_applied_eur",
        "matches_json",
        "jurisdiction",
        "tax_year",
        "fx_set_id",
        "calc_run_id",
        "run_ref",
    ])

    for r in rows:
        w.writerow([
            r.get("timestamp") or "",
            r.get("asset") or "",
            r.get("qty_sold") or "",
            r.get("proceeds") or "",
            r.get("cost_basis") or "",
            r.get("gain") or "",
            r.get("quote_asset") or "",
            r.get("fee_applied") or "",
            r.get("matches_json") or "",
            run_meta.get("jurisdiction") or "",
            run_meta.get("tax_year") or "",
            run_meta.get("fx_set_id") or "",
            run_meta.get("id") or run_id_val,
            run_meta.get("run_ref") or "",
        ])

    output.seek(0)

    filename = f"realized_events_run_{run_id_val}.csv"
    return StreamingResponse(
        iter([output.read()]),
        media_type="text/csv; charset=utf-8",
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
            dict(rid=int(run_id))
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
            dict(rid=int(run_id))
        ).fetchone()

    if not row:
        return {"run_id": run_id, "verified": False, "reason": "No stored digest for this run.", "recomputed": live}

    stored = {"input_hash": row[0], "output_hash": row[1], "manifest_hash": row[2]}
    ok = (stored == live)
    return {"run_id": run_id, "verified": bool(ok), "stored": stored, "recomputed": live}


@app.post("/admin/bundle", tags=["admin"])
def create_support_bundle(
    request: Request,
    _admin: None = Depends(require_bundle_admin),
):
    # Auth is enforced via require_bundle_admin dependency.
    # (Header token preferred; query tokens are allowed only in dev or when explicitly enabled.)

    # ------- rest of your existing implementation unchanged --------
    script = PROJECT_ROOT / "automation" / "collect_support_bundle.py"
    if not script.exists():
        raise HTTPException(status_code=500, detail=f"Bundle script not found: {script}")

    api_base = os.getenv("API_BASE", "http://127.0.0.1:8000")
    tail = 300
    ps_exe = sys.executable
    cmd = [ps_exe, "-u", str(script), "--api-base", api_base, "--tail-lines", str(tail), "--keep-zips", "5"]
    env = os.environ.copy()
    env["RUN_CONTEXT"] = "api"

    proc = subprocess.run(
        cmd,
        cwd=str(script.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        timeout=600,
        env=env,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    zip_path = None
    for line in stdout.splitlines():
        s = line.strip()
        if s.startswith("::zip::"):
            zip_path = s.split("::zip::", 1)[1].strip()
            break
    if not zip_path:
        time.sleep(0.5)
        zip_path = _latest_zip_path()
    zip_exists = bool(zip_path and os.path.exists(zip_path))

    if proc.returncode != 0 or not zip_exists:
        diag = {}
        latest_bundle_dir = _latest_bundle_dir()
        if latest_bundle_dir:
            def _read_if(p):
                try:
                    with open(p, "r", encoding="utf-8", errors="ignore") as f:
                        return f.read()[:5000]
                except Exception:
                    return None
            diag["states"] = _read_if(os.path.join(latest_bundle_dir, "_meta", "states.log"))
            diag["fatal_error"] = _read_if(os.path.join(latest_bundle_dir, "_meta", "fatal_error.txt"))
            diag["zip_error"] = _read_if(os.path.join(latest_bundle_dir, "_meta", "zip_error.txt"))

        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Bundle not created" if proc.returncode == 0 else "Script failed",
                "zip_path": zip_path,
                "zip_exists": zip_exists,
                "script": str(script),
                "stdout": stdout,
                "stderr": stderr,
                "return_code": proc.returncode,
                "ps_exe": ps_exe,
                "support_dir": str(SUPPORT_BUNDLES_DIR),
                "diag": diag,
            },
        )

    return {
        "status": "ok",
        "zip_path": zip_path,
        "zip_exists": zip_exists,
        "script": str(script),
        "stdout": stdout,
        "stderr": stderr,
        "return_code": proc.returncode,
        "ps_exe": ps_exe,
    }


@app.post("/admin/smoke", tags=["admin"])
def admin_smoke(_admin: None = Depends(require_admin_scripts)):

    runner = PROJECT_ROOT / "automation" / "run_smoke_and_email.py"
    if not runner.exists():
        raise HTTPException(status_code=500, detail=f"Runner not found: {runner}")

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env["RUN_CONTEXT"] = "api"

    proc = subprocess.run(
        [sys.executable, "-u", str(runner)],
        cwd=str(runner.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=900,
        env=env,
    )

    # Try to find the latest bundle zip after the run
    latest_zip = _latest_zip_path()

    # Tail of stdout for quick UI feedback
    stdout_tail = (proc.stdout or "")[-8000:]
    stderr_tail = (proc.stderr or "")[-8000:]

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "return_code": proc.returncode,
        "latest_bundle_zip": latest_zip,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


@app.post("/admin/git-sync", tags=["admin"])
def admin_git_sync(_admin: None = Depends(require_admin_scripts)):

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


@app.get("/api/v1/runs", tags=["api"])
def api_list_runs(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Machine-facing list of calculation runs (DB-backed).

    Returns newest runs first from the calc_runs table, including:
      - id (integer PK)
      - run_id (external reference, uuid/string if present)
      - jurisdiction, tax_year
      - finished_at timestamp
      - totals (from summary_json.totals if available)
    """
    with SessionLocal() as session:
        q = session.query(CalcRun).order_by(CalcRun.id.desc())
        total = q.count()
        rows = q.offset(offset).limit(limit).all()

        items: list[dict[str, Any]] = []
        for r in rows:
            raw_summary = getattr(r, "summary_json", None)
            summary_obj = None
            if isinstance(raw_summary, dict):
                summary_obj = raw_summary
            elif raw_summary:
                try:
                    summary_obj = json.loads(raw_summary)
                except Exception:
                    summary_obj = None

            totals: dict[str, Any] = {}
            if isinstance(summary_obj, dict):
                maybe_totals = summary_obj.get("totals") or {}
                if isinstance(maybe_totals, dict):
                    totals = maybe_totals

            items.append(
                {
                    "id": int(r.id),
                    "run_id": str(r.run_id or r.id),
                    "jurisdiction": r.jurisdiction,
                    "tax_year": getattr(r, "tax_year", None),
                    "finished_at": (
                        r.finished_at.isoformat().replace("+00:00", "Z")
                        if getattr(r, "finished_at", None)
                        else None
                    ),
                    "totals": totals,
                }
            )

    return {
        "items": items,
        "limit": limit,
        "offset": offset,
        "total": total,
    }


@app.get("/api/v1/runs/{run_id}", tags=["api"])
def api_get_run_manifest(
    run_id: str,
    session: SASession = Depends(get_session),
):
    """
    Machine-facing manifest for a completed run.

    Returns a compact JSON manifest using the same logic as the history
    download flow, but without bundling files. Good for API clients.
    """
    rid_int = _resolve_db_run_id(session, run_id)
    manifest = _build_manifest(session, rid_int, run_id)
    return manifest


@app.get("/api/v1/runs/{run_id}/events", tags=["api"])
def api_get_run_events(
    run_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    year: int | None = Query(None),
    asset: str | None = Query(None),
    session: SASession = Depends(get_session),
):
    """
    Paginated, machine-friendly realized events for a run.

    Optional filters (year/asset) are applied server-side so the Events table and charts
    remain correct even when only the first page is loaded.
    """
    # Resolve external run_id (UUID-like) → internal integer id
    rid_int = _resolve_db_run_id(session, run_id)

    where = ["run_id = :rid"]
    params: dict[str, object] = {"rid": rid_int, "lim": int(limit), "off": int(offset)}

    if year is not None:
        # realized_events.timestamp is stored as ISO string -> filter with ISO string bounds
        start = f"{year:04d}-01-01"
        end = f"{year + 1:04d}-01-01"
        where.append("timestamp >= :start AND timestamp < :end")
        params["start"] = start
        params["end"] = end

    if asset:
        a = str(asset).strip().upper()
        if a:
            where.append("asset = :asset")
            params["asset"] = a

    where_sql = " AND ".join(where)

    # Count + range for premium progress UI and truthful scope meta
    meta = session.execute(
        text(
            f"SELECT COUNT(*) AS total, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts "
            f"FROM realized_events WHERE {where_sql}"
        ),
        params,
    ).mappings().first() or {}

    total = meta.get("total") or 0
    min_ts = meta.get("min_ts") or ""
    max_ts = meta.get("max_ts") or ""

    # Pull only the fields we want to expose; realized_events already stores strings
    rows = session.execute(
        text(
            f"""
            SELECT
              timestamp,
              asset,
              qty_sold,
              proceeds,
              cost_basis,
              gain,
              quote_asset,
              fee_applied
            FROM realized_events
            WHERE {where_sql}
            ORDER BY timestamp, id
            LIMIT :lim OFFSET :off
            """
        ),
        params,
    ).mappings().all()

    items = [
        {
            "timestamp": r["timestamp"],
            "asset": r["asset"],
            "qty_sold": r["qty_sold"],
            "proceeds": r["proceeds"],
            "cost_basis": r["cost_basis"],
            "gain": r["gain"],
            "quote_asset": r["quote_asset"],
            "fee_applied": r["fee_applied"],
        }
        for r in rows
    ]

    return {
        "run_id": run_id,
        "items": items,
        "limit": limit,
        "offset": offset,
        "items_count": len(items),
        "total": int(total),
        "min_ts": min_ts,
        "max_ts": max_ts,
    }


@app.get("/api/v1/runs/{run_id}/tax", tags=["api"])
def api_get_run_tax(
    run_id: str,
    session: SASession = Depends(get_session),
):
    """
    Machine-facing tax summary for a run.

    Uses the stored calc_runs.summary_json (if present) as the single source
    of truth for EUR totals and taxable gain. Falls back to zero totals if
    no summary_json is available.
    """
    # Resolve external run_id (UUID-like or numeric string) → internal integer id
    rid_int = _resolve_db_run_id(session, run_id)

    # Load the CalcRun row
    run = session.get(CalcRun, rid_int)
    if not run:
        raise HTTPException(status_code=404, detail=f"calc run not found: id={rid_int}")

    # Try to parse summary_json if present
    source = "fallback"
    totals: dict[str, str] = {
        "proceeds_eur": "0",
        "cost_eur": "0",
        "gain_eur": "0",
        "taxable_gain_eur": "0",
        "exempt_gain_eur": "0",
    }

    raw_summary = getattr(run, "summary_json", None)
    summary_obj = None

    # summary_json may already be a dict (JSON column) or a JSON string.
    if isinstance(raw_summary, dict):
        summary_obj = raw_summary
    elif raw_summary:
        try:
            summary_obj = json.loads(raw_summary)
        except Exception:
            summary_obj = None

    if isinstance(summary_obj, dict):
        maybe_totals = summary_obj.get("totals") or {}
        if isinstance(maybe_totals, dict):
            source = "summary_json"
            for key in (
                "proceeds_eur",
                "cost_eur",
                "gain_eur",
                "taxable_gain_eur",
                "exempt_gain_eur",
            ):
                val = maybe_totals.get(key)
                if val is not None:
                    totals[key] = str(val)

    return {
        "run_id": str(run.run_id or rid_int),
        "run_db_id": int(run.id),
        "jurisdiction": run.jurisdiction,
        "rule_version": run.rule_version,
        "tax_year": getattr(run, "tax_year", None),
        "lot_method": run.lot_method,
        "fx_set_id": run.fx_set_id,
        "totals": totals,
        "source": source,
    }


@app.get("/history", tags=["history"])
def history_index(
    request: Request,
    format: str = Query(
        "json",
        description="json for API clients and tests, html for the Recent runs page",
    ),
):
    """
    Recent runs index.

    - format=json (default): returns a plain JSON list (tests and API clients).
    - format=html: renders the Recent runs page.
    """
    items = _list_calc_runs_meta()

    if format.lower() == "html":
        return templates.TemplateResponse(
            "history.html",
            {"request": request, "runs": items},
        )
    return JSONResponse(items)


@app.get("/history/{run_id}/download", summary="Download calculation run as ZIP", tags=["history"])
def history_download(run_id: str, request: Request, session: SASession = Depends(get_session)):
    debug = request.query_params.get("debug") == "1"
    with SessionLocal() as session:
        rid_int = _resolve_db_run_id(session, run_id)
    
    with SessionLocal() as session:
        # Get run_id row to pull started_at (preferred for manifest.created_at)
        row = session.execute(
            select(CalcRun.started_at).where(CalcRun.id == rid_int)
        ).first()
        started_at_dt = row[0] if row else None

    created_at_iso = (
        started_at_dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        if started_at_dt else
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )
    
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    manifest = {
        "id": rid_int,
        "run_id": run_id,
        "created_at": created_at_iso,
        # keep whatever else you already put in your manifest
        # e.g. "files": files_list, "notes": ..., etc.
    }

    # Build a minimal bundle for this run (reusing your existing bundle builder if you have one)
    # Here we just produce a tiny ZIP with a manifest
    buf = io.BytesIO()

    # Build manifest using a fresh, open session
    with SessionLocal() as s:
        manifest = _build_manifest(s, rid_int, run_id)

    # Now write the manifest into the zip
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":"))
        )

    if debug:
        return JSONResponse(content={"run_id": run_id, "id": rid_int})
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp.write(buf.getvalue()); tmp.flush(); tmp.close()
    filename = f"run_{run_id}.zip"
    return FileResponse(tmp.name, media_type="application/zip", filename=filename)


@app.get("/history/run/{run_id}/download", include_in_schema=False)
def history_download_compat(
    run_id: str,
    request: Request,
    session: SASession = Depends(get_session),
):
    return history_download(run_id, request, session)


@app.get("/history/runs", response_class=JSONResponse, tags=["history"])
def history_list_runs():
    """
    List all stored calculation runs with light metadata.
    """
    return JSONResponse({"items": _list_calc_runs_meta()})


@app.get("/history/run/{run_id}", response_class=JSONResponse, tags=["history"])
def history_get_run(run_id: str = PathParam(..., description="The run_id (UUID) stored on disk")):
    data = _load_calc_run(run_id)
    if not data:
        raise HTTPException(status_code=404, detail="Run not found")
    return JSONResponse(data)


@app.delete("/history/run/{run_id}", response_class=JSONResponse, tags=["history"])
def history_delete_run(run_id: str = PathParam(...)):
    ok = _delete_calc_run(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Run not found")
    return JSONResponse({"status": "deleted", "run_id": run_id})


@app.get("/history/run/{run_id}/events.csv")
def history_events_csv(run_id: str, session: SASession = Depends(get_session)):
    """
    Return realized events for a run as CSV.
    Always emit a CSV header even when there are zero rows.

    This uses the same column structure as /export/events_csv.
    """
    # Resolve external run_id → internal integer id
    try:
        rid_int = _resolve_db_run_id(session, run_id)
    except HTTPException:
        # fall back if caller passed the numeric id directly
        try:
            rid_int = int(run_id)
        except Exception:
            header = "timestamp,asset,qty_sold,proceeds_eur,cost_basis_eur,gain_eur,quote_asset,fee_applied_eur,matches_json,jurisdiction,tax_year,fx_set_id,calc_run_id,run_ref\n"
            return Response(header, media_type="text/csv; charset=utf-8")

    with engine.begin() as conn:
        # Run metadata (for audit context)
        run_meta = conn.execute(
            text(
                """
                SELECT id, jurisdiction, tax_year, fx_set_id, run_id AS run_ref
                FROM calc_runs
                WHERE id = :rid
                """
            ),
            {"rid": rid_int},
        ).mappings().first()

        # Events for this run
        rows = conn.execute(
            text(
                """
                SELECT
                    timestamp,
                    asset,
                    qty_sold,
                    proceeds,
                    cost_basis,
                    gain,
                    quote_asset,
                    fee_applied,
                    matches_json
                FROM realized_events
                WHERE run_id = :rid
                ORDER BY id
                """
            ),
            {"rid": rid_int},
        ).mappings().all()

    output = io.StringIO()
    w = _csv.writer(output)

    # Always emit header
    w.writerow([
        "timestamp",
        "asset",
        "qty_sold",
        "proceeds_eur",
        "cost_basis_eur",
        "gain_eur",
        "quote_asset",
        "fee_applied_eur",
        "matches_json",
        "jurisdiction",
        "tax_year",
        "fx_set_id",
        "calc_run_id",
        "run_ref",
    ])

    if not rows or not run_meta:
        output.seek(0)
        filename = f"realized_events_run_{rid_int}.csv"
        return StreamingResponse(
            iter([output.read()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}\"'}
        )

    for r in rows:
        w.writerow([
            r.get("timestamp") or "",
            r.get("asset") or "",
            r.get("qty_sold") or "",
            r.get("proceeds") or "",
            r.get("cost_basis") or "",
            r.get("gain") or "",
            r.get("quote_asset") or "",
            r.get("fee_applied") or "",
            r.get("matches_json") or "",
            run_meta.get("jurisdiction") or "",
            run_meta.get("tax_year") or "",
            run_meta.get("fx_set_id") or "",
            run_meta.get("id") or rid_int,
            run_meta.get("run_ref") or "",
        ])

    output.seek(0)

    filename = f"realized_events_run_{rid_int}.csv"
    return StreamingResponse(
        iter([output.read()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    )


@app.get("/audit/history")
def audit_history(limit: int = 50):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, created_at, actor, action, meta_json
                FROM calc_audit
                ORDER BY id DESC
                LIMIT :lim
            """),
            {"lim": limit}
        ).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        # rename created_at → timestamp for test compatibility
        if "created_at" in d:
            d["timestamp"] = d.pop("created_at")
        items.append(d)

    return items


@app.get("/export", response_class=HTMLResponse, tags=["export"])
def export_ui():
    return """
<!doctype html><html><head><meta charset="utf-8"><title>Project Export</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu;max-width:720px;margin:2rem auto;padding:0 1rem}
#app-logo, #footer-logo { max-height: 48px; height: auto; width: auto; }
input[type="password"]{padding:.4rem .5rem; width: 320px; max-width: 100%;}
small{color:#666}
</style>
</head><body>
<h1>Project Export</h1>
<p>Creates a zip with source, history, logs, and (if SQLite) a DB snapshot.</p>

<p><strong>Admin token required</strong> (sent via <code>X-Admin-Token</code> header).</p>
<label>Admin token:
  <input type="password" id="admintoken" placeholder="Enter ADMIN_TOKEN">
</label>
<br><small>For production safety, avoid using tokens in URLs.</small><br><br>

<form id="form">
<label><input type="checkbox" id="history" checked> Include history</label><br>
<label><input type="checkbox" id="db" checked> Include database</label><br>
<label><input type="checkbox" id="logs" checked> Include logs</label><br>
<label><input type="checkbox" id="env" checked> Include .env (redacted)</label><br>
<label><input type="checkbox" id="req" checked> Include requirements.txt</label><br>
<label><input type="checkbox" id="pyproj" checked> Include pyproject.toml</label><br>
<label><input type="checkbox" id="git" checked> Include git metadata</label><br><br>
<button type="submit">Create & Download Bundle</button>
</form>

<p id="status"></p>

<script>
const form=document.getElementById('form'); const status=document.getElementById('status');
form.addEventListener('submit', async (e)=>{
  e.preventDefault(); status.textContent='Building bundle...';

  const token = (document.getElementById('admintoken').value || '').trim();
  if (!token) { status.textContent='Admin token is required.'; return; }

  const body = {
    include_history: document.getElementById('history').checked,
    include_db: document.getElementById('db').checked,
    include_logs: document.getElementById('logs').checked,
    include_env_redacted: document.getElementById('env').checked,
    include_requirements: document.getElementById('req').checked,
    include_pyproject: document.getElementById('pyproj').checked,
    include_git_meta: document.getElementById('git').checked
  };

  const res = await fetch('/export/bundle', {
    method:'POST',
    headers:{'Content-Type':'application/json', 'X-Admin-Token': token},
    body: JSON.stringify(body)
  });

  if (!res.ok) { status.textContent='Failed to build bundle.'; return; }
  const blob = await res.blob(); const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = 'CryptoTaxCalc_Export.zip';
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url); status.textContent='Done.';
});
</script></body></html>
    """


class ExportBody(BaseModel):
    include_history: bool = True
    include_db: bool = True
    include_logs: bool = True
    include_env_redacted: bool = True
    include_requirements: bool = True
    include_pyproject: bool = True
    include_git_meta: bool = True


@app.post("/export/bundle", tags=["export"])
def export_bundle(body: ExportBody, request: Request, _admin: None = Depends(require_admin)):
    debug = request.query_params.get("debug") == "1"

    body_dict = body.model_dump()

    # Build ExportOptions in a forward/backward compatible way.
    # This prevents 500s when the request schema evolves faster than exporter.py.
    import inspect
    sig = inspect.signature(ExportOptions)
    params = set(sig.parameters.keys())

    # Compatibility mapping: env toggle name changed across versions
    if "include_env_redacted" in body_dict and "include_env_redacted" not in params:
        if "include_env" in params and "include_env" not in body_dict:
            body_dict["include_env"] = body_dict["include_env_redacted"]
        body_dict.pop("include_env_redacted", None)

    if "include_env" in body_dict and "include_env" not in params and "include_env_redacted" in params:
        if "include_env_redacted" not in body_dict:
            body_dict["include_env_redacted"] = body_dict["include_env"]
        body_dict.pop("include_env", None)

    filtered = {k: v for k, v in body_dict.items() if k in params}
    dropped = sorted(set(body_dict.keys()) - set(filtered.keys()))
    if dropped:
        get_logger("export").warning("Ignoring unknown export bundle options: %s", dropped)

    opts = ExportOptions(**filtered)

    # exporter.build_export_zip() (current) returns a Path to the zip file.
    # Older implementations may return raw bytes. Support both.
    result = build_export_zip(opts)

    zip_path: Path | None = None
    zip_bytes: bytes | None = None

    if isinstance(result, (bytes, bytearray, memoryview)):
        zip_bytes = bytes(result)
    else:
        try:
            zip_path = Path(result)
        except Exception:
            zip_path = None

    if debug:
        out = {
            "ok": True,
            "opts": body.model_dump(),
            "filtered_opts": filtered,
            "ignored_opts": dropped,
            "result_type": type(result).__name__,
        }
        if zip_bytes is not None:
            out["size"] = len(zip_bytes)
        elif zip_path is not None and zip_path.exists():
            out["path"] = str(zip_path)
            out["size"] = zip_path.stat().st_size
        return JSONResponse(content=out)

    # Bytes-based bundle (fallback)
    if zip_bytes is not None:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        tmp.write(zip_bytes); tmp.flush(); tmp.close()
        return FileResponse(tmp.name, media_type="application/zip", filename="CryptoTaxCalc_Export.zip")

    # Path-based bundle (current exporter.py)
    if zip_path is None or not zip_path.exists() or not zip_path.is_file():
        raise HTTPException(status_code=500, detail="Export bundle failed: zip not created.")

    # Delete after response to avoid filling TEMP with old bundles
    from starlette.background import BackgroundTask

    def _safe_unlink(p: str) -> None:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass

    return FileResponse(
        str(zip_path),
        media_type="application/zip",
        filename="CryptoTaxCalc_Export.zip",
        background=BackgroundTask(_safe_unlink, str(zip_path)),
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catch any uncaught exceptions not handled by routes.
    """
    logger = get_logger("app")
    msg = f"Unhandled exception at {request.url.path}: {exc}"
    logger.error(msg)
    _atomic_write_json(
         FSPath("logs/app/last_error.json"),
        {"timestamp": _now_iso_z(), "error": str(exc), "path": request.url.path, "method": request.method},
    )
    return PlainTextResponse("Internal server error", status_code=500)


logger = get_logger("app")
route_list = [
    f"{r.methods} {r.path}" for r in app.routes if hasattr(r, "methods") and hasattr(r, "path")
]
_atomic_write_json(
    PROJECT_ROOT / "logs/app/routes_snapshot.json",
    {"timestamp": _now_iso_z(), "routes": sorted(route_list)},
)
logger.info(f"Loaded {len(route_list)} routes.")


app.include_router(router)

# Mount demo routes only when DEMO_MODE is enabled,
# so production deployments don't expose /demo/* by accident.
allow_demo_in_prod = _truthy_env(os.getenv("ALLOW_DEMO_IN_PROD"))
if is_demo_mode_enabled() and (not IS_PROD or allow_demo_in_prod):
    app.include_router(demo_router)
    app.include_router(demo_build_router)


# ---------------------------------------------------------------------------
# Static assets and demo manifest auto-loader
# ---------------------------------------------------------------------------


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """
    Serve a favicon if available under /static or project root.
    """
    for candidate in [
        PROJECT_ROOT / "favicon.ico",
        STATIC_DIR / "favicon.ico",
        PROJECT_ROOT / "logo" / "favicon.ico"
    ]:
        if candidate.exists():
            return FileResponse(candidate, media_type="image/x-icon")
    raise HTTPException(status_code=404, detail="favicon not found")


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def landing_page(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})

@app.get("/workspace", response_class=HTMLResponse, include_in_schema=False)
def workspace_page(request: Request):
    """
    Main workspace for real users (non-demo).
    Injects user_display_name for personalization in the hero.
    """

    # -------------------------------
    # 1) Determine the username
    # -------------------------------
    # If you later add authentication, replace this block.
    # For now, we simply use a placeholder or cookie-based extraction if present.

    # Preferred order:
    # - request.state.user.full_name (future)
    # - request.state.user.email name part
    # - fallback to None (will show generic text)

    user_display_name = None

    # If in the future you attach a user object in middleware:
    if hasattr(request.state, "user") and request.state.user:
        u = request.state.user
        # Try full_name first
        if hasattr(u, "full_name") and u.full_name:
            user_display_name = u.full_name.strip()
        # Else fallback to email prefix
        elif hasattr(u, "email") and u.email:
            user_display_name = u.email.split("@")[0]

    # Temporary fallback (until login system is added)
    # COMMENT THIS OUT when real auth exists
    if user_display_name is None:
        user_display_name = "User"

    # -------------------------------
    # 2) Render template with name
    # -------------------------------
    return templates.TemplateResponse(
        "workspace.html",
        {
            "request": request,
            "user_display_name": user_display_name,
        }
    )
    

@app.get("/workspace/results", response_class=HTMLResponse, include_in_schema=False)
def workspace_results_page(
    request: Request,
    run_id: int | None = Query(None, description="Calc run DB id (from /calculate/v2)"),
):
    user_display_name = None

    if hasattr(request.state, "user") and request.state.user:
        u = request.state.user
        if hasattr(u, "full_name") and u.full_name:
            user_display_name = u.full_name.strip()
        elif hasattr(u, "email") and u.email:
            user_display_name = u.email.split("@")[0]

    if user_display_name is None:
        user_display_name = "User"

    return templates.TemplateResponse(
        "workspace_results.html",
        {
            "request": request,
            "user_display_name": user_display_name,
            "run_id": run_id,
        }
    )



@app.get("/demo/logo", include_in_schema=False)
async def demo_logo():
    """
    Serve project logo (white on dark preferred). Looks for:
    - /logo/favicon.png
    """
    if not _demo_allowed_here():
        _admin_not_found()
    
    for candidate in [
        PROJECT_ROOT / "logo" / "White_transparent.png",
        PROJECT_ROOT / "logo" / "Black_transparent.png",
        PROJECT_ROOT / "logo.png"
    ]:
        if candidate.exists():
            return FileResponse(candidate, media_type="image/png")
    raise HTTPException(status_code=404, detail="logo not found")


# ---------------------------------------------------------------------------
# Demo build manifest auto-load
# ---------------------------------------------------------------------------
BUILD_MANIFEST_PATH = PROJECT_ROOT / "demo_build_manifest.json"


def _load_demo_manifest() -> dict:
    if BUILD_MANIFEST_PATH.exists():
        try:
            data = json.loads(BUILD_MANIFEST_PATH.read_text(encoding="utf-8"))
            return data
        except Exception as e:
            return {"status": "error", "error": str(e)}
    return {"status": "missing", "version": "n/a", "commit": "n/a"}


@app.get("/demo/manifest", include_in_schema=False)
def get_demo_manifest():
    """
    Returns version/build info used in Demo footer.
    """
    if not _demo_allowed_here():
        _admin_not_found()
    
    data = _load_demo_manifest()
    return JSONResponse(data)


@app.get("/demo/runs/recent", summary="List recent demo calculation runs")
def demo_runs_recent(limit: int = Query(10, ge=1, le=50)):
    """
    Return a compact list of recent calculation runs for the demo dashboard.

    Each item includes:
      - internal id (calc_runs.id)
      - external run_id reference (uuid-like string, if present)
      - jurisdiction and tax_year
      - finished_at timestamp
      - basic totals: gain, taxable_gain, exempt_gain (in EUR)
      - warning count from summary_json
    """
    if not _demo_allowed_here():
        _admin_not_found()
    
    items: list[dict] = []
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, run_id, jurisdiction, tax_year, finished_at, summary_json
                FROM calc_runs
                ORDER BY id DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings().all()

    for r in rows:
        # summary_json may be a dict (JSON column) or a JSON string; handle both.
        raw_summary = r.get("summary_json")
        summary_obj = None
        if isinstance(raw_summary, dict):
            summary_obj = raw_summary
        elif raw_summary:
            try:
                summary_obj = json.loads(raw_summary)
            except Exception:
                summary_obj = None

        totals = {}
        warnings = []
        if isinstance(summary_obj, dict):
            maybe_totals = summary_obj.get("totals") or {}
            if isinstance(maybe_totals, dict):
                totals = maybe_totals
            w = summary_obj.get("warnings")
            if isinstance(w, list):
                warnings = w

        def _as_str_dec(value, default="0"):
            """
            Safely convert a totals field to a normalized decimal string.
            """
            if value is None:
                return default
            try:
                d = Decimal(str(value))
                return str(d)
            except Exception:
                return str(value)

        gain_eur = totals.get("gain_eur") or totals.get("gain")
        taxable_gain_eur = (
            totals.get("taxable_gain_eur")
            or totals.get("taxable_gain")
        )
        exempt_gain_eur = (
            totals.get("exempt_gain_eur")
            or totals.get("exempt_gain")
        )

        items.append(
            {
                "id": r.get("id"),
                "run_id": r.get("run_id"),
                "jurisdiction": r.get("jurisdiction"),
                "tax_year": r.get("tax_year"),
                "finished_at": r.get("finished_at"),
                "gain_eur": _as_str_dec(gain_eur),
                "taxable_gain_eur": _as_str_dec(taxable_gain_eur),
                "exempt_gain_eur": _as_str_dec(exempt_gain_eur),
                "warning_count": len(warnings),
            }
        )

    return {"runs": items}

# ============================================================
# ONE-TIME DB RESET ENDPOINT (Safe for Option A migration)
# ============================================================
@app.post(
    "/admin/reset_database_option_a",
    tags=["admin"],
    summary="⚠️ Reset database for Option A migration",
    description=(
        "WARNING: This deletes ALL transactional data.\n"
        "Use ONLY once before applying Option A migration.\n"
        "Requires admin token + explicit confirmation."
    ),
    include_in_schema=False,
)
def reset_database_option_a(
    confirm: str = Query("", description="Must be 'I_UNDERSTAND' to proceed"),
    _admin: None = Depends(require_admin),
):
    if confirm != "I_UNDERSTAND":
        raise HTTPException(
            status_code=400,
            detail="Confirmation missing. Add ?confirm=I_UNDERSTAND to proceed (this will delete ALL data).",
        )

    with SessionLocal() as session:
        # Child tables first (FK → calc_runs)
        session.query(RunInput).delete()
        session.query(RealizedEvent).delete()
        session.query(RunDigest).delete()
        session.query(AuditLog).delete()

        # Parents after children
        session.query(CalcRun).delete()
        session.query(TransactionRow).delete()
        session.query(RawEvent).delete()
        session.commit()

    return {"status": "OK", "message": "Database fully reset for Option A migration."}


def _compute_subset_tax_split(run: CalcRun, events: list[RealizedEvent]) -> tuple[Decimal, Decimal, Decimal]:
    """
    Compute total, taxable and exempt gain (in EUR) for a subset of realized_events.

    For HR:
      - Uses long-term holding logic (2-year rule) when per-lot timestamps are available
      - Losses always reduce taxable gain (never "exempt")
      - If match-level data is missing or untrusted, falls back to treating the
        event's full positive gain as taxable.

    For IT (and any other jurisdictions for now):
      - All gain is treated as taxable (no exemption logic here yet).
    """
    dec0 = Decimal("0")

    if not events:
        return dec0, dec0, dec0

    juris = (run.jurisdiction or "HR").upper()

    # Sum of event-level gains for this subset
    total_gain = dec0
    for e in events:
        try:
            total_gain += Decimal(str(e.gain or "0"))
        except Exception:
            continue

    # Non-HR: apply jurisdiction finalize logic so filtered summaries match run totals.
    if juris != "HR":
        if juris == "IT":
            # Read IT threshold + rounding from params_json if available
            cfg_dict: dict = {}
            raw_params = getattr(run, "params_json", None)
            try:
                if isinstance(raw_params, str):
                    cfg_dict = json.loads(raw_params) or {}
                elif isinstance(raw_params, dict):
                    cfg_dict = raw_params or {}
            except Exception:
                cfg_dict = {}

            it_thr = None
            try:
                if cfg_dict.get("it_threshold_eur") is not None:
                    it_thr = Decimal(str(cfg_dict.get("it_threshold_eur")))
            except Exception:
                it_thr = None

            round_dp = 2
            try:
                if cfg_dict.get("round_dp") is not None:
                    round_dp = int(cfg_dict.get("round_dp") or 2)
            except Exception:
                round_dp = 2

            fx_source = str(cfg_dict.get("fx_source") or "ECB")
            rule_version = str(getattr(run, "rule_version", None) or cfg_dict.get("rule_version") or "2025.1")

            cfg = CalcConfig(
                jurisdiction="IT",
                rule_version=rule_version,
                fx_source=fx_source,
                it_threshold_eur=it_thr,
                round_dp=round_dp,
            )

            from cryptotaxcalc.rules.base import RunContext
            from cryptotaxcalc.rules.it import ItRule

            tax_year = int(getattr(run, "tax_year", datetime.utcnow().year) or datetime.utcnow().year)
            ctx = RunContext(cfg=cfg, tax_year=tax_year)

            taxable = ItRule().finalize_taxable_gain(total_gain, ctx)
            exempt = total_gain - taxable
            return total_gain, taxable, exempt

        return total_gain, total_gain, dec0

    # HR-specific split -------------------------------
    # Determine holding_exemption_days from params_json (default 730)
    threshold_days = 730
    raw_params = getattr(run, "params_json", None)
    try:
        if isinstance(raw_params, dict):
            cfg_dict = raw_params
        elif raw_params:
            cfg_dict = json.loads(raw_params)
        else:
            cfg_dict = {}
        hed = cfg_dict.get("holding_exemption_days")
        if hed is not None:
            threshold_days = int(hed)
    except Exception:
        # keep default if anything goes wrong
        pass
    if threshold_days <= 0:
        threshold_days = 730

    taxable = dec0
    exempt = dec0

    for e in events:
        # Event-level gain (already in EUR)
        try:
            gain_eur = Decimal(str(e.gain or "0"))
        except Exception:
            continue

        # Losses always reduce taxable base, never become "exempt"
        if gain_eur <= 0:
            taxable += gain_eur
            continue

        # If we have no match-level detail, treat the full positive gain as taxable
        if not e.matches_json:
            taxable += gain_eur
            continue

        try:
            matches = json.loads(e.matches_json) or []
        except Exception:
            taxable += gain_eur
            continue

        event_taxable = dec0
        event_exempt = dec0

        for m in matches:
            proceeds_raw = m.get("proceeds_eur")
            cost_raw = m.get("cost_eur")

            # If we lack per-match proceeds/cost, fall back to full-event taxable gain
            if proceeds_raw is None or cost_raw is None:
                event_taxable = gain_eur
                event_exempt = dec0
                break

            try:
                proceeds = Decimal(str(proceeds_raw))
                cost = Decimal(str(cost_raw))
            except Exception:
                event_taxable = gain_eur
                event_exempt = dec0
                break

            mgain = proceeds - cost

            # Per-match losses: fully taxable (as negative)
            if mgain <= 0:
                event_taxable += mgain
                continue

            acquired_at = m.get("acquired_at")
            disposed_at = m.get("disposed_at")

            # Fallback disposed_at to the event timestamp if missing
            if disposed_at is None:
                disposed_at = getattr(e, "timestamp", None)

            held_days = None
            try:
                buy_ts = None
                sell_ts = None

                if isinstance(acquired_at, str):
                    buy_ts = datetime.fromisoformat(acquired_at.replace("Z", "+00:00"))
                elif isinstance(acquired_at, datetime):
                    buy_ts = acquired_at

                if isinstance(disposed_at, str):
                    sell_ts = datetime.fromisoformat(disposed_at.replace("Z", "+00:00"))
                elif isinstance(disposed_at, datetime):
                    sell_ts = disposed_at

                if buy_ts and sell_ts:
                    held_days = max((sell_ts - buy_ts).days, 0)
            except Exception:
                held_days = None

            # If we don't know holding period, be conservative: taxable
            if held_days is None or threshold_days <= 0:
                event_taxable += mgain
            elif held_days > threshold_days:
                # Long-term → exempt
                event_exempt += mgain
            else:
                # Within threshold → taxable
                event_taxable += mgain

        taxable += event_taxable
        exempt += event_exempt

    return total_gain, taxable, exempt


@app.get("/calc/run/{run_id}/summary_filtered", response_class=JSONResponse, tags=["calc"])
def summary_filtered(
    run_id: int,
    year: int | None = Query(None),
    asset: str | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    Return a filtered EUR summary for an existing run **without re-running FIFO**.

    Filters slice the already-computed realized_events so long-term exemptions stay intact.
    Includes breakdowns (by_asset / by_month) so charts remain consistent across scope changes.
    """
    run = db.query(CalcRun).filter(CalcRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    q = db.query(RealizedEvent).filter(RealizedEvent.run_id == run_id)

    if year is not None:
        start = f"{year:04d}-01-01"
        end = f"{year + 1:04d}-01-01"
        q = q.filter(RealizedEvent.timestamp >= start, RealizedEvent.timestamp < end)

    if asset:
        a = str(asset).strip().upper()
        if a:
            q = q.filter(RealizedEvent.asset == a)

    events = q.all()
    if not events:
        return {
            "count": 0,
            "by_asset": {},
            "by_month": {},
            "totals": {
                "proceeds_eur": "0",
                "cost_eur": "0",
                "gain_eur": "0",
                "taxable_gain_eur": "0",
                "exempt_gain_eur": "0",
                "taxable_base_eur": "0",
                "national_rate": "0",
                "local_rate": "0",
                "effective_rate": "0",
                "tax_due_eur": "0",
            },
        }

    dec0 = Decimal("0")
    proceeds = dec0
    cost = dec0

    by_asset_totals: dict[str, Decimal] = {}
    by_month_totals: dict[str, Decimal] = {}

    for e in events:
        try:
            proceeds += Decimal(str(e.proceeds or "0"))
            cost += Decimal(str(e.cost_basis or "0"))
        except Exception:
            pass

        try:
            g = Decimal(str(e.gain or "0"))
        except Exception:
            continue

        a = str(getattr(e, "asset", "") or "").strip().upper()
        if a:
            by_asset_totals[a] = (by_asset_totals.get(a) or dec0) + g

        ts = str(getattr(e, "timestamp", "") or "")
        m = ts[:7] if len(ts) >= 7 else ""
        if m:
            by_month_totals[m] = (by_month_totals.get(m) or dec0) + g

    total_gain, taxable, exempt = _compute_subset_tax_split(run, events)

    try:
        tax_year_fallback = int(getattr(run, "tax_year", None) or datetime.utcnow().year)
    except Exception:
        tax_year_fallback = datetime.utcnow().year

    tax_year_used = int(year) if year is not None else tax_year_fallback

    j = (run.jurisdiction or "HR").upper().strip()
    if j == "HR":
        national_rate = Decimal("0.12") if tax_year_used >= 2024 else Decimal("0.10")
    elif j == "IT":
        national_rate = Decimal("0.33") if tax_year_used >= 2026 else Decimal("0.26")
    else:
        national_rate = Decimal("0")

    local_rate = Decimal("0")
    effective_rate = national_rate + local_rate

    taxable_base_eur = taxable if taxable > 0 else dec0
    tax_due_eur = (taxable_base_eur * effective_rate).quantize(Decimal("0.01"))

    q2 = Decimal("0.01")

    by_asset = {
        k: {"gain_eur": str(v.quantize(q2))}
        for k, v in sorted(by_asset_totals.items(), key=lambda kv: kv[0])
    }
    by_month = {
        k: {"gain_eur": str(v.quantize(q2))}
        for k, v in sorted(by_month_totals.items(), key=lambda kv: kv[0])
    }

    return {
        "count": len(events),
        "by_asset": by_asset,
        "by_month": by_month,
        "totals": {
            "proceeds_eur": str(proceeds.quantize(q2)),
            "cost_eur": str(cost.quantize(q2)),
            "gain_eur": str(total_gain.quantize(q2)),
            "taxable_gain_eur": str(taxable.quantize(q2)),
            "exempt_gain_eur": str(exempt.quantize(q2)),
            "taxable_base_eur": str(taxable_base_eur.quantize(q2)),
            "national_rate": str(national_rate),
            "local_rate": str(local_rate),
            "effective_rate": str(effective_rate),
            "tax_due_eur": str(tax_due_eur),
        },
    }


@app.get("/calc/run/{run_id}/filters_meta", response_class=JSONResponse, tags=["calc"])
def filters_meta(
    run_id: int,
    db: Session = Depends(get_db),
):
    """
    Lightweight metadata for filter UX.

    Returns the full list of years and assets present in realized_events for the run, so
    filter dropdowns remain stable and complete (independent of paginated loading).
    """
    run = db.query(CalcRun).filter(CalcRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    years = db.execute(
        text(
            "SELECT DISTINCT substr(timestamp, 1, 4) AS y "
            "FROM realized_events "
            "WHERE run_id = :rid AND timestamp IS NOT NULL AND length(timestamp) >= 4 "
            "ORDER BY y DESC"
        ),
        {"rid": run_id},
    ).scalars().all()

    assets = db.execute(
        text(
            "SELECT DISTINCT asset "
            "FROM realized_events "
            "WHERE run_id = :rid AND asset IS NOT NULL AND asset != '' "
            "ORDER BY asset ASC"
        ),
        {"rid": run_id},
    ).scalars().all()

    years_out = [str(y) for y in years if y and str(y).isdigit()]
    assets_out = [str(a).strip().upper() for a in assets if a and str(a).strip()]

    return {"run_id": run_id, "years": years_out, "assets": assets_out}

 
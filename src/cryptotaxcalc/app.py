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
import asyncio
import anyio
# Load .env BEFORE importing project modules.
# This prevents security flags/tokens from being frozen to defaults at import-time.
_DOTENV_DISABLED = (os.getenv("CTC_DISABLE_DOTENV") or "").strip().lower() in {"1", "true", "yes", "on"}
if not _DOTENV_DISABLED:
    try:
        from pathlib import Path as _Path
        from dotenv import load_dotenv
        _ROOT = _Path(__file__).resolve().parents[2]  # <repo>/CryptoTaxCalc/
        load_dotenv(dotenv_path=str((_ROOT / ".env").resolve()), override=False)
    except Exception:
        pass
from typing import Dict, Any, List, Literal, Optional, Iterator, Tuple
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Response, Header, Request, Path as PathParam, Body, Depends, APIRouter
from decimal import Decimal, InvalidOperation
from sqlalchemy.exc import IntegrityError
from datetime import datetime, date, timezone, datetime as dt
from csv import DictReader
from io import StringIO, BytesIO
from sqlalchemy import text, and_, text as _sqltext, select
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse, HTMLResponse, PlainTextResponse
from fastapi.exception_handlers import request_validation_exception_handler as fastapi_request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.templating import Jinja2Templates
from datetime import datetime as _dt
from pathlib import Path as FSPath, Path
import uuid
from dataclasses import is_dataclass, asdict
from uuid import UUID, uuid4
from cryptotaxcalc.report_pdf import build_summary_pdf
from cryptotaxcalc.price_autosync import price_autosync_enabled, price_autosync_interval_seconds, price_autosync_loop
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
from .schemas import (
    CSVPreviewResponse,
    ImportCSVResponse,
    Transaction,
    CalcConfig,
    PrecheckAssetIssue,
    PrecheckResponse,
    WalletOutItem,
    WalletTransferOverrideRequest,
    WalletTransferRow,
    WalletTransferFileGroup,
    WalletTransferBatchRequest,
    PrecheckFileIssue,
)
from .db import SessionLocal, engine, init_db
from .models import (
    Base,
    TransactionRow,
    CalcRun,
    RunDigest,
    AuditLog,
    RealizedEvent,
    RawEvent,
    RunInput,
    WalletOutOverride,
)
from .calc_runner import run_calculation, run_calculation_on_subset
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
import html as _html
import hmac

from .runtime_paths import PROJECT_ROOT, RESOURCE_ROOT, AUTOMATION

# Writable/script/log folders (dev repo; or EXE folder)
GIT_SCRIPT = (PROJECT_ROOT / "automation" / "git_auto_push.ps1")
LOG_DIR = (PROJECT_ROOT / "automation" / "logs")

_fx_boot_logger = logging.getLogger("cryptotaxcalc.fx.bootstrap")

def _bootstrap_fx_from_csv_if_empty(_engine) -> None:
    """
    Bootstrap fx_rates from automation/fx_ecb.csv when the DB is empty.

    CSV format: date, usd_per_eur (USD per 1 EUR)
    Stored format: base='USD', quote='EUR', rate=<EUR per 1 USD> as TEXT (Decimal-safe).
    """
    try:
        with SessionLocal() as session:
            # Ensure schema exists before querying/inserting
            try:
                ensure_fx_rates_schema(session)
            except Exception:
                pass

            try:
                cnt = session.execute(text("SELECT COUNT(*) FROM fx_rates")).scalar() or 0
            except Exception as e:
                _fx_boot_logger.info("FX bootstrap skipped: cannot query fx_rates (%s)", e)
                return

            if int(cnt) > 0:
                _fx_boot_logger.info("FX bootstrap skipped: fx_rates already has %s rows.", cnt)
                return

            csv_path = (AUTOMATION / "fx_ecb.csv")
            if not csv_path.exists():
                _fx_boot_logger.warning("FX bootstrap skipped: %s not found.", csv_path)
                return

            # Optional: associate inserts to today's FX batch if available
            try:
                bid = get_or_create_current_fx_batch_id(session)
            except Exception:
                bid = None

            inserted = 0
            errors = 0

            with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
                rdr = csv.DictReader(f)
                if not rdr.fieldnames:
                    _fx_boot_logger.warning("FX bootstrap skipped: fx_ecb.csv has no header.")
                    return

                hm = {h.strip().lower(): h for h in rdr.fieldnames}
                if "date" not in hm or "usd_per_eur" not in hm:
                    _fx_boot_logger.warning("FX bootstrap skipped: fx_ecb.csv must include date, usd_per_eur.")
                    return

                for row in rdr:
                    try:
                        raw_date = (row.get(hm["date"]) or "").strip()
                        raw_rate = (row.get(hm["usd_per_eur"]) or "").strip()
                        if not raw_date or not raw_rate:
                            raise ValueError("missing date/rate")

                        d = datetime.strptime(raw_date, "%Y-%m-%d").date().isoformat()

                        usd_per_eur = Decimal(raw_rate)
                        if usd_per_eur <= 0:
                            raise ValueError("usd_per_eur <= 0")

                        eur_per_usd = (Decimal("1") / usd_per_eur)

                        session.execute(
                            text(
                                """
                                INSERT INTO fx_rates (date, base, quote, rate, batch_id)
                                VALUES (:d, 'USD', 'EUR', :r, :b)
                                ON CONFLICT(date, base, quote)
                                DO UPDATE SET rate=excluded.rate, batch_id=excluded.batch_id
                                """
                            ),
                            {"d": d, "r": str(eur_per_usd), "b": bid},
                        )
                        inserted += 1
                    except Exception:
                        errors += 1
                        continue

            session.commit()
            _fx_boot_logger.info("FX bootstrap imported %s rows from %s (errors=%s).", inserted, csv_path, errors)

    except Exception as e:
        _fx_boot_logger.warning("FX bootstrap skipped due to unexpected error: %s", e)
        

# -----------------------------------------------------------------------------
# FX Auto-Sync (startup + periodic)
# -----------------------------------------------------------------------------
_fx_autosync_logger = logging.getLogger("cryptotaxcalc.fx.autosync")
_FX_AUTOSYNC_TASK: asyncio.Task | None = None
_FX_AUTOSYNC_STOP: asyncio.Event | None = None
_PRICE_AUTOSYNC_TASK: asyncio.Task | None = None
_PRICE_AUTOSYNC_STOP: asyncio.Event | None = None


def _env_truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or not str(v).strip():
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not str(v).strip():
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _fx_autosync_enabled() -> bool:
    # Enabled by default (can be disabled with FX_AUTOSYNC_ENABLED=0)
    return _env_truthy("FX_AUTOSYNC_ENABLED", default=True)


def _fx_autosync_interval_seconds() -> int:
    minutes = _env_int("FX_AUTOSYNC_INTERVAL_MINUTES", 360)
    if minutes < 1:
        minutes = 1
    return minutes * 60


def _fx_autosync_fetch_ecb() -> bool:
    # Optional: download ECB zip and refresh fx_ecb.csv automatically
    return _env_truthy("FX_AUTOSYNC_FETCH_ECB", default=False)


def _fx_autosync_ecb_zip_url() -> str:
    return (os.getenv("FX_AUTOSYNC_ECB_ZIP_URL") or "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip").strip()


def _fx_autosync_max_stale_days() -> int:
    return _env_int("FX_AUTOSYNC_MAX_STALE_DAYS", 10)


def _fx_autosync_source_csv() -> FSPath:
    p = (os.getenv("FX_AUTOSYNC_SOURCE_CSV") or str((AUTOMATION / "fx_ecb.csv"))).strip()
    return FSPath(p).expanduser().resolve()


def _fx_refresh_fx_ecb_csv_from_ecb(target_csv: FSPath) -> dict:
    """
    Download ECB eurofxref-hist.zip and write automation/fx_ecb.csv (date, usd_per_eur).
    This removes the need to run update_fx.ps1 externally when FX_AUTOSYNC_FETCH_ECB=1.
    """
    try:
        import requests
    except Exception as e:
        return {"ok": False, "error": f"requests not available: {e}"}

    url = _fx_autosync_ecb_zip_url()
    timeout_s = _env_int("FX_AUTOSYNC_HTTP_TIMEOUT_SECONDS", 30)

    try:
        r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "CryptoTaxCalc/1.0"})
        r.raise_for_status()
    except Exception as e:
        return {"ok": False, "error": f"download failed: {e}", "url": url}

    try:
        buf = BytesIO(r.content)
        with zipfile.ZipFile(buf) as zf:
            # ECB zip contains eurofxref-hist.csv
            name = None
            for n in zf.namelist():
                if n.lower().endswith("eurofxref-hist.csv"):
                    name = n
                    break
            if not name:
                return {"ok": False, "error": "ECB zip missing eurofxref-hist.csv", "url": url}

            raw = zf.read(name).decode("utf-8-sig", errors="replace")
    except Exception as e:
        return {"ok": False, "error": f"zip parse failed: {e}", "url": url}

    rdr = _csv.DictReader(StringIO(raw))
    if not rdr.fieldnames:
        return {"ok": False, "error": "ECB CSV has no header", "url": url}

    hm = {h.strip(): h for h in rdr.fieldnames}
    if "Date" not in hm or "USD" not in hm:
        return {"ok": False, "error": "ECB CSV schema missing Date/USD", "headers": rdr.fieldnames}

    rows = []
    max_d = None
    for row in rdr:
        try:
            d = (row.get(hm["Date"]) or "").strip()
            usd = (row.get(hm["USD"]) or "").strip()
            if not d or not usd:
                continue
            # keep: date, usd_per_eur
            rows.append({"date": d, "usd_per_eur": usd})
            if max_d is None or d > max_d:
                max_d = d
        except Exception:
            continue

    if not rows or not max_d:
        return {"ok": False, "error": "No usable rows from ECB"}

    # Recency validation (max business day should be recent)
    try:
        max_dt = datetime.strptime(max_d, "%Y-%m-%d").date()
        age_days = (datetime.now(timezone.utc).date() - max_dt).days
        if age_days > _fx_autosync_max_stale_days():
            return {"ok": False, "error": f"ECB data stale (max date {max_d}, age {age_days}d)"}
    except Exception:
        pass

    # Sort by date asc for stable output
    rows.sort(key=lambda x: x["date"])

    target_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp = target_csv.with_suffix(".tmp")

    try:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=["date", "usd_per_eur"])
            w.writeheader()
            for rr in rows:
                w.writerow(rr)
        tmp.replace(target_csv)
    except Exception as e:
        return {"ok": False, "error": f"write failed: {e}", "path": str(target_csv)}

    return {"ok": True, "rows": len(rows), "max_date": max_d, "path": str(target_csv)}


def _fx_import_missing_usd_eur_from_csv(csv_path: FSPath) -> dict:
    """
    Import any missing USD->EUR FX dates from fx_ecb.csv into fx_rates.
    Does NOT overwrite existing days (preserves historical batches/auditability).
    """
    if not csv_path.exists():
        return {"ok": False, "skipped": "missing_csv", "path": str(csv_path)}

    inserted = 0
    skipped = 0
    errors = 0

    with SessionLocal() as session:
        try:
            ensure_fx_rates_schema(session)
        except Exception:
            pass

        # Existing coverage (fills gaps, not just tail)
        existing = set(
            r[0] for r in session.execute(
                text("SELECT date FROM fx_rates WHERE base='USD' AND quote='EUR'")
            ).fetchall()
        )

        bid = None
        try:
            bid = get_or_create_current_fx_batch_id(session)
        except Exception:
            bid = None

        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            rdr = csv.DictReader(f)
            if not rdr.fieldnames:
                return {"ok": False, "error": "fx_ecb.csv has no header", "path": str(csv_path)}

            hm = {h.strip().lower(): h for h in rdr.fieldnames}
            if "date" not in hm or "usd_per_eur" not in hm:
                return {"ok": False, "error": "fx_ecb.csv must include date, usd_per_eur", "path": str(csv_path)}

            for row in rdr:
                try:
                    d = (row.get(hm["date"]) or "").strip()
                    r = (row.get(hm["usd_per_eur"]) or "").strip()
                    if not d or not r:
                        raise ValueError("missing date/rate")

                    if d in existing:
                        skipped += 1
                        continue

                    usd_per_eur = Decimal(r)
                    if usd_per_eur <= 0:
                        raise ValueError("usd_per_eur <= 0")

                    eur_per_usd = (Decimal("1") / usd_per_eur)

                    session.execute(
                        text(
                            """
                            INSERT INTO fx_rates (date, base, quote, rate, batch_id)
                            VALUES (:d, 'USD', 'EUR', :r, :b)
                            ON CONFLICT(date, base, quote)
                            DO UPDATE SET rate=excluded.rate, batch_id=excluded.batch_id
                            """
                        ),
                        {"d": d, "r": str(eur_per_usd), "b": bid},
                    )
                    inserted += 1
                    existing.add(d)

                except (InvalidOperation, ValueError):
                    errors += 1
                    continue
                except Exception:
                    errors += 1
                    continue

        session.commit()

    return {"ok": True, "inserted": inserted, "skipped": skipped, "errors": errors, "path": str(csv_path)}


def _fx_autosync_tick(reason: str = "tick") -> dict:
    """
    One autosync iteration:
      - optionally refresh fx_ecb.csv from ECB
      - import missing FX dates into DB
    """
    if not _fx_autosync_enabled():
        return {"ok": True, "enabled": False, "reason": reason}

    csv_path = _fx_autosync_source_csv()
    refresh = None

    if _fx_autosync_fetch_ecb():
        refresh = _fx_refresh_fx_ecb_csv_from_ecb(csv_path)
        if refresh and not refresh.get("ok"):
            _fx_autosync_logger.warning("FX autosync: ECB refresh failed: %s", refresh)

    imp = _fx_import_missing_usd_eur_from_csv(csv_path)

    if imp.get("ok") and int(imp.get("inserted") or 0) > 0:
        _fx_autosync_logger.info(
            "FX autosync (%s): inserted=%s errors=%s source=%s",
            reason,
            imp.get("inserted"),
            imp.get("errors"),
            csv_path,
        )
    elif imp.get("ok"):
        _fx_autosync_logger.info("FX autosync (%s): no new rows (source=%s)", reason, csv_path)

    return {"ok": True, "enabled": True, "reason": reason, "refresh": refresh, "import": imp}


async def _fx_autosync_loop() -> None:
    interval = _fx_autosync_interval_seconds()
    _fx_autosync_logger.info(
        "FX autosync loop started (interval=%ss fetch_ecb=%s source=%s)",
        interval,
        _fx_autosync_fetch_ecb(),
        _fx_autosync_source_csv(),
    )

    while True:
        try:
            if _FX_AUTOSYNC_STOP is not None and _FX_AUTOSYNC_STOP.is_set():
                break

            # Run blocking file/DB work off the event loop
            await asyncio.to_thread(_fx_autosync_tick, "periodic")

            if _FX_AUTOSYNC_STOP is None:
                await asyncio.sleep(interval)
            else:
                try:
                    await asyncio.wait_for(_FX_AUTOSYNC_STOP.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            _fx_autosync_logger.warning("FX autosync loop error: %s", e)
            await asyncio.sleep(min(interval, 60))

    _fx_autosync_logger.info("FX autosync loop stopped")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Async lifespan context used by FastAPI to handle startup/shutdown events.
    Performs idempotent DB checks, initializes logs, and records startup diagnostics.
    """
    global _FX_AUTOSYNC_STOP, _FX_AUTOSYNC_TASK
    global _PRICE_AUTOSYNC_STOP, _PRICE_AUTOSYNC_TASK
    
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
            
        # Start price autosync loop (external provider) if enabled
        if price_autosync_enabled():
            _PRICE_AUTOSYNC_STOP = asyncio.Event()
            _PRICE_AUTOSYNC_TASK = asyncio.create_task(price_autosync_loop(_PRICE_AUTOSYNC_STOP))
            diag["price_autosync"] = {
                "enabled": True,
                "interval_seconds": price_autosync_interval_seconds(),
                "provider": os.getenv("PRICE_AUTOSYNC_PROVIDER", "auto"),
                "quote": os.getenv("PRICE_AUTOSYNC_QUOTE", "USDT"),
                "coingecko_vs": os.getenv("PRICE_AUTOSYNC_COINGECKO_VS_CURRENCY", "eur"),
            }
        else:
            diag["price_autosync"] = {"enabled": False}
            
        # Start FX autosync background loop (optional; enabled by FX_AUTOSYNC_ENABLED)
        if _fx_autosync_enabled():
            _FX_AUTOSYNC_STOP = asyncio.Event()
            _FX_AUTOSYNC_TASK = asyncio.create_task(_fx_autosync_loop())
            diag["fx_autosync"] = {
                "enabled": True,
                "interval_seconds": _fx_autosync_interval_seconds(),
                "fetch_ecb": _fx_autosync_fetch_ecb(),
                "source_csv": str(_fx_autosync_source_csv()),
            }
        else:
            diag["fx_autosync"] = {"enabled": False}

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
    
    # Stop FX autosync loop cleanly on shutdown
    try:
        if _FX_AUTOSYNC_STOP is not None:
            _FX_AUTOSYNC_STOP.set()
        if _FX_AUTOSYNC_TASK is not None:
            _FX_AUTOSYNC_TASK.cancel()
            try:
                await _FX_AUTOSYNC_TASK
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
    except Exception:
        pass
    
    # Stop Price autosync loop cleanly on shutdown
    try:
        if _PRICE_AUTOSYNC_STOP is not None:
            _PRICE_AUTOSYNC_STOP.set()
        if _PRICE_AUTOSYNC_TASK is not None:
            _PRICE_AUTOSYNC_TASK.cancel()
            try:
                await _PRICE_AUTOSYNC_TASK
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
    except Exception:
        pass

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

from .security import (
    _truthy_env,
    IS_PROD,
    ENABLE_ADMIN_ENDPOINTS,
    ENABLE_ADMIN_SCRIPTS,
    ALLOW_QUERY_TOKENS,
    ADMIN_HEADER_ONLY,
    ADMIN_ALLOW_REMOTE,
    ADMIN_TOKEN,
    BUNDLE_TOKEN,
    MAX_PREVIEW_BYTES,
    MAX_UPLOAD_BYTES,
)
from cryptotaxcalc.demo_mode import router as demo_router
from cryptotaxcalc.demo_builder import router as demo_build_router
from .admin_ops import router as admin_ops_router
from .routes.data_admin import router as data_admin_router
from .routes.csv_admin import router as csv_admin_router
from .routes.ops_admin import router as ops_admin_router
from .routes.ui import router as ui_router
from .routes.export_ui import router as export_ui_router

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
IMPORT_CSV_DEPRECATION_WARNING = '299 - "Deprecated; use /import/multiple"'


def _import_csv_warning_headers() -> dict[str, str]:
    return {"Warning": IMPORT_CSV_DEPRECATION_WARNING}


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
            CalcRun.tax_year,
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
        "tax_year": row.tax_year,
        "lot_method": row.lot_method,
        "fx_set_id": row.fx_set_id,
        "finished_at": _iso_utc(row.finished_at) if getattr(row, "finished_at", None) else None,
    }

    # End-to-end wall time (includes everything between CalcRun.start and finish).
    # This is the number users see in the wizard "Elapsed".
    try:
        if row.started_at and row.finished_at:
            dt_ms = int((row.finished_at - row.started_at).total_seconds() * 1000)
            if dt_ms >= 0:
                manifest["elapsed_wall_ms"] = dt_ms
    except Exception:
        pass

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


from .security import (
    _admin_not_found,
    require_admin,
    require_admin_scripts,
    require_bundle_admin,
)


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
        f"{tx.memo}|"
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


def _tax_context_for(
    jurisdiction: str,
    tax_year: int,
    local_area: str | None = None,
) -> Dict[str, Any]:
    """
    Compute tax context for a scope.

    Returns keys required by summary_filtered + subset exports:
      - tax_year_used
      - jurisdiction
      - national_rate
      - local_surtax_pct
      - local_rate
      - effective_rate
      - rate_model
      - local_area
    """
    j = (jurisdiction or "").strip().upper()

    # National base rates
    if j == "HR":
        national_rate = Decimal("0.12") if tax_year >= 2024 else Decimal("0.10")
    elif j == "IT":
        national_rate = Decimal("0.33") if tax_year >= 2026 else Decimal("0.26")
    else:
        national_rate = Decimal("0")

    local_area_code = (local_area or "").strip().upper()

    rate_model = "flat"
    local_surtax_pct = Decimal("0")

    # Croatia prirez applies only for tax years <= 2023 (abolished from 01.01.2024)
    if j == "HR" and tax_year <= 2023 and local_area_code:
        rate_model = "hr_prirez"
        HR_PRIREZ_2023 = {
            "ZAGREB": Decimal("0.18"),
            "SPLIT": Decimal("0.15"),
            "RIJEKA": Decimal("0.13"),
            "OSIJEK": Decimal("0.13"),
        }
        local_surtax_pct = HR_PRIREZ_2023.get(local_area_code, Decimal("0"))

    # prirez applies on tax amount → convert to an add-on rate
    local_rate = (national_rate * local_surtax_pct) if local_surtax_pct > 0 else Decimal("0")
    effective_rate = national_rate + local_rate

    return {
        "tax_year_used": int(tax_year),
        "jurisdiction": j,
        "national_rate": national_rate,
        "local_surtax_pct": local_surtax_pct,
        "local_rate": local_rate,
        "effective_rate": effective_rate,
        "rate_model": rate_model,
        "local_area": local_area_code,
    }


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


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Preserve /import/csv deprecation warning even when FastAPI rejects the request
    before the route function is entered, for example an empty multipart filename.
    """
    if request.url.path.endswith("/import/csv"):
        return JSONResponse(
            status_code=422,
            content={
                "results": [
                    {
                        "filename": "(no-name)",
                        "inserted": 0,
                        "skipped_duplicates": 0,
                        "skipped_errors": 1,
                        "errors": [
                            "Invalid CSV upload. Provide a .csv file using the multipart field named 'file'."
                        ],
                        "recognized_source_id": "unknown",
                        "recognized_source_name": "Unknown",
                        "recognized_source_status": "unsupported",
                        "recognized_source_confidence": 0.0,
                    }
                ],
                "meta": {
                    "min_year": None,
                    "max_year": None,
                },
            },
            headers=_import_csv_warning_headers(),
        )

    return await fastapi_request_validation_exception_handler(request, exc)


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
    logger = get_logger("app")
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
    _fx_autosync_tick("startup")
        
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
                "Croatia: crypto disposals are treated as capital income (capital gains).",
                "Rate: 10% through tax period 2023; 12% from tax period 2024 (final income rates increased when prirez was abolished).",
                "Local surtax (prirez): applies only to tax years ≤ 2023; abolished from 01.01.2024 (so local add-on is 0% for 2024+ in this model).",
                "Sources: Porezna uprava (tax changes 2023/2024) + Porezna uprava prirez table (2023).",
                "This is a technical EUR summary; mapping into JOPPD / forms requires professional review.",
            ],
        }

    if j == "IT":
        return {
            **base,
            "title": "Country Notes – Italy",
            "subtitle": "High-level context only.",
            "bullets": [
                "Italy: crypto gains are taxed via a flat substitute tax (no regional/municipal add-on applied in this model).",
                "Rate: 26% for tax period 2025; 33% from tax period 2026; €2,000 exemption removed from 2025.",
                "Sources: PwC Tax Summaries (Italy – taxation of cryptocurrencies).",
                "This is a technical EUR summary only; it does not replace Italian tax forms (e.g., Quadro RT/RW).",
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


@app.get("/export/status", summary="Explain why exports may be blocked")
def export_status(db: Session = Depends(get_db)):
    """
    User-facing explanation for export blocking.
    Psychology: users feel guided instead of punished when errors explain intent.
    """
    run = db.query(CalcRun).order_by(CalcRun.id.desc()).first()
    if not run:
        return {"export_allowed": True}

    summary = run.summary_json or {}
    warnings = summary.get("warnings", []) if isinstance(summary, dict) else []

    blockers = []

    for w in warnings:
        if isinstance(w, dict):
            if w.get("severity") == "blocker":
                blockers.append(w.get("message", "Data integrity issue detected."))
            continue

        try:
            s = str(w)
        except Exception:
            continue

        if s.startswith("BLOCKER:"):
            blockers.append(s)

    if not blockers:
        return {"export_allowed": True}

    return {
        "export_allowed": False,
        "title": "We need more history before exporting",
        "message": (
            "Some assets were sold without recorded acquisition history. "
            "Exporting now could result in incorrect tax calculations."
        ),
        "recommended_actions": [
            "Upload older CSVs from the same exchange",
            "Upload deposit or transfer history",
            "Ensure your dataset starts before your first sale",
        ],
        "blockers": blockers[:3],  # limit for UI clarity
    }


@app.get("/data_quality/missing_history", summary="Detect assets with missing acquisition history")
def missing_history(db: Session = Depends(get_db)):
    """
    Identify assets that were sold without acquisition history.

    Psychology:
    Users fix problems faster when the system tells them exactly
    what is missing, how much, and since when.
    """
    run = db.query(CalcRun).order_by(CalcRun.id.desc()).first()
    if not run or not isinstance(run.summary_json, dict):
        return {"assets": []}

    warnings = run.summary_json.get("warnings", [])
    if not isinstance(warnings, list):
        return {"assets": []}

    missing: dict[str, dict] = {}

    for w in warnings:
        if not isinstance(w, dict):
            continue
        if w.get("type") != "missing_history":
            continue

        asset = w.get("asset")
        if not asset:
            continue

        entry = missing.setdefault(
            asset,
            {
                "asset": asset,
                "missing_qty_total": 0,
                "first_seen_ts": None,
                "events": 0,
            },
        )

        try:
            entry["missing_qty_total"] += float(w.get("missing_qty", 0))
        except Exception:
            pass

        ts = w.get("timestamp")
        if ts:
            if entry["first_seen_ts"] is None or ts < entry["first_seen_ts"]:
                entry["first_seen_ts"] = ts

        entry["events"] += 1

    return {
        "assets": list(missing.values())
    }
    

@app.get("/wallet/out", response_model=list[WalletOutItem])
def list_wallet_outs(db: Session = Depends(get_db)):
    """
    List wallet OUT transactions that require user classification.
    """
    rows = (
        db.query(TransactionRow)
        .filter(TransactionRow.type == "TRANSFER")
        .filter(TransactionRow.base_amount < 0)
        .order_by(TransactionRow.timestamp.desc(), TransactionRow.id.desc())
        .all()
    )

    out = []
    for t in rows:
        out.append(
            WalletOutItem(
                transaction_id=t.id,
                timestamp=t.timestamp.isoformat(),
                asset=t.base_asset,
                amount=str(t.base_amount),
                exchange=t.exchange,
            )
        )

    return out


@app.post("/wallet/out/{transaction_id}/classify")
def classify_wallet_out(
    transaction_id: int,
    req: WalletTransferOverrideRequest,
    db: Session = Depends(get_db),
):
    """
    Set user intent for a wallet OUT transaction.
    """
    if req.classification in {"sell", "buy"} and not req.proceeds_eur:
        raise HTTPException(
            status_code=400,
            detail="proceeds_eur is required when classification is 'taxable'",
        )

    override = (
        db.query(WalletOutOverride)
        .filter(WalletOutOverride.transaction_id == transaction_id)
        .one_or_none()
    )

    if override is None:
        override = WalletOutOverride(
            transaction_id=transaction_id,
        )
        db.add(override)

    override.classification = req.classification
    override.proceeds_eur = req.proceeds_eur
    override.note = req.note

    db.commit()

    return {"ok": True}


@app.get("/wallet/transfers/grouped", response_model=list[WalletTransferFileGroup])
def wallet_transfers_grouped(db: Session = Depends(get_db)):
    """
    Group wallet transfers by original uploaded file (raw_event_id), split into IN and OUT.
    Newest -> oldest ordering.
    """
    txs = (
        db.query(TransactionRow)
        .filter(TransactionRow.type == "TRANSFER")
        .order_by(TransactionRow.timestamp.desc(), TransactionRow.id.desc())
        .all()
    )
    
    # Filter out dust transfers so they don't trigger the resolve modal on re-upload.
    # Value-aware: keep dust rows if their countervalue is material (>= €0.01).
    dust_cutoff = Decimal("0.00000001")
    value_cutoff_eur = Decimal("0.01")

    filtered = []
    for t in txs:
        try:
            amt = Decimal(str(t.base_amount))
            fv = t.fair_value if t.fair_value is not None else None

            is_dust = abs(amt) < dust_cutoff
            is_material_value = (fv is not None and Decimal(str(fv)) >= value_cutoff_eur)

            if is_dust and not is_material_value:
                continue
        except Exception:
            # If parsing fails, keep the row (conservative)
            pass

        filtered.append(t)

    txs = filtered

    # overrides (same table, now treated as transfer overrides)
    ovs = db.query(WalletOutOverride).all()
    ov_by_txid = {int(o.transaction_id): o for o in ovs if o.transaction_id is not None}

    raw_ids = sorted({int(t.raw_event_id) for t in txs if t.raw_event_id is not None})
    raw_name: dict[int, str] = {}
    if raw_ids:
        rows = (
            db.query(RawEvent.id, RawEvent.source_filename)
            .filter(RawEvent.id.in_(raw_ids))
            .all()
        )
        raw_name = {int(r[0]): str(r[1] or f"raw_event_{r[0]}") for r in rows}

    def _parse_cv_ticker(memo: str | None) -> str | None:
        if not memo:
            return None
        parts = [p.strip() for p in memo.split("|")]
        for p in parts:
            if p.startswith("cv_ticker="):
                return p.split("=", 1)[1].strip().upper() or None
        return None

    def _suggest_proceeds_eur(trow: TransactionRow) -> Decimal | None:
        fv = trow.fair_value
        if fv is None or fv == 0:
            return None
        cv = _parse_cv_ticker(trow.memo)
        if (cv or "").upper() == "EUR":
            return fv
        if (cv or "").upper() == "USD":
            from cryptotaxcalc.fx_utils import ensure_rate_or_default_lookup
            d = trow.timestamp.date()
            fx = ensure_rate_or_default_lookup(
                db,
                d,
                base="USD",
                quote="EUR",
                default_rate=Decimal("1.0"),
                max_lookback_days=7,
            )
            eur_per_usd = fx.rate if isinstance(fx.rate, Decimal) else Decimal(str(fx.rate))
            return (fv * eur_per_usd)
        return None

    grouped: dict[int, WalletTransferFileGroup] = {}

    for t in txs:
        if t.raw_event_id is None:
            continue
        rid = int(t.raw_event_id)
        fname = raw_name.get(rid, f"raw_event_{rid}")

        g = grouped.get(rid)
        if g is None:
            g = WalletTransferFileGroup(raw_event_id=rid, filename=fname, ins=[], outs=[])
            grouped[rid] = g

        cv_ticker = _parse_cv_ticker(t.memo)
        suggested = _suggest_proceeds_eur(t)
        suggested_str = None if suggested is None else str(suggested.quantize(Decimal("0.01")))

        ov = ov_by_txid.get(int(t.id))
        cls = (ov.classification or "transfer") if ov else "transfer"
        cls_norm = str(cls).strip().lower()
        if cls_norm not in {"transfer", "sell", "buy"}:
            cls_norm = "transfer"

        proceeds = None if not ov or ov.proceeds_eur is None else str(ov.proceeds_eur)

        row = WalletTransferRow(
            transaction_id=int(t.id),
            raw_event_id=rid,
            filename=fname,
            timestamp=t.timestamp.isoformat(),
            asset=t.base_asset,
            amount=str(t.base_amount),
            fair_value=(None if t.fair_value is None else str(t.fair_value)),
            cv_ticker=cv_ticker,
            classification=cls_norm,  # transfer/sell/buy
            proceeds_eur=proceeds,
            suggested_proceeds_eur=suggested_str,
        )

        if t.base_amount is not None and Decimal(str(t.base_amount)) < 0:
            g.outs.append(row)
        else:
            g.ins.append(row)

    out = list(grouped.values())
    out.sort(key=lambda x: x.filename.lower())
    return out


@app.post("/wallet/transfers/{raw_event_id}/batch_classify")
def wallet_transfers_batch_classify(
    raw_event_id: int,
    req: WalletTransferBatchRequest,
    db: Session = Depends(get_db),
):
    """
    Batch save classifications (transfer/sell/buy) for a specific uploaded file.
    Auto-fills proceeds_eur from EUR countervalue or USD->EUR FX if missing.
    """
    if int(req.raw_event_id) != int(raw_event_id):
        raise HTTPException(status_code=400, detail="raw_event_id mismatch")

    txs = (
        db.query(TransactionRow)
        .filter(TransactionRow.raw_event_id == raw_event_id)
        .filter(TransactionRow.type == "TRANSFER")
        .all()
    )
    tx_by_id = {int(t.id): t for t in txs}

    def _parse_cv_ticker(memo: str | None) -> str | None:
        if not memo:
            return None
        parts = [p.strip() for p in memo.split("|")]
        for p in parts:
            if p.startswith("cv_ticker="):
                return p.split("=", 1)[1].strip().upper() or None
        return None

    def _auto_proceeds_eur(trow: TransactionRow) -> Decimal | None:
        fv = trow.fair_value
        if fv is None or fv == 0:
            return None
        cv = _parse_cv_ticker(trow.memo)
        if (cv or "").upper() == "EUR":
            return fv
        if (cv or "").upper() == "USD":
            from cryptotaxcalc.fx_utils import ensure_rate_or_default_lookup
            d = trow.timestamp.date()
            fx = ensure_rate_or_default_lookup(
                db,
                d,
                base="USD",
                quote="EUR",
                default_rate=Decimal("1.0"),
                max_lookback_days=7,
            )
            eur_per_usd = fx.rate if isinstance(fx.rate, Decimal) else Decimal(str(fx.rate))
            return (fv * eur_per_usd)
        return None

    for item in req.items:
        txid = int(item.transaction_id)
        trow = tx_by_id.get(txid)
        if trow is None:
            continue

        cls = (item.classification or "").strip().lower()
        if cls not in {"transfer", "sell", "buy"}:
            raise HTTPException(status_code=400, detail=f"Invalid classification for tx_id={txid}")

        proceeds = item.proceeds_eur
        if cls in {"sell", "buy"} and (proceeds is None or not str(proceeds).strip()):
            auto = _auto_proceeds_eur(trow)
            if auto is not None:
                proceeds = str(auto.quantize(Decimal("0.01")))
            else:
                raise HTTPException(status_code=400, detail=f"proceeds_eur required for tx_id={txid}")

        ov = (
            db.query(WalletOutOverride)
            .filter(WalletOutOverride.transaction_id == txid)
            .one_or_none()
        )
        if ov is None:
            ov = WalletOutOverride(transaction_id=txid)
            db.add(ov)

        ov.classification = cls
        ov.proceeds_eur = proceeds
        ov.note = item.note

    db.commit()
    return {"ok": True}


@app.get("/wallet/transfers/{raw_event_id}/export_resolved.csv")
def export_resolved_transfers_csv(raw_event_id: int, db: Session = Depends(get_db)):
    """
    Download an updated version of the ORIGINAL Ledger Operations CSV:
    - OUT classified sell -> Operation Type = SELL, countervalue set to EUR proceeds
    - IN classified buy  -> Operation Type = BUY,  countervalue set to EUR proceeds
    Preserves original layout and row order.
    """
    raw = db.query(RawEvent).filter(RawEvent.id == raw_event_id).one_or_none()
    if raw is None or not raw.blob_path:
        raise HTTPException(status_code=404, detail="raw_event not found or missing blob_path")

    txs = (
        db.query(TransactionRow)
        .filter(TransactionRow.raw_event_id == raw_event_id)
        .filter(TransactionRow.type == "TRANSFER")
        .all()
    )

    def _op_hash(memo: str | None) -> str | None:
        if not memo:
            return None
        parts = [p.strip() for p in memo.split("|")]
        for p in parts:
            if p.startswith("hash="):
                return p.split("=", 1)[1].strip() or None
        return None

    ovs = db.query(WalletOutOverride).all()
    ov_by_txid = {int(o.transaction_id): o for o in ovs if o.transaction_id is not None}

    # Build update maps:
    # 1) by hash when available
    # 2) fallback key when hash is missing
    updates_by_hash: dict[tuple[str, str, str, str, str, str, str], list[tuple[str, str]]] = {}
    updates_by_key: dict[tuple[int, str, str, str, str, str, str], list[tuple[str, str]]] = {}

    def _account_name(memo: str | None) -> str:
        if not memo:
            return ""
        for p in [x.strip() for x in memo.split("|")]:
            if p.startswith("account="):
                return p.split("=", 1)[1].strip()
        return ""

    def _norm_amt(x) -> str:
        try:
            return str(abs(Decimal(str(x))))
        except Exception:
            return str(x or "")

    def _norm_fee(x) -> str:
        try:
            d = Decimal(str(x))
            if d <= 0:
                return "0"
            return str(d)
        except Exception:
            return "0"
        
    def _norm_cv(x) -> str:
        try:
            s = str(x).strip()
            if not s:
                return ""
            s = s.replace(",", ".")
            return str(Decimal(s))
        except Exception:
            return ""

    for t in txs:
        ov = ov_by_txid.get(int(t.id))
        if not ov:
            continue

        cls = str(ov.classification or "").strip().lower()
        if cls not in {"sell", "buy"}:
            continue

        if not ov.proceeds_eur:
            continue

        new_type = cls.upper()
        proceeds = str(ov.proceeds_eur)

        h = _op_hash(t.memo)

        # Timestamp key (seconds)
        ts_sec = int(t.timestamp.replace(tzinfo=timezone.utc).timestamp())

        asset = str(t.base_asset or "").strip().upper()
        amt = _norm_amt(t.base_amount)
        fee = _norm_fee(t.fee_amount)
        acc = _account_name(t.memo)
        cv = _norm_cv(t.fair_value)

        op = "OUT" if (t.base_amount is not None and Decimal(str(t.base_amount)) < 0) else "IN"

        if h:
            updates_by_hash.setdefault((h, op, asset, amt, fee, acc, cv), []).append((new_type, proceeds))
        else:
            updates_by_key.setdefault((ts_sec, op, asset, amt, fee, acc, cv), []).append((new_type, proceeds))

    import csv as _csv
    from io import StringIO
    from pathlib import Path as _Path

    path = _Path(str(raw.blob_path))
    if not path.exists():
        raise HTTPException(status_code=404, detail="source CSV file not found on disk")

    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
        rdr = _csv.DictReader(f)
        if not rdr.fieldnames:
            raise HTTPException(status_code=400, detail="CSV has no headers")
        rows = list(rdr)
        fieldnames = list(rdr.fieldnames)

    def _parse_row_ts_sec(s: str) -> int | None:
        try:
            # CSV is ISO like 2025-12-03T08:28:59.000Z
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None

    for row in rows:
        op = (row.get("Operation Type") or "").strip().upper()
        if op not in {"IN", "OUT"}:
            continue
        
        h = (row.get("Operation Hash") or "").strip()

        # Always compute the row’s identity fields first (used for both hash and fallback matching)
        asset = (row.get("Currency Ticker") or "").strip().upper()
        amt = _norm_amt(row.get("Operation Amount"))
        fee = _norm_fee(row.get("Operation Fees"))
        acc = (row.get("Account Name") or "").strip()
        cv = _norm_cv(row.get("Countervalue at Operation Date"))

        lst = None

        if h:
            lst = updates_by_hash.get((h, op, asset, amt, fee, acc, cv))

        if not lst:
            ts_raw = (row.get("Operation Date") or "").strip()
            ts_sec = _parse_row_ts_sec(ts_raw)
            if ts_sec is None:
                continue
            lst = updates_by_key.get((ts_sec, op, asset, amt, fee, acc, cv))

        if not lst:
            continue

        new_type, proceeds = lst.pop(0)

        row["Operation Type"] = new_type
        row["Countervalue Ticker"] = "EUR"
        row["Countervalue at Operation Date"] = proceeds
        if "Countervalue at CSV Export" in row:
            row["Countervalue at CSV Export"] = proceeds

    buf = StringIO()
    w = _csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    w.writeheader()
    for row in rows:
        w.writerow(row)

    out_bytes = buf.getvalue().encode("utf-8")

    filename = (raw.source_filename or f"ledger_{raw_event_id}.csv").replace(".csv", "_resolved.csv")

    # Starlette encodes headers as latin-1; ensure ASCII-safe fallback filename.
    safe_name = "".join(ch if (32 <= ord(ch) < 127 and ch not in {'"', "\\", "\r", "\n"}) else "_" for ch in filename)
    if not safe_name.lower().endswith(".csv"):
        safe_name += ".csv"

    # RFC5987 filename* supports UTF-8 via percent-encoding (ASCII-only header value).
    import urllib.parse
    fn_star = urllib.parse.quote(filename, safe="")

    headers = {
        "Content-Disposition": f'attachment; filename="{safe_name}"; filename*=UTF-8\'\'{fn_star}'
    }

    return Response(content=out_bytes, media_type="text/csv; charset=utf-8", headers=headers)


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
        "latest_rate": str(latest_rate) if latest_rate is not None else None,
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


@app.post("/import/csv", response_model=ImportCSVResponse)
async def import_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    DEPRECATED: Use /import/multiple instead. This wrapper calls the same logic.
    """
    # Reuse the same function you use inside /import/multiple for a single file.
    # NOTE: /import/multiple is async, so we await it.
    result = await import_multiple([file], reset=False)

    # Add a gentle deprecation warning header.
    return JSONResponse(
        result,
        headers=_import_csv_warning_headers(),
    )


@app.post("/data_quality/precheck", response_model=PrecheckResponse)
async def precheck_import(
    files: List[UploadFile] = File(...),
):
    """
    Pre-import diagnostic to detect disposals without acquisition history.

    Returns:
    - global aggregate issues
    - per-file issues (so UI can badge the offending file)
    """
    from collections import defaultdict
    from .csv_normalizer import parse_csv_stream_with_meta

    def _guidance_for_sources(srcs: set[str]) -> str:
        if srcs == {"binance_spot_trades"}:
            return (
                "This looks like a Binance Spot Trades export. Spot trades typically do not include deposits or transfers. "
                "To fix missing acquisition history, also upload Binance Deposits/Withdrawals (or earlier Spot Trades) "
                "covering when you acquired this asset."
            )
        return (
            "This file set appears to be trade history only. Trades often exclude deposits/transfers. "
            "To fix missing acquisition history, also upload deposit/withdrawal/transfer history (or earlier trading history) "
            "from the same exchange or wallet covering when you acquired this asset."
        )

    def _compute_issues(
        buy_qty: dict[str, Decimal],
        sell_qty: dict[str, Decimal],
        first_sell_ts: dict[str, str],
        sources: set[str],
    ) -> list[PrecheckAssetIssue]:
        out: list[PrecheckAssetIssue] = []
        for asset, total_sold in sell_qty.items():
            total_bought = buy_qty.get(asset, Decimal("0"))
            if total_sold > total_bought:
                missing = total_sold - total_bought
                out.append(
                    PrecheckAssetIssue(
                        asset=asset,
                        first_sell_ts=first_sell_ts.get(asset),
                        total_sell_qty=str(total_sold),
                        reason=(
                            f"Total sells/disposals exceed total buys in uploaded files "
                            f"(missing at least {missing} {asset} acquisition history)"
                        ),
                        guidance=_guidance_for_sources(sources),
                    )
                )
        return out

    # Global aggregates
    g_buy = defaultdict(Decimal)
    g_sell = defaultdict(Decimal)
    g_first_sell_ts: dict[str, str] = {}
    g_sources: set[str] = set()

    # We collect per-file aggregates first, then compute:
    #  - global missing-assets list
    #  - per-file "contribution" issues (files that contain disposals of missing assets)
    file_stats: list[dict] = []

    for file in files:
        await file.seek(0)
        text_stream = io.TextIOWrapper(
            file.file,
            encoding="utf-8-sig",
            errors="replace",
            newline="",
        )

        try:
            rows, _errors, _meta = parse_csv_stream_with_meta(
                text_stream,
                filename=file.filename or "(no-name)",
            )
        finally:
            try:
                text_stream.detach()
            except Exception:
                pass

        # Per-file aggregates
        f_buy = defaultdict(Decimal)
        f_sell = defaultdict(Decimal)
        f_first_sell_ts: dict[str, str] = {}
        f_sources: set[str] = set()

        try:
            src_id = None
            if isinstance(_meta, dict):
                src_id = _meta.get("recognized_source_id")
            if src_id:
                f_sources.add(str(src_id))
                g_sources.add(str(src_id))
        except Exception:
            pass

        for tx in rows:
            asset = (tx.base_asset or "").upper().strip()
            qty = tx.base_amount
            ts = tx.timestamp.isoformat()
            ttype = (tx.type or "").strip().lower()

            # Treat:
            # - buy as acquisition
            # - sell as disposal
            # - trade as disposal of base (many exchange exports normalize swaps to "trade")
            if ttype == "buy":
                f_buy[asset] += qty
                g_buy[asset] += qty

            elif ttype in {"sell", "trade"}:
                f_sell[asset] += qty
                g_sell[asset] += qty

                if asset not in f_first_sell_ts or ts < f_first_sell_ts[asset]:
                    f_first_sell_ts[asset] = ts
                if asset not in g_first_sell_ts or ts < g_first_sell_ts[asset]:
                    g_first_sell_ts[asset] = ts

        file_stats.append(
            dict(
                filename=(file.filename or "(no-name)"),
                buy=f_buy,
                sell=f_sell,
                first_sell_ts=f_first_sell_ts,
                sources=f_sources,
            )
        )

    # Global issues (source of truth)
    g_list = _compute_issues(g_buy, g_sell, g_first_sell_ts, g_sources)
    missing_assets = {i.asset for i in g_list}

    # Per-file contribution issues: mark files that contain disposals of globally-missing assets.
    file_issues: list[PrecheckFileIssue] = []
    for st in file_stats:
        fn = str(st["filename"])
        f_sell = st["sell"]
        f_first_sell_ts = st["first_sell_ts"]
        f_sources = st["sources"]

        contrib: list[PrecheckAssetIssue] = []
        if missing_assets:
            multi = len(file_stats) > 1
            for asset in sorted(missing_assets):
                sold_here = f_sell.get(asset, Decimal("0"))
                if sold_here <= 0:
                    continue

                bought_here = st["buy"].get(asset, Decimal("0"))

                # Behavior:
                # - Single file: if it sells a missing asset, it must be flagged.
                # - Multiple files: flag only the file that is locally deficit for that asset.
                is_deficit = sold_here > bought_here
                if (not multi) or is_deficit:
                    contrib.append(
                        PrecheckAssetIssue(
                            asset=asset,
                            first_sell_ts=f_first_sell_ts.get(asset),
                            total_sell_qty=str(sold_here),
                            reason=(
                                "This file contains disposals exceeding acquisitions for an asset "
                                "whose acquisition history is incomplete in the uploaded set."
                                if multi else
                                "This file contains disposals for an asset without sufficient acquisition history."
                            ),
                            guidance=_guidance_for_sources(set(f_sources) or set(g_sources)),
                        )
                    )

        file_issues.append(
            PrecheckFileIssue(
                filename=fn,
                issues_detected=bool(contrib),
                assets=contrib,
            )
        )

    return PrecheckResponse(
        issues_detected=bool(g_list),
        assets=g_list,
        files=file_issues,
    )


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

    # Batch preflight: parse every file before any transaction rows are deleted
    # or inserted. This makes /import/multiple atomic at the batch level:
    # if any file has validation errors, no file in the same request is imported.
    #
    # Keep one result entry per uploaded file so the UI can map validation cards
    # back to the original upload order.
    preflight_results: list[dict[str, Any]] = []
    preflight_has_errors = False

    for file in files:
        filename = file.filename or "(no-name)"

        if not filename.lower().endswith(".csv"):
            preflight_has_errors = True
            preflight_results.append({
                "filename": filename,
                "inserted": 0,
                "skipped_duplicates": 0,
                "skipped_errors": 1,
                "errors": ["Only .csv files are supported"],
                "recognized_source_id": "unknown",
                "recognized_source_name": "Unknown",
                "recognized_source_status": "unsupported",
                "recognized_source_confidence": 0.0,
            })
            try:
                await file.seek(0)
            except Exception:
                pass
            continue

        try:
            await file.seek(0)
            text_stream = io.TextIOWrapper(file.file, encoding="utf-8-sig", errors="replace", newline="")
            try:
                valid_rows, parse_errors, csv_meta = parse_csv_stream_with_meta(
                    text_stream,
                    filename=filename,
                )
            finally:
                try:
                    text_stream.detach()
                except Exception:
                    pass

            if parse_errors:
                preflight_has_errors = True
                preflight_results.append({
                    "filename": filename,
                    "inserted": 0,
                    "skipped_duplicates": 0,
                    "skipped_errors": len(parse_errors),
                    "errors": parse_errors[:20],
                    **(csv_meta or {}),
                })
            else:
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

                preflight_results.append({
                    "filename": filename,
                    "inserted": 0,
                    "skipped_duplicates": 0,
                    "skipped_errors": 0,
                    "min_year": file_min_year,
                    "max_year": file_max_year,
                    **(csv_meta or {}),
                })

        except CSVFormatError as e:
            preflight_has_errors = True
            preflight_results.append({
                "filename": filename,
                "inserted": 0,
                "skipped_duplicates": 0,
                "skipped_errors": 1,
                "errors": [str(e)],
                **(e.meta or {}),
            })

        except Exception as e:
            preflight_has_errors = True
            preflight_results.append({
                "filename": filename,
                "inserted": 0,
                "skipped_duplicates": 0,
                "skipped_errors": 1,
                "errors": [f"Failed to parse CSV: {e}"],
            })

        finally:
            try:
                await file.seek(0)
            except Exception:
                pass

    if preflight_has_errors:
        return {
            "results": preflight_results,
            "meta": {
                "min_year": None,
                "max_year": None,
            },
        }

    # For workspace / paid flows we call /import/multiple?reset=1 so that
    # the new dataset replaces any previous uploads. In demo mode we keep
    # the same behavior as before.
    if reset or is_demo_mode_enabled():
        with SessionLocal() as session:
            # Clear transactions and any saved wallet transfer overrides.
            # Otherwise old overrides can be misapplied after resets (SQLite ID reuse).
            session.query(TransactionRow).delete()
            session.query(WalletOutOverride).delete()
            session.commit()

    for file in files:
        filename = file.filename or "(no-name)"
        inserted = 0
        skipped_duplicates = 0
        skipped_errors = 0

        if not filename.lower().endswith(".csv"):
            results.append({
                "filename": filename,
                "inserted": 0,
                "skipped_duplicates": 0,
                "skipped_errors": 1,
                "errors": ["Only .csv files are supported"],
                "recognized_source_id": "unknown",
                "recognized_source_name": "Unknown",
                "recognized_source_status": "unsupported",
                "recognized_source_confidence": 0.0,
            })
            continue

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

            if parse_errors:
                results.append({
                    "filename": filename,
                    "inserted": 0,
                    "skipped_duplicates": 0,
                    "skipped_errors": len(parse_errors),
                    "errors": parse_errors[:20],
                    **(csv_meta or {}),
                })
                continue

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
        _export_block_if_blockers(warnings)

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
        _audit("calc:error", {"detail": "HTTPException"})
        raise

    except ValueError as e:
        # Input/data/config problem (client-side), not a server crash.
        logger.warning(
            "Calculation rejected due to input/config error (run_id=%s): %s",
            getattr(run, "id", None),
            str(e),
        )
        try:
            run.finished_at = datetime.now(timezone.utc)
            run.summary_json = {"status": "error", "error": str(e)}
            db.add(run)
            db.commit()
        except Exception:
            db.rollback()
        _audit("calc:error", {"detail": str(e)})
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        logger.exception("Calculation failed (run_id=%s).", getattr(run, "id", None))
        try:
            db.rollback()
        except Exception:
            pass

        # Close the run so it doesn't look "stuck" in history/UIs.
        try:
            run.finished_at = datetime.now(timezone.utc)
            run.summary_json = {"status": "error", "error": str(e)}
            db.add(run)
            db.commit()
        except Exception:
            db.rollback()

        _audit("calc:error", {"detail": str(e)})
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
                try:
                    proceeds_eur += usd_to_eur(e.proceeds, dt.date(), db=session)
                    basis_eur    += usd_to_eur(e.cost_basis, dt.date(), db=session)
                    gain_eur     += usd_to_eur(e.gain, dt.date(), db=session)
                except Exception:
                    notes.append(f"Bad USD->EUR FX conversion for {dt.date()}; skipped conversion for an event.")
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


def _export_block_if_blockers(warnings: list[str | dict] | None) -> None:
    """
    Block exports if the run contains data-integrity blockers.

    Supports:
    - legacy string warnings
    - structured dict warnings (preferred)
    """
    w = warnings or []

    blockers: list[str] = []

    for item in w:
        # New structured warning
        if isinstance(item, dict):
            if item.get("severity") == "blocker":
                msg = item.get("message") or "Data integrity issue detected."
                blockers.append(msg)
            continue

        # Legacy string warning
        try:
            s = str(item)
        except Exception:
            continue

        if s.startswith("BLOCKER:"):
            blockers.append(s)

    if blockers:
        sample = " | ".join(blockers[:2])
        raise HTTPException(
            status_code=409,
            detail={
                "title": "Export blocked to protect your tax results",
                "reason": "Some assets were sold without any recorded acquisition history.",
                "what_this_means": (
                    "Without knowing how you acquired these assets, the system would "
                    "assume a zero cost basis, which could significantly overstate taxes."
                ),
                "how_to_fix": [
                    "Import earlier trades from the same exchange",
                    "Import deposit or transfer history",
                    "Ensure CSVs cover your full trading history",
                ],
                "technical_details": sample,
            },
        )
        

# -----------------------------------------------------------------------------
# PDF export cache (disk)
PDF_CACHE_VERSION = "ws_pdf_v1"
PDF_CACHE_DIR = (PROJECT_ROOT / "artifacts" / "pdf_cache")
PDF_CACHE_MAX_FILES = 250  # keep it bounded for demo EXE + local use

def _pdf_cache_key(kind: str, parts: dict[str, Any]) -> str:
    """
    Deterministic cache key: stable across restarts, invalidated by PDF_CACHE_VERSION.
    """
    # Stable order for hashing
    payload = {"v": PDF_CACHE_VERSION, "kind": kind, **parts}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()

def _pdf_cache_path(key: str) -> Path:
    return PDF_CACHE_DIR / f"{key}.pdf"

def _pdf_cache_get(key: str) -> bytes | None:
    try:
        p = _pdf_cache_path(key)
        if p.exists() and p.is_file():
            return p.read_bytes()
    except Exception:
        return None
    return None

def _pdf_cache_prune() -> None:
    try:
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        files = [p for p in PDF_CACHE_DIR.glob("*.pdf") if p.is_file()]
        if len(files) <= PDF_CACHE_MAX_FILES:
            return
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for p in files[PDF_CACHE_MAX_FILES:]:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass

def _pdf_cache_put(key: str, data: bytes) -> None:
    try:
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = PDF_CACHE_DIR / f".{key}.tmp"
        dst = _pdf_cache_path(key)
        tmp.write_bytes(data)
        tmp.replace(dst)
        _pdf_cache_prune()
    except Exception:
        pass
    
    
# -----------------------------------------------------------------------------
# PDF generation jobs (in-memory; demo-friendly)
PDF_JOB_TTL_SECONDS = 20 * 60  # 20 minutes
_pdf_jobs: dict[str, dict[str, Any]] = {}

def _pdf_job_prune() -> None:
    try:
        now = time.time()
        dead = [k for k, v in _pdf_jobs.items() if (now - float(v.get("ts", now))) > PDF_JOB_TTL_SECONDS]
        for k in dead:
            _pdf_jobs.pop(k, None)
    except Exception:
        pass

def _pdf_job_new(payload: dict[str, Any]) -> str:
    _pdf_job_prune()
    job_id = uuid.uuid4().hex
    _pdf_jobs[job_id] = {
        "ts": time.time(),
        "status": "queued",
        "message": "Queued",
        "progress": 0.0,
        "payload": payload,
        "pdf_url": None,
        "cache": None,
        "error": None,
    }
    return job_id

@app.get("/export/pdf_job/{job_id}", tags=["export"])
def export_pdf_job_status(job_id: str):
    _pdf_job_prune()
    job = _pdf_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "message": job.get("message"),
        "progress": job.get("progress"),
        "pdf_url": job.get("pdf_url"),
        "cache": job.get("cache"),
        "error": job.get("error"),
    }


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
    _export_block_if_blockers(warnings)

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
    _export_block_if_blockers(warnings)

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
    _export_block_if_blockers(warnings)

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
    force: bool = Query(False, description="Proceed even if data-integrity blockers are present"),
    download: bool = Query(False, description="Download as attachment instead of inline preview"),
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
    if not force:
        _export_block_if_blockers(warnings)
    cache_key = _pdf_cache_key(
        "workspace_summary_full",
        {
            "run_db_id": int(run_db_id),
            "run_id": str(run.run_id or run.id),
            "jur": str(run.jurisdiction or ""),
            "rule": str(run.rule_version or ""),
            "tax_year": int(run.tax_year) if getattr(run, "tax_year", None) else None,
            "force": bool(force),
        },
    )
    cached = _pdf_cache_get(cache_key)
    if cached is not None:
        filename = f"workspace_summary_run_{run_db_id}.pdf"
        disp = "attachment" if download else "inline"
        return StreamingResponse(
            BytesIO(cached),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'{disp}; filename="{filename}"',
                "X-Cache": "HIT",
            },
        )
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

    _pdf_cache_put(cache_key, pdf_bytes)

    filename = f"workspace_summary_run_{run_db_id}.pdf"
    disp = "attachment" if download else "inline"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'{disp}; filename="{filename}"',
            "X-Cache": "MISS",
        },
    )
    

@app.get(
    "/export/workspace_summary/{run_db_id}/subset.pdf",
    summary="Export a Workspace summary PDF for the current workspace filter.",
)
def export_workspace_summary_subset(
    run_db_id: int,
    year: int | None = Query(None, description="Optional tax-year filter (YYYY)"),
    asset: str | None = Query(None, description="Optional asset filter (e.g. BTC)"),
    local_area: str | None = Query(None, description="Optional local area code (e.g. ZAGREB) for HR prirez ≤ 2023"),
    force: bool = Query(False, description="Proceed even if data-integrity blockers are present"),
    download: bool = Query(False, description="Download as attachment instead of inline preview"),
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
        
    if not force:
        _export_block_if_blockers(run_warnings)

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

    tax_year_used = int(year) if year is not None else int(getattr(run, "tax_year", 0) or datetime.utcnow().year)
    tax_ctx = _tax_context_for(jurisdiction=run.jurisdiction, tax_year=tax_year_used, local_area=local_area)

    taxable_base_eur = taxable if taxable > 0 else Decimal("0")
    tax_due_eur = (taxable_base_eur * tax_ctx["effective_rate"]).quantize(Decimal("0.01"))

    subset_totals = {
        "proceeds_eur": dec_to_str(proceeds),
        "cost_eur": dec_to_str(cost),
        "gain_eur": dec_to_str(total_gain),
        "taxable_gain_eur": dec_to_str(taxable),
        "exempt_gain_eur": dec_to_str(exempt),

        "tax_year_used": str(tax_ctx["tax_year_used"]),
        "national_rate": str(tax_ctx["national_rate"]),
        "local_rate": str(tax_ctx["local_rate"]),
        "effective_rate": str(tax_ctx["effective_rate"]),
        "tax_due_eur": str(tax_due_eur),

        "rate_model": str(tax_ctx["rate_model"]),
        "local_surtax_pct": str(tax_ctx["local_surtax_pct"]),
        "local_area": str(tax_ctx["local_area"]),
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
    
    cache_key = _pdf_cache_key(
        "workspace_summary_subset",
        {
            "run_db_id": int(run_db_id),
            "run_id": str(run.run_id or run.id),
            "asset": str(asset_label),
            "year": str(year_label),
            "local_area": str(local_area or ""),
            "force": bool(force),
        },
    )
    cached = _pdf_cache_get(cache_key)
    if cached is not None:
        filename = f"workspace_summary_run_{run_db_id}_{fname_asset}_{fname_year}.pdf"
        disp = "attachment" if download else "inline"
        return StreamingResponse(
            BytesIO(cached),
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'{disp}; filename="{filename}"',
                "X-Cache": "HIT",
            },
        )

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
    disp = "attachment" if download else "inline"
    return StreamingResponse(
        BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'{disp}; filename="{filename}"'},
    )


@app.post("/export/workspace_summary/{run_db_id}.pdf/job", tags=["export"])
async def export_workspace_summary_job(
    run_db_id: int,
    force: bool = Query(False),
    download: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Start PDF generation in background. Uses existing export endpoint logic so
    PDF content/caching rules stay identical.
    """
    _pdf_job_prune()

    # Build the final URL the client will open when ready
    pdf_url = f"/export/workspace_summary/{run_db_id}.pdf?force={'1' if force else '0'}&download={'1' if download else '0'}"

    # If cached already, return ready immediately (fast path)
    run = db.query(CalcRun).filter(CalcRun.id == run_db_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    summary = getattr(run, "summary_json", None)
    if not summary:
        raise HTTPException(status_code=400, detail="Run has no summary_json — cannot export workspace PDF.")

    # Match your cache-key inputs used in export_workspace_summary (so HIT works)
    warnings = summary.get("warnings", []) if isinstance(summary, dict) else []
    if not force:
        _export_block_if_blockers(warnings)

    cache_key = _pdf_cache_key(
        "workspace_summary_full",
        {
            "run_db_id": int(run_db_id),
            "run_id": str(getattr(run, "run_id", None) or run.id),
            "jur": str(getattr(run, "jurisdiction", "") or ""),
            "rule": str(getattr(run, "rule_version", "") or ""),
            "tax_year": int(getattr(run, "tax_year", 0) or 0),
            "force": bool(force),
        },
    )
    if _pdf_cache_get(cache_key) is not None:
        job_id = _pdf_job_new({"kind": "full", "run_db_id": run_db_id})
        _pdf_jobs[job_id]["status"] = "ready"
        _pdf_jobs[job_id]["message"] = "Ready (cached)"
        _pdf_jobs[job_id]["progress"] = 1.0
        _pdf_jobs[job_id]["pdf_url"] = pdf_url
        _pdf_jobs[job_id]["cache"] = "HIT"
        return {"job_id": job_id, "status": "ready", "pdf_url": pdf_url, "cache": "HIT"}

    job_id = _pdf_job_new({"kind": "full", "run_db_id": run_db_id, "force": force, "download": download})
    _pdf_jobs[job_id]["status"] = "running"
    _pdf_jobs[job_id]["message"] = "Generating PDF…"
    _pdf_jobs[job_id]["progress"] = 0.05

    async def _work():
        try:
            def _run_export():
                # IMPORTANT: use a fresh DB session in the background thread.
                # The request-scoped session from Depends(get_db) will be closed.
                with SessionLocal() as s:
                    export_workspace_summary(run_db_id=run_db_id, force=force, download=False, db=s)

            await anyio.to_thread.run_sync(_run_export)

            _pdf_jobs[job_id]["status"] = "ready"
            _pdf_jobs[job_id]["message"] = "Ready"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["pdf_url"] = pdf_url
            _pdf_jobs[job_id]["cache"] = "MISS"
        except HTTPException as e:
            _pdf_jobs[job_id]["status"] = "error"
            _pdf_jobs[job_id]["message"] = "Failed"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["error"] = str(e.detail)
        except Exception as e:
            _pdf_jobs[job_id]["status"] = "error"
            _pdf_jobs[job_id]["message"] = "Failed"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["error"] = str(e)

    asyncio.create_task(_work())
    return {"job_id": job_id, "status": "running"}


@app.post("/export/workspace_summary/{run_db_id}/subset.pdf/job", tags=["export"])
async def export_workspace_summary_subset_job(
    run_db_id: int,
    year: int | None = Query(None),
    asset: str | None = Query(None),
    local_area: str | None = Query(None),
    force: bool = Query(False),
    download: bool = Query(False),
    db: Session = Depends(get_db),
):
    _pdf_job_prune()

    qs = f"?year={year or ''}&asset={(asset or '')}&local_area={(local_area or '')}&force={'1' if force else '0'}&download={'1' if download else '0'}"
    pdf_url = f"/export/workspace_summary/{run_db_id}/subset.pdf{qs}"

    run = db.query(CalcRun).filter(CalcRun.id == run_db_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    raw_summary = getattr(run, "summary_json", None)
    summary = raw_summary if isinstance(raw_summary, dict) else None
    if not summary and raw_summary:
        try:
            summary = json.loads(raw_summary)
        except Exception:
            summary = None
    if not summary:
        raise HTTPException(status_code=400, detail="Run has no summary_json — cannot export workspace PDF.")

    run_warnings = summary.get("warnings", []) if isinstance(summary, dict) else []
    if not force:
        _export_block_if_blockers(run_warnings)

    asset_label = (asset or "").strip().upper() or "ALL"
    year_label = str(year) if year else "ALL"

    cache_key = _pdf_cache_key(
        "workspace_summary_subset",
        {
            "run_db_id": int(run_db_id),
            "run_id": str(getattr(run, "run_id", None) or run.id),
            "asset": str(asset_label),
            "year": str(year_label),
            "local_area": str(local_area or ""),
            "force": bool(force),
        },
    )
    if _pdf_cache_get(cache_key) is not None:
        job_id = _pdf_job_new({"kind": "subset", "run_db_id": run_db_id})
        _pdf_jobs[job_id]["status"] = "ready"
        _pdf_jobs[job_id]["message"] = "Ready (cached)"
        _pdf_jobs[job_id]["progress"] = 1.0
        _pdf_jobs[job_id]["pdf_url"] = pdf_url
        _pdf_jobs[job_id]["cache"] = "HIT"
        return {"job_id": job_id, "status": "ready", "pdf_url": pdf_url, "cache": "HIT"}

    job_id = _pdf_job_new({"kind": "subset", "run_db_id": run_db_id, "force": force, "download": download})
    _pdf_jobs[job_id]["status"] = "running"
    _pdf_jobs[job_id]["message"] = "Generating PDF…"
    _pdf_jobs[job_id]["progress"] = 0.05

    async def _work():
        try:
            def _run_export():
                # IMPORTANT: use a fresh DB session in the background thread.
                with SessionLocal() as s:
                    export_workspace_summary_subset(
                        run_db_id=run_db_id,
                        year=year,
                        asset=asset,
                        local_area=local_area,
                        force=force,
                        download=False,
                        db=s,
                    )

            await anyio.to_thread.run_sync(_run_export)

            _pdf_jobs[job_id]["status"] = "ready"
            _pdf_jobs[job_id]["message"] = "Ready"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["pdf_url"] = pdf_url
            _pdf_jobs[job_id]["cache"] = "MISS"
        except HTTPException as e:
            _pdf_jobs[job_id]["status"] = "error"
            _pdf_jobs[job_id]["message"] = "Failed"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["error"] = str(e.detail)
        except Exception as e:
            _pdf_jobs[job_id]["status"] = "error"
            _pdf_jobs[job_id]["message"] = "Failed"
            _pdf_jobs[job_id]["progress"] = 1.0
            _pdf_jobs[job_id]["error"] = str(e)

    asyncio.create_task(_work())
    return {"job_id": job_id, "status": "running"}

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
    _export_block_if_blockers(warnings)

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
    

@app.get("/export/events_csv/preview", tags=["export"])
def export_events_csv_preview(
    run_id: str = "latest",
):
    """
    HTML preview for realized events CSV (all rows), using virtual scrolling + paged JSON fetches.
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

        total = conn.execute(
            text("SELECT COUNT(1) AS n FROM realized_events WHERE run_id = :rid"),
            {"rid": run_id_val},
        ).mappings().first()
        total_n = int(total["n"]) if total and total.get("n") is not None else 0

    dl_url = f"/export/events_csv?run_id={run_id_val}"
    data_url = f"/export/events_csv/preview_data?run_id={run_id_val}"
    title = f"Events CSV preview — run {run_id_val}"

    def esc(x: object) -> str:
        return _html.escape("" if x is None else str(x))

    html_doc = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{esc(title)}</title>
  <style>
    :root {{
      --bg0: #070b14;
      --bg1: #0b1224;
      --card: rgba(18, 24, 40, 0.72);
      --border: rgba(255,255,255,0.10);
      --text: rgba(255,255,255,0.92);
      --muted: rgba(255,255,255,0.70);
      --muted2: rgba(255,255,255,0.55);
      --accent: #33d6ff;
      --accent2: #7c5cff;
      --shadow: 0 18px 60px rgba(0,0,0,0.45);
    }}

    html, body {{ height: 100%; overflow: hidden; }}

    body {{
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at 20% 10%, rgba(51, 214, 255, 0.12), transparent 60%),
        radial-gradient(900px 500px at 80% 0%, rgba(124, 92, 255, 0.12), transparent 55%),
        radial-gradient(900px 700px at 60% 110%, rgba(51, 214, 255, 0.06), transparent 55%),
        linear-gradient(180deg, var(--bg0), var(--bg1));
    }}

    .page {{
      height: 100dvh;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }}

    .topbar {{
        position: sticky;
        top: 0;
        left: 0;
        right: 0;
        z-index: 10;
        padding: 18px 18px 14px;
        background: linear-gradient(180deg, rgba(7,11,20,0.94), rgba(7,11,20,0.62));
        backdrop-filter: blur(12px);
        border-bottom: 0; /* remove harsh line */
        box-shadow: 0 18px 48px rgba(0,0,0,0.22);
    }}

    .topbar:after {{
        content: "";
        position: absolute;
        left: 0;
        right: 0;
        bottom: -1px;
        height: 34px;
        background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.00));
        opacity: 0.14;
        pointer-events: none;
    }}

    .topbar-inner {{
      display: flex;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
      max-width: 1400px;
      margin: 0 auto;

      padding: 14px 14px;
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.04);
      box-shadow: 0 16px 54px rgba(0,0,0,0.30);
      backdrop-filter: blur(14px);
    }}

    .title {{
      font-weight: 900;
      letter-spacing: -0.02em;
      font-size: 20px;
      margin: 0;
      line-height: 1.05;
    }}

    .meta {{
      margin-top: 6px;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.35;
    }}

    .pillrow {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 10px;
      font-size: 12px;
      color: var(--muted2);
    }}

    .pill {{
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.04);
      backdrop-filter: blur(8px);
      white-space: nowrap;
    }}

    .actions {{
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      padding-top: 2px;
      flex-shrink: 0;
    }}

    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.12);
      color: var(--text);
      text-decoration: none;
      background: rgba(255,255,255,0.04);
      cursor: pointer;
      user-select: none;
      transition: transform .08s ease, background .12s ease, border-color .12s ease;
    }}
    .btn:hover {{
      background: rgba(255,255,255,0.07);
      border-color: rgba(255,255,255,0.16);
    }}
    .btn:active {{ transform: translateY(1px); }}

    .btn-primary {{
      background: linear-gradient(135deg, rgba(51,214,255,0.26), rgba(124,92,255,0.22));
      border-color: rgba(51,214,255,0.32);
      box-shadow: 0 14px 38px rgba(51,214,255,0.10), 0 18px 60px rgba(0,0,0,0.25);
    }}

    .content {{
      flex: 1;
      padding: 0 18px 18px;
      min-height: 0;

      /* Critical: makes .card {{ flex: 1 }} take effect so the table scrolls, not the page */
      display: flex;
      flex-direction: column;
    }}

    .card {{
      max-width: 1400px;
      margin: 0 auto;
      width: 100%;
      flex: 1;
      min-height: 0;
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,0.08);
      background: var(--card);
      box-shadow: var(--shadow);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }}

    .card-head {{
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      padding: 12px 14px;
      background: rgba(255,255,255,0.03);
      border-bottom: 1px solid rgba(255,255,255,0.06);
    }}

    .card-head-right {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      flex-shrink: 0;
    }}

    .btn-compact {{
      padding: 8px 10px;
      border-radius: 12px;
      font-size: 12px;
    }}

    .status {{
      font-size: 12px;
      color: var(--muted);
    }}

    .mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }}

    .tableWrap {{
      flex: 1;
      min-height: 0;
      overflow: auto;

      /* Helps visibility on dark UI; harmless on Chromium/Opera */
      scrollbar-width: auto;
      scrollbar-color: rgba(255,255,255,0.30) rgba(255,255,255,0.06);
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}

    th, td {{
      padding: 10px 10px;
      border-bottom: 1px solid rgba(255,255,255,0.07);
      text-align: left;
      white-space: nowrap;
    }}

    th {{
      position: sticky;
      top: 0;
      z-index: 2;
      font-size: 12px;
      color: rgba(255,255,255,0.82);
      background: linear-gradient(180deg, rgba(10,14,26,0.78), rgba(10,14,26,0.58));
      backdrop-filter: blur(12px);
      border-bottom: 1px solid rgba(255,255,255,0.06);
      box-shadow: 0 10px 24px rgba(0,0,0,0.18);
    }}

    tbody tr:nth-child(2n) td {{ background: rgba(255,255,255,0.015); }}
    tbody tr:hover td {{ background: rgba(51,214,255,0.06); }}

    td.num, th.num {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}

    .tableWrap::-webkit-scrollbar {{ width: 16px; height: 16px; }}

    .tableWrap::-webkit-scrollbar-track {{
      background: rgba(255,255,255,0.06);
      border-left: 1px solid rgba(255,255,255,0.08);
    }}

    .tableWrap::-webkit-scrollbar-thumb {{
      background: rgba(255,255,255,0.30);
      border-radius: 999px;
      border: 4px solid rgba(0,0,0,0);
      background-clip: padding-box;
    }}

    .tableWrap::-webkit-scrollbar-thumb:hover {{
      background: rgba(255,255,255,0.40);
      background-clip: padding-box;
    }}

    .tableWrap::-webkit-scrollbar-corner {{
      background: rgba(255,255,255,0.06);
    }}

    @media (max-width: 900px) {{
      .card {{ height: calc(100vh - 190px); }}
      th, td {{ padding: 9px 8px; }}
    }}
  </style>
</head>
<body>
<div class="page">
  <div class="topbar">
    <div class="topbar-inner">
      <div style="min-width:0;">
        <div class="title">{esc(title)}</div>
        <div class="meta">
          Jurisdiction: {esc(run_meta.get("jurisdiction"))} • Tax year: {esc(run_meta.get("tax_year"))} • FX: {esc(run_meta.get("fx_set_id"))}
        </div>
        <div class="pillrow">
          <div class="pill">Total rows: <span class="mono">{esc(total_n)}</span></div>
          <div class="pill"><span id="st">Loading…</span></div>
          <div class="pill mono" id="st2"></div>
        </div>
      </div>
      <div class="actions">
        <a class="btn btn-primary" href="{esc(dl_url)}" target="_blank" rel="noopener">Download CSV</a>
      </div>
    </div>
  </div>

  <div class="content">
    <div class="card">
      <div class="card-head">
        <div class="status">Tip: scroll to load the full dataset. The table stays smooth with virtual scrolling.</div>
        <div class="card-head-right">
          <div class="status mono">Run {esc(run_id_val)} • Events <span id="st3" class="mono">—</span></div>
          <button id="btnScrollEnd" class="btn btn-compact" type="button">Scroll to end</button>
        </div>
      </div>

      <div class="tableWrap" id="wrap">
        <table>
          <thead>
            <tr>
              <th>timestamp</th>
              <th>asset</th>
              <th class="num">qty_sold</th>
              <th class="num">proceeds_eur</th>
              <th class="num">cost_basis_eur</th>
              <th class="num">gain_eur</th>
              <th>quote_asset</th>
              <th class="num">fee_applied_eur</th>
            </tr>
          </thead>
          <tbody id="tb"></tbody>
        </table>
      </div>
    </div>
  </div>
</div>

<script>
(() => {{
  const DATA_URL = {json.dumps(data_url)};
  const TOTAL = {int(total_n)};

  const wrap = document.getElementById('wrap');
  const tb = document.getElementById('tb');
  const st = document.getElementById('st');
  const st2 = document.getElementById('st2');
  const st3 = document.getElementById('st3');
  const btnScrollEnd = document.getElementById('btnScrollEnd');

  st3.textContent = TOTAL.toLocaleString();
  if (btnScrollEnd) {{
    btnScrollEnd.addEventListener('click', async () => {{
      // Ensure all rows are loaded first, then scroll after layout settles.
      await ensureLoaded(TOTAL);

      requestAnimationFrame(() => {{
        wrap.scrollTop = wrap.scrollHeight;
        requestAnimationFrame(() => {{
          wrap.scrollTop = wrap.scrollHeight;
          renderWindow(true);
        }});
      }});
    }});
  }}

  const PAGE = 800;
  const rows = [];
  let loaded = 0;
  let loading = false;
  let done = false;

  let rowH = 34;
  const overscan = 14;
  let raf = 0;

  function esc(s) {{
    const div = document.createElement('div');
    div.textContent = (s == null ? '' : String(s));
    return div.innerHTML;
  }}

  async function fetchPage(offset) {{
    const u = new URL(DATA_URL, window.location.origin);
    u.searchParams.set('offset', String(offset));
    u.searchParams.set('limit', String(PAGE));
    const r = await fetch(u.toString(), {{ credentials: 'same-origin' }});
    if (!r.ok) throw new Error('fetch');
    return await r.json();
  }}

  async function ensureLoaded(need) {{
    if (done || loading) return;
    if (loaded >= need) return;

    loading = true;
    st.textContent = 'Loading…';
    try {{
      while (loaded < need && !done) {{
        const j = await fetchPage(loaded);
        const items = Array.isArray(j.items) ? j.items : [];
        for (let i = 0; i < items.length; i++) rows.push(items[i]);
        loaded = rows.length;
        if (!items.length || loaded >= (j.total ?? TOTAL)) done = true;
        st2.textContent = `${{loaded.toLocaleString()}} / ${{(j.total ?? TOTAL).toLocaleString()}}`;
        await new Promise(res => requestAnimationFrame(res));
      }}
    }} finally {{
      loading = false;
      st.textContent = done ? 'Loaded' : 'Loaded (partial)';
      st2.textContent = `${{loaded.toLocaleString()}} / ${{TOTAL.toLocaleString()}}`;
      renderWindow(true);
    }}
  }}

  function renderWindow(force) {{
    const n = rows.length;
    if (!n) {{
      tb.innerHTML = '';
      return;
    }}

    const scrollTop = wrap.scrollTop;
    const viewH = wrap.clientHeight;

    const rowsPerView = Math.max(1, Math.ceil(viewH / rowH));
    const start = Math.max(0, Math.floor(scrollTop / rowH) - overscan);

    // Clamp start so the final window always reaches the end.
    const maxStart = Math.max(0, n - (rowsPerView + (overscan * 2)));
    const start2 = Math.min(start, maxStart);
    const end = Math.min(n, start2 + rowsPerView + (overscan * 2));

    const key = `${{start2}}:${{end}}:${{n}}`;
    if (!force && tb.getAttribute('data-vkey') === key) return;
    tb.setAttribute('data-vkey', key);

    const topPad = start2 * rowH;
    const botPad = (n - end) * rowH;

    let html = '';
    if (topPad > 0) {{
      html += `<tr class="sp"><td colspan="8" style="height:${{topPad}}px;padding:0;border:0;"></td></tr>`;
    }}

    for (let i = start2; i < end; i++) {{
      const r = rows[i] || {{}};
      html += `<tr>
        <td>${{esc(r.timestamp)}}</td>
        <td>${{esc(r.asset)}}</td>
        <td class="num">${{esc(r.qty_sold)}}</td>
        <td class="num">${{esc(r.proceeds)}}</td>
        <td class="num">${{esc(r.cost_basis)}}</td>
        <td class="num">${{esc(r.gain)}}</td>
        <td>${{esc(r.quote_asset)}}</td>
        <td class="num">${{esc(r.fee_applied)}}</td>
      </tr>`;
    }}

    if (botPad > 0) {{
      html += `<tr class="sp"><td colspan="8" style="height:${{botPad}}px;padding:0;border:0;"></td></tr>`;
    }}

    tb.innerHTML = html;

    const tr = tb.querySelector('tr:not(.sp)');
    if (tr) {{
      const h = tr.getBoundingClientRect().height;
      if (h && h >= 22 && h <= 80) rowH = h;
    }}
  }}

  function onScroll() {{
    if (raf) return;
    raf = requestAnimationFrame(() => {{
      raf = 0;
      renderWindow(false);

      const scrollBottom = wrap.scrollTop + wrap.clientHeight;
      const needRows = Math.min(TOTAL, Math.ceil(scrollBottom / rowH) + overscan * 3);
      ensureLoaded(needRows);
    }});
  }}

  wrap.addEventListener('scroll', onScroll, {{ passive: true }});

  const initialNeed = Math.min(TOTAL, 1400);
  ensureLoaded(initialNeed);
}})();
</script>
</body>
</html>
"""
    return HTMLResponse(content=html_doc)


@app.get("/export/events_csv/preview_data", tags=["export"])
def export_events_csv_preview_data(
    run_id: int = Query(..., description="calc_runs.id"),
    offset: int = Query(0, ge=0),
    limit: int = Query(800, ge=50, le=5000),
):
    """
    JSON paging for Events CSV preview (ordered by realized_events.id).
    """
    with engine.begin() as conn:
        total = conn.execute(
            text("SELECT COUNT(1) AS n FROM realized_events WHERE run_id = :rid"),
            {"rid": int(run_id)},
        ).mappings().first()
        total_n = int(total["n"]) if total and total.get("n") is not None else 0

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
                    fee_applied
                FROM realized_events
                WHERE run_id = :rid
                ORDER BY id
                LIMIT :lim OFFSET :off
                """
            ),
            {"rid": int(run_id), "lim": int(limit), "off": int(offset)},
        ).mappings().all()

    return JSONResponse(
        {
            "run_id": int(run_id),
            "total": total_n,
            "offset": int(offset),
            "limit": int(limit),
            "items": [dict(r) for r in rows],
        }
    )
    

@app.get("/audit/run/{run_id}")
def audit_get_run(run_id: int):
    """
    Return the stored manifest + hashes for a run and recompute live hashes.

    Also validates stored run_digests.manifest_json when present. This catches
    tampering where someone edits the stored manifest JSON without updating the
    stored digest fields.
    """
    from .audit_digest import build_run_manifest, compute_digests

    manifest = build_run_manifest(run_id)
    live = compute_digests(manifest)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT input_hash, output_hash, manifest_hash, manifest_json, created_at
                FROM run_digests
                WHERE run_id = :rid
                """
            ),
            dict(rid=int(run_id)),
        ).mappings().first()

    stored = None
    stored_manifest_json_valid = True
    stored_manifest_json_digests = None
    stored_manifest_json_error = None

    if row:
        stored = {
            "input_hash": row.get("input_hash"),
            "output_hash": row.get("output_hash"),
            "manifest_hash": row.get("manifest_hash"),
            "created_at": row.get("created_at"),
        }

        manifest_json_raw = row.get("manifest_json")
        if manifest_json_raw:
            try:
                stored_manifest = json.loads(manifest_json_raw)
                stored_manifest_json_digests = compute_digests(stored_manifest)
                stored_manifest_json_valid = (
                    stored_manifest_json_digests.get("input_hash") == stored.get("input_hash")
                    and stored_manifest_json_digests.get("output_hash") == stored.get("output_hash")
                    and stored_manifest_json_digests.get("manifest_hash") == stored.get("manifest_hash")
                )
            except Exception as exc:
                stored_manifest_json_valid = False
                stored_manifest_json_error = str(exc)

    match = False
    if stored:
        match = (
            stored.get("input_hash") == live.get("input_hash")
            and stored.get("output_hash") == live.get("output_hash")
            and stored.get("manifest_hash") == live.get("manifest_hash")
            and stored_manifest_json_valid
        )

    return {
        "run_id": run_id,
        "stored": stored,
        "recomputed": live,
        "matches": match,
        "stored_manifest_json_valid": stored_manifest_json_valid,
        "stored_manifest_json_digests": stored_manifest_json_digests,
        "stored_manifest_json_error": stored_manifest_json_error,
        "manifest": manifest,
    }


@app.get("/audit/verify/{run_id}")
def audit_verify_run(run_id: int):
    """
    Recompute digest and compare with stored digests.

    Verification also checks stored run_digests.manifest_json when present so
    stored audit artifacts cannot be altered without detection.
    """
    from .audit_digest import build_run_manifest, compute_digests

    manifest = build_run_manifest(run_id)
    live = compute_digests(manifest)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT input_hash, output_hash, manifest_hash, manifest_json
                FROM run_digests
                WHERE run_id = :rid
                """
            ),
            dict(rid=int(run_id)),
        ).mappings().first()

    if not row:
        return {
            "run_id": run_id,
            "verified": False,
            "reason": "No stored digest for this run.",
            "recomputed": live,
        }

    stored = {
        "input_hash": row.get("input_hash"),
        "output_hash": row.get("output_hash"),
        "manifest_hash": row.get("manifest_hash"),
    }

    stored_manifest_json_valid = True
    stored_manifest_json_digests = None
    stored_manifest_json_error = None

    manifest_json_raw = row.get("manifest_json")
    if manifest_json_raw:
        try:
            stored_manifest = json.loads(manifest_json_raw)
            stored_manifest_json_digests = compute_digests(stored_manifest)
            stored_manifest_json_valid = (
                stored_manifest_json_digests.get("input_hash") == stored.get("input_hash")
                and stored_manifest_json_digests.get("output_hash") == stored.get("output_hash")
                and stored_manifest_json_digests.get("manifest_hash") == stored.get("manifest_hash")
            )
        except Exception as exc:
            stored_manifest_json_valid = False
            stored_manifest_json_error = str(exc)

    ok = stored == live and stored_manifest_json_valid

    return {
        "run_id": run_id,
        "verified": bool(ok),
        "stored": stored,
        "recomputed": live,
        "stored_manifest_json_valid": stored_manifest_json_valid,
        "stored_manifest_json_digests": stored_manifest_json_digests,
        "stored_manifest_json_error": stored_manifest_json_error,
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
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    year: int | None = Query(None),
    asset: str | None = Query(None),
    mode: str = Query("all", pattern="^(all|gains|losses)$"),
    q: str | None = Query(None, description="Search (asset or date prefix)"),
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
    
    # Gain mode filter (gain stored as TEXT; cast for numeric compare)
    m = (mode or "all").strip().lower()
    if m == "gains":
        where.append("CAST(gain AS REAL) > 0")
    elif m == "losses":
        where.append("CAST(gain AS REAL) < 0")

    # Search: match either asset or timestamp (simple LIKE; fast with run_id+timestamp index)
    if q:
        s = str(q).strip()
        if s:
            params["q_asset"] = f"%{s.upper()}%"
            params["q_ts"] = f"%{s}%"
            where.append("(UPPER(asset) LIKE :q_asset OR timestamp LIKE :q_ts)")

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

    warnings: list[str] = []
    fx_context: dict[str, Any] = {}
    fee_valuation: dict[str, Any] = {}

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

        w = summary_obj.get("warnings")
        if isinstance(w, list):
            warnings = [str(x) for x in w if x is not None]

        fx = summary_obj.get("fx_context")
        if isinstance(fx, dict):
            fx_context = fx

        fv = summary_obj.get("fee_valuation")
        if isinstance(fv, dict):
            fee_valuation = fv

    return {
        "run_id": str(run.run_id or rid_int),
        "run_db_id": int(run.id),
        "jurisdiction": run.jurisdiction,
        "rule_version": run.rule_version,
        "tax_year": getattr(run, "tax_year", None),
        "lot_method": run.lot_method,
        "fx_set_id": run.fx_set_id,
        "totals": totals,
        "warnings": warnings,
        "warnings_count": len(warnings),
        "fx_context": fx_context,
        "fee_valuation": fee_valuation,
        "source": source,
    }


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
app.include_router(data_admin_router)
app.include_router(ui_router)

if (not IS_PROD) and ENABLE_ADMIN_ENDPOINTS and ENABLE_ADMIN_SCRIPTS:
    app.include_router(admin_ops_router)
    app.include_router(csv_admin_router)
    app.include_router(ops_admin_router)
    app.include_router(export_ui_router)
    
    # EXE builder is an admin-scripts surface (Swagger only).
    app.include_router(demo_build_router)
    
from .history_routes import router as history_router
app.include_router(history_router)

# Mount demo routes only when DEMO_MODE is enabled,
# so production deployments don't expose /demo/* by accident.
allow_demo_in_prod = _truthy_env(os.getenv("ALLOW_DEMO_IN_PROD"))
if is_demo_mode_enabled() and (not IS_PROD or allow_demo_in_prod):
    app.include_router(demo_router)


# ---------------------------------------------------------------------------
# Static assets and demo manifest auto-loader
# ---------------------------------------------------------------------------

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
    local_area: str | None = Query(None, description="Optional local area code (e.g. ZAGREB) for HR prirez ≤ 2023"),
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
    tax_ctx = _tax_context_for(jurisdiction=j, tax_year=tax_year_used, local_area=local_area)

    national_rate = tax_ctx.get("national_rate", Decimal("0"))
    local_rate = tax_ctx.get("local_rate", Decimal("0"))
    effective_rate = tax_ctx.get("effective_rate", national_rate + local_rate)
    rate_model = str(tax_ctx.get("rate_model", "flat"))
    local_surtax_pct = tax_ctx.get("local_surtax_pct", Decimal("0"))
    local_area_code = str(tax_ctx.get("local_area", "") or "")

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
            "local_area": local_area_code,
            "local_surtax_pct": str(local_surtax_pct),
            "rate_model": rate_model,
            "effective_rate": str(effective_rate),
            "tax_due_eur": str(tax_due_eur),
            "tax_year_used": str(tax_ctx.get("tax_year_used", tax_year_used)),
            "rate_model": str(tax_ctx.get("rate_model") or "flat"),
            "local_surtax_pct": str(tax_ctx.get("local_surtax_pct") or "0"),
            "local_area": str(tax_ctx.get("local_area") or (local_area or "")),
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

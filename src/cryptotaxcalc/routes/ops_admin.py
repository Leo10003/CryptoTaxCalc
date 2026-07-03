from __future__ import annotations

import inspect
import os
import shutil
import tempfile
from datetime import datetime, datetime as dt
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from starlette.background import BackgroundTask

from ..db import SessionLocal, engine
from ..exporter import ExportOptions, build_export_zip
from ..logging_setup import get_logger
from ..models import AuditLog, CalcRun, RealizedEvent, RawEvent, RunDigest, RunInput, TransactionRow, FXRate as FxRate
from ..runtime_paths import PROJECT_ROOT
from ..security import IS_PROD, _admin_not_found, require_admin_scripts


router = APIRouter(tags=["admin"])


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
        from sqlalchemy import create_engine

        tmp_engine = create_engine(f"sqlite:///{db_path}")
        with tmp_engine.connect() as conn:
            res = conn.execute(text("PRAGMA integrity_check;")).scalar()
        tmp_engine.dispose()
        return res == "ok"
    except Exception:
        return False


@router.post("/maintenance/prune_fx", include_in_schema=False)
def prune_fx(
    keep_years: int = Query(5, ge=1, le=20),
    _admin: None = Depends(require_admin_scripts),
) -> Dict[str, Any]:
    """Keep only the last N years of FX rates (rolling window)."""
    if IS_PROD:
        _admin_not_found()

    today = dt.today()
    cutoff_year = today.year - keep_years
    cutoff = dt(cutoff_year, 1, 1)

    with SessionLocal() as session:
        total_before = session.query(FxRate).count()
        session.query(FxRate).filter(FxRate.date < cutoff).delete(synchronize_session=False)
        session.commit()
        total_after = session.query(FxRate).count()

    return {"status": "ok", "kept_from": str(cutoff), "deleted_rows": (total_before - total_after)}


@router.post("/maintenance/vacuum", include_in_schema=False)
def vacuum_sqlite(_admin: None = Depends(require_admin_scripts)) -> Dict[str, Any]:
    """Run VACUUM to reclaim file space in SQLite after large deletes."""
    if IS_PROD:
        _admin_not_found()

    with engine.connect() as conn:
        conn.execute(text("VACUUM"))
    return {"status": "ok"}


@router.get("/export/db", summary="Download current database backup", include_in_schema=False)
def export_database(_admin: None = Depends(require_admin_scripts)):
    if IS_PROD:
        _admin_not_found()

    db_name = getattr(engine.url, "database", None) or "cryptotaxcalc.db"
    db_path = db_name if os.path.isabs(db_name) else str((PROJECT_ROOT / db_name).resolve())

    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"Database not found at {db_path}")

    import datetime as _datetime
    backups_dir = PROJECT_ROOT / "backups"
    backups_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_name = f"db_backup_{timestamp}.sqlite"
    backup_path = backups_dir / backup_name
    shutil.copy2(db_path, backup_path)

    return FileResponse(
        path=str(backup_path),
        filename=backup_name,
        media_type="application/x-sqlite3",
    )


@router.post("/import/db", summary="Restore the SQLite database from an uploaded .db file (with safety checks)", include_in_schema=False)
async def import_database(
    file: UploadFile = File(...),
    confirm: str = Query("", description="Must be 'I_UNDERSTAND' to proceed"),
    _admin: None = Depends(require_admin_scripts),
):
    if IS_PROD:
        _admin_not_found()

    if confirm != "I_UNDERSTAND":
        raise HTTPException(
            status_code=400,
            detail="Confirmation missing. Add ?confirm=I_UNDERSTAND to proceed (this will overwrite the current DB).",
        )

    os.makedirs("backups", exist_ok=True)

    suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp_dir = tempfile.mkdtemp(prefix="restore_")
    tmp_upload = os.path.join(tmp_dir, f"uploaded_{suffix}.db")

    try:
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        with open(tmp_upload, "wb") as f:
            f.write(raw)

        if not _is_sqlite_file(tmp_upload):
            raise HTTPException(status_code=400, detail="Uploaded file is not a valid SQLite database.")

        if not _integrity_ok(tmp_upload):
            raise HTTPException(status_code=400, detail="Uploaded database failed integrity_check.")

        db_name = getattr(engine.url, "database", None) or "cryptotaxcalc.db"
        src_db = db_name if os.path.isabs(db_name) else str((PROJECT_ROOT / db_name).resolve())

        if not os.path.exists(src_db):
            backup_path = None
        else:
            backup_name = f"data_before_restore_{suffix}.db"
            backup_path = os.path.join("backups", backup_name)
            shutil.copy2(src_db, backup_path)

        try:
            engine.dispose()
        except Exception:
            pass

        shutil.copy2(tmp_upload, src_db)

        if not _integrity_ok(src_db):
            if backup_path and os.path.exists(backup_path):
                shutil.copy2(backup_path, src_db)
            raise HTTPException(status_code=500, detail="Restored DB failed integrity_check; original DB has been restored.")

        return JSONResponse(
            {
                "status": "ok",
                "message": "Database restored successfully.",
                "backup": (backup_path or "none"),
                "restored_from": file.filename,
            }
        )

    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass


class ExportBody(BaseModel):
    include_history: bool = True
    include_db: bool = True
    include_logs: bool = True
    include_env_redacted: bool = True
    include_requirements: bool = True
    include_pyproject: bool = True
    include_git_meta: bool = True


@router.post("/export/bundle", tags=["export"], include_in_schema=False)
def export_bundle(body: ExportBody, request: Request, _admin: None = Depends(require_admin_scripts)):
    if IS_PROD:
        _admin_not_found()

    debug = request.query_params.get("debug") == "1"
    body_dict = body.model_dump()

    sig = inspect.signature(ExportOptions)
    params = set(sig.parameters.keys())

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
        out: Dict[str, Any] = {
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

    if zip_bytes is not None:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        tmp.write(zip_bytes)
        tmp.flush()
        tmp.close()
        return FileResponse(tmp.name, media_type="application/zip", filename="CryptoTaxCalc_Export.zip")

    if zip_path is None or not zip_path.exists() or not zip_path.is_file():
        raise HTTPException(status_code=500, detail="Export bundle failed: zip not created.")

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


@router.post(
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
    _admin: None = Depends(require_admin_scripts),
):
    if IS_PROD:
        _admin_not_found()

    if confirm != "I_UNDERSTAND":
        raise HTTPException(
            status_code=400,
            detail="Confirmation missing. Add ?confirm=I_UNDERSTAND to proceed (this will delete ALL data).",
        )

    with SessionLocal() as session:
        session.query(RunInput).delete()
        session.query(RealizedEvent).delete()
        session.query(RunDigest).delete()
        session.query(AuditLog).delete()

        session.query(CalcRun).delete()
        session.query(TransactionRow).delete()
        session.query(RawEvent).delete()
        session.commit()

    return {"status": "OK", "message": "Database fully reset for Option A migration."}

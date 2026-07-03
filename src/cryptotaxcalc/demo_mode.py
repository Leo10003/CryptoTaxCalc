from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from sqlalchemy import text
import json
import os
import sys
import hmac

from cryptotaxcalc.logging_setup import get_logger, _now_iso_z
from cryptotaxcalc.db import engine
from cryptotaxcalc.demo_assets import ensure_demo_env, reset_demo_db, is_demo_mode_enabled


logger = get_logger("demo")
router = APIRouter(prefix="/demo", tags=["demo"])


def _truthy_env(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def _is_prod() -> bool:
    ctc_env = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
    return ctc_env in {"prod", "production"}


def _require_demo_admin(
    request: Request,
    *,
    authorization: str | None,
    x_admin_token: str | None,
) -> None:
    """Best-effort admin check for demo-only sensitive actions.

    Psychology:
      - Protects “export logs / reset dataset” actions, reducing the feeling that
        the product can silently leak or destroy data.
    """
    expected = (os.getenv("ADMIN_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not configured")

    # Localhost-only unless explicitly allowed.
    if not _truthy_env(os.getenv("ADMIN_ALLOW_REMOTE")):
        host = request.client.host if request.client else ""
        if host not in {"127.0.0.1", "::1"}:
            raise HTTPException(status_code=404, detail="Not found")

    supplied = _extract_bearer_token(authorization) or (x_admin_token or "").strip()
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _resolve_resource_root() -> Path:
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass).resolve()

    here = Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "templates").exists():
            return p.resolve()

    return here.parent.resolve()


RESOURCE_ROOT = _resolve_resource_root()
TEMPLATES_DIR = (RESOURCE_ROOT / "templates").resolve()
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


DEMO_STATE_PATH = Path("artifacts/demo")
DEMO_STATE_PATH.mkdir(parents=True, exist_ok=True)
BUILD_MANIFEST = DEMO_STATE_PATH / "demo_build_manifest.json"


def _read_build_manifest() -> dict:
    """Return build info even if manifest is missing."""
    if BUILD_MANIFEST.is_file():
        try:
            data = json.loads(BUILD_MANIFEST.read_text(encoding="utf-8"))
            return {
                "version": data.get("version") or data.get("app_version") or "n/a",
                "commit": data.get("commit", "n/a"),
                "built_at": data.get("built_at") or data.get("timestamp") or "n/a",
                "status": "ready" if data.get("verified") else data.get("status", "none"),
            }
        except Exception as e:
            logger.warning(f"demo manifest read failed: {e}")
    return {"version": "n/a", "commit": "n/a", "built_at": "n/a", "status": "none"}


@router.get("/build_info")
def demo_build_info():
    ensure_demo_env()
    info = _read_build_manifest()
    return JSONResponse(
        {
            "timestamp": _now_iso_z(),
            "demo_mode": is_demo_mode_enabled(),
            "version": info.get("version", "n/a"),
            "commit": info.get("commit", "n/a"),
            "built_at": info.get("built_at", "n/a"),
            "status": info.get("status", "none"),
        }
    )


@router.get("/self_check")
def demo_self_check():
    """Light runtime checks for dashboard panel."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception as e:
        logger.warning(f"DB check failed: {e}")
        db_ok = False

    return {
        "timestamp": _now_iso_z(),
        "demo_mode": is_demo_mode_enabled(),
        "database": "connected" if db_ok else "failed",
        "backup": "active",
        "git_gatekeeper": "ready",
    }


@router.post("/reset")
def demo_reset(
    request: Request,
    authorization: str | None = Header(default=None, alias="Authorization"),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    if not is_demo_mode_enabled():
        raise HTTPException(status_code=403, detail="Demo mode disabled")

    # In production, never allow unauthenticated destructive actions.
    if _is_prod() and not _truthy_env(os.getenv("DEMO_ALLOW_PUBLIC_RESET")):
        _require_demo_admin(request, authorization=authorization, x_admin_token=x_admin_token)

    with engine.begin() as conn:
        conn.execute(text("DROP INDEX IF EXISTS ix_calc_runs_run_id"))
        conn.execute(text("DROP TABLE IF EXISTS run_digests"))
        conn.execute(text("DROP TABLE IF EXISTS calc_runs"))

    reset_demo_db(engine)

    try:
        from cryptotaxcalc.app import _bootstrap_fx_from_csv_if_empty
        _bootstrap_fx_from_csv_if_empty(engine)
    except Exception as e:
        logger.warning(f"Demo reset: FX bootstrap skipped: {e}")

    return {"ok": True, "message": "Demo DB restored to default dataset."}


@router.get("/logo")
def demo_logo(variant: str = Query("light", pattern="^(light|dark)$")):
    candidates = []
    if variant == "light":
        candidates += [
            Path("logo/icon_white.png"),
            Path("logo/White_transparent.png"),
            Path("icon_white.png"),
            Path("White_transparent.png"),
        ]
    else:
        candidates += [
            Path("logo/icon_black.png"),
            Path("logo/Black_transparent.png"),
            Path("icon_black.png"),
            Path("Black_transparent.png"),
        ]
    candidates += [Path("logo/logo.png"), Path("logo.png")]
    for p in candidates:
        if p.exists():
            return FileResponse(p)
    raise HTTPException(status_code=404, detail="Logo not found")


@router.get("/diagnostics/export")
def demo_diagnostics_export(
    request: Request,
    include_db: bool = Query(False, description="Include a SQLite DB snapshot (may contain sensitive data)"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Create a lightweight diagnostics zip.

    Default posture:
      - Admin-only unless DEMO_PUBLIC_DIAGNOSTICS=1
      - DB snapshot is opt-in via include_db=1

    Psychology:
      - Makes “data leaving the system” feel deliberate and controlled.
    """
    if not _truthy_env(os.getenv("DEMO_PUBLIC_DIAGNOSTICS")):
        _require_demo_admin(request, authorization=authorization, x_admin_token=x_admin_token)

    from pathlib import Path
    import io
    import zipfile

    out_dir = Path("logs/export")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "demo_diagnostics.zip"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for log_root in [Path("logs/app"), Path("logs")]:
            if log_root.exists():
                for p in log_root.rglob("*"):
                    if p.is_file():
                        zf.write(p, p.as_posix())

        if BUILD_MANIFEST.exists():
            zf.write(BUILD_MANIFEST, f"demo/{BUILD_MANIFEST.name}")

        if include_db:
            try:
                db_name = getattr(engine.url, "database", None) or "cryptotaxcalc.db"
                db_path = os.path.abspath(db_name)
                if os.path.exists(db_path):
                    zf.write(db_path, "database_snapshot.sqlite")
            except Exception as e:
                zf.writestr("notes/db_copy.txt", f"DB snapshot skipped: {e}")

        zf.writestr(
            "README.txt",
            "CryptoTaxCalc Demo Diagnostics\n"
            "This bundle contains recent logs, the demo build manifest, and (optionally) a DB snapshot.\n"
            "DB snapshots can contain sensitive financial data. Share with care.\n",
        )

    out_path.write_bytes(buf.getvalue())
    return FileResponse(
        path=str(out_path),
        media_type="application/zip",
        filename="CryptoTaxCalc_Demo_Diagnostics.zip",
    )


@router.get("/country_notes")
def demo_country_notes(jurisdiction: str = Query("HR", pattern="^[A-Za-z]{2}$")):
    base = {
        "icon_url": "/static/img/icons/country_notes.png",
        "disclaimer": (
            "These notes are illustrative and for information only — not tax advice. "
            "Your actual obligations depend on your country, thresholds and "
            "personal circumstances."
        ),
    }

    j = (jurisdiction or "HR").strip().upper()

    if j == "IT":
        payload = {
            **base,
            "jurisdiction": "IT",
            "title": "Country Notes – Italy (demo)",
            "subtitle": "High-level, non-binding context only.",
            "bullets": [
                "This demo does not implement the full Italian tax code.",
                "Real filings may consider yearly thresholds, specific reporting forms and additional income categories.",
                "Numbers shown here are for product testing and orientation only and must not be used directly for a tax return.",
            ],
        }
    elif j == "HR":
        payload = {
            **base,
            "jurisdiction": "HR",
            "title": "Country Notes – Croatia (demo)",
            "subtitle": "High-level, non-binding context only.",
            "bullets": [
                "This demo does not implement the full Croatian tax code.",
                "Real filings depend on holding periods, local guidance and your supporting documentation.",
                "Numbers shown here are for product testing and orientation only and must not be used directly for a tax return.",
            ],
        }
    else:
        payload = {
            **base,
            "jurisdiction": j,
            "title": f"Country Notes – {j} (demo)",
            "subtitle": "High-level, non-binding context only.",
            "bullets": [
                f"This demo does not yet provide detailed guidance for {j}.",
                "This is a technical FIFO + FX summary only; it does not replace local reporting or forms.",
                "Use a tax professional to validate obligations, thresholds, and forms for your jurisdiction.",
            ],
        }

    return JSONResponse(payload)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
def demo_dashboard(request: Request):
    return templates.TemplateResponse("demo_dashboard.html", {"request": request})

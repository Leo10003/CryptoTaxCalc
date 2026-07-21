from __future__ import annotations

import subprocess
import sys
import shutil
import json
import os
import time
import hashlib
import socket
import hmac
import importlib
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse

from cryptotaxcalc.logging_setup import get_logger, _now_iso_z, _atomic_write_json


router = APIRouter(prefix="/admin/demo", tags=["demo-build"])
logger = get_logger("demo.builder")

REPO_ROOT = Path(__file__).resolve().parents[2]

ARTIFACTS = (REPO_ROOT / "artifacts" / "demo")
ARTIFACTS.mkdir(parents=True, exist_ok=True)

# USB / investor bundle output (root-level)
BUNDLES = (REPO_ROOT / "demo_usb")
BUNDLES.mkdir(parents=True, exist_ok=True)

EXE_READY = ARTIFACTS / "CryptoTaxCalc_Demo.exe"
EXE_TMP = ARTIFACTS / "CryptoTaxCalc_Demo.tmp.exe"
MANIFEST = ARTIFACTS / "demo_build_manifest.json"
BUILDLOG = ARTIFACTS / "build.log"

# For --onedir builds we must keep the whole dist folder (exe + _internal + deps)
DIST_TMP = ARTIFACTS / "CryptoTaxCalc_Demo.tmp"
DIST_READY = ARTIFACTS / "CryptoTaxCalc_Demo_dist"

ZIP_LATEST = BUNDLES / "CryptoTaxCalc_Demo_LATEST.zip"

_build_lock = False


def _truthy_env(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or not val.strip():
        return default
    return _truthy_env(val)


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def _admin_not_found() -> None:
    # 404 reduces endpoint discovery; treat builder as an internal surface.
    raise HTTPException(status_code=404, detail="Not found")


def require_demo_builder_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    """Protect demo builder endpoints.

    Product / security rationale:
      - This endpoint can build and execute binaries; it must never be reachable accidentally.
      - Default posture is: disabled, localhost-only, header-only token auth.

    Psychology:
      - Eliminates “hidden control surfaces,” increasing perceived professionalism and safety.
    """
    ctc_env = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
    is_prod = ctc_env in {"prod", "production"}

    # Never expose builder endpoints in production.
    if is_prod:
        _admin_not_found()

    # Builder endpoints are treated as "admin scripts" and are opt-in.
    if not _env_bool("ENABLE_ADMIN_ENDPOINTS", default=False):
        _admin_not_found()
    if not _env_bool("ENABLE_ADMIN_SCRIPTS", default=False):
        _admin_not_found()

    # Localhost-only unless explicitly allowed.
    if not _env_bool("ADMIN_ALLOW_REMOTE", default=False):
        host = request.client.host if request.client else ""
        if host not in {"127.0.0.1", "::1"}:
            _admin_not_found()

    supplied = _extract_bearer_token(authorization) or (x_admin_token or "").strip()
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    expected = (os.getenv("ADMIN_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not configured")

    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_manifest(status: str, extra: dict):
    m = {"status": status, "timestamp": _now_iso_z(), **extra}
    _atomic_write_json(MANIFEST, m)
    return m


def _find_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _ensure_pyinstaller() -> None:
    """Verify PyInstaller is available in the current interpreter.

    Optional auto-install is permitted only in non-production environments and
    only when explicitly enabled (ALLOW_DEMO_AUTO_INSTALL=true).
    """
    try:
        importlib.import_module("PyInstaller")
        return
    except ImportError:
        pass

    ctc_env = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
    is_prod = ctc_env in {"prod", "production"}

    if os.getenv("ALLOW_DEMO_AUTO_INSTALL", "false").lower() == "true":
        if is_prod:
            raise HTTPException(status_code=403, detail="Auto-install is disabled in production")

        logger.info("PyInstaller not found; attempting auto-install...")
        cmd = [sys.executable, "-m", "pip", "install", "pyinstaller==6.10.*"]
        ret = subprocess.call(cmd)
        if ret != 0:
            raise HTTPException(status_code=500, detail="Auto-install of PyInstaller failed")
        try:
            importlib.import_module("PyInstaller")
            return
        except ImportError:
            raise HTTPException(status_code=500, detail="PyInstaller still not importable after install")

    raise HTTPException(
        status_code=400,
        detail=(
            "PyInstaller is not installed in this environment. "
            "Activate your venv and run: "
            "`python -m pip install pyinstaller==6.10.*`"
        ),
    )


@router.post("/build_exe", include_in_schema=True)
def build_exe(_admin: None = Depends(require_demo_builder_admin)):
    global _build_lock
    if _build_lock:
        raise HTTPException(status_code=429, detail="A build is already running")
    _build_lock = True

    _ensure_pyinstaller()

    try:
        BUILDLOG.write_text("", encoding="utf-8")
        _write_manifest("queued", {"message": "Build queued"})
        _write_manifest("building", {"message": "PyInstaller running"})

        repo_root = REPO_ROOT
        src_dir = repo_root / "src"
        demo_dir = repo_root / "demo"
        logo_dir = repo_root / "logo"
        templates_dir = repo_root / "templates"
        static_dir = repo_root / "static"

        cmd = [
            sys.executable,
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onedir",
            "--name",
            "CryptoTaxCalc_Demo",
            "--console",
            "--paths",
            str(src_dir),
            "--add-data",
            f"{demo_dir};demo",
            "--add-data",
            f"{logo_dir};logo",
            "--add-data",
            f"{templates_dir};templates",
            "--add-data",
            f"{static_dir};static",
        ]

        fx_csv = (repo_root / "automation" / "fx_ecb.csv")
        if fx_csv.exists():
            cmd += ["--add-data", f"{fx_csv};automation"]

        cmd.append("src/cryptotaxcalc/demo_launcher.py")

        with BUILDLOG.open("a", encoding="utf-8") as log_fp:
            with subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            ) as p:
                assert p.stdout is not None
                for line in p.stdout:
                    log_fp.write(line)
                    log_fp.flush()
                ret = p.wait()
                if ret != 0:
                    _write_manifest("failed", {"reason": "PyInstaller failed", "returncode": ret})
                    raise HTTPException(status_code=500, detail="Build failed")

        dist_dir = Path("dist/CryptoTaxCalc_Demo")
        produced = dist_dir / "CryptoTaxCalc_Demo.exe"
        # Copy the full onedir folder to a temp artifact folder for smoke testing.
        # Running the exe without its _internal folder will fail to load python312.dll.
        if DIST_TMP.exists():
            shutil.rmtree(DIST_TMP, ignore_errors=True)
        if not dist_dir.exists():
        actual_dist_entries = []
        dist_root = Path("dist")
        if dist_root.exists():
            actual_dist_entries = sorted(p.name for p in dist_root.iterdir())

        raise RuntimeError(
            "PyInstaller did not create the expected demo EXE folder. "
            f"Expected: {dist_dir}. "
            f"Actual dist entries: {actual_dist_entries}. "
            "Check the PyInstaller command, --name value, spec file, and warn-*.txt output."
        )

    shutil.copytree(dist_dir, DIST_TMP, dirs_exist_ok=True)

        tmp_exe = DIST_TMP / "CryptoTaxCalc_Demo.exe"
        
        if not produced.exists():
            _write_manifest("failed", {"reason": "EXE not found after build"})
            raise HTTPException(status_code=500, detail="No exe produced")

        _write_manifest("verifying", {"message": "Launching smoke test"})
        shutil.copy2(produced, EXE_TMP)
        port = _find_free_port()

        with subprocess.Popen(
            [str(tmp_exe), "--smoke", f"--port={port}"],
            cwd=str(DIST_TMP),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        ) as p:
            # Frozen apps can take longer to warm up (import time, DLL load, etc.)
            time.sleep(2.0)

            try:
                out, _ = p.communicate(timeout=120)
                ret = p.returncode
                reason = "Smoke test failed"
            except subprocess.TimeoutExpired:
                try:
                    p.kill()
                except Exception:
                    pass
                out, _ = p.communicate(timeout=5)
                ret = -1
                reason = "Smoke test timed out"

            if ret != 0:
                tail = (out or "")[-4000:]
                _write_manifest(
                    "failed",
                    {"reason": reason, "ret": ret, "output_tail": tail},
                )
                raise HTTPException(
                    status_code=500,
                    detail={"error": reason, "ret": ret, "output_tail": tail},
                )

        # Keep legacy single EXE artifact for quick download
        # Keep legacy single-exe artifact (optional convenience)
        try:
            shutil.copy2(tmp_exe, EXE_READY)
        except Exception:
            pass

        # Persist the full onedir folder as the "ready" dist
        if DIST_READY.exists():
            shutil.rmtree(DIST_READY, ignore_errors=True)
        shutil.copytree(DIST_TMP, DIST_READY, dirs_exist_ok=True)
        exe_sha = _sha256(EXE_READY)

        # Create USB bundle folder in repo root and zip it
        ts = time.strftime("%Y-%m-%d_%H%M%S")
        bundle_dir = BUNDLES / f"CryptoTaxCalc_Demo_{ts}"
        if bundle_dir.exists():
            shutil.rmtree(bundle_dir, ignore_errors=True)
        bundle_dir.mkdir(parents=True, exist_ok=True)

        # Copy full onedir dist (exe + all dependencies + add-data folders)
        shutil.copytree(DIST_READY, bundle_dir, dirs_exist_ok=True)

        # Copy build artifacts into bundle for traceability
        try:
            shutil.copy2(BUILDLOG, bundle_dir / "build.log")
        except Exception:
            pass
        try:
            shutil.copy2(MANIFEST, bundle_dir / "demo_build_manifest.json")
        except Exception:
            pass

        # Zip the bundle directory
        zip_path = BUNDLES / f"{bundle_dir.name}.zip"
        if zip_path.exists():
            zip_path.unlink(missing_ok=True)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in bundle_dir.rglob("*"):
                if p.is_file():
                    zf.write(p, arcname=str(p.relative_to(BUNDLES)))

        zip_sha = _sha256(zip_path)

        # Update latest pointer zip
        try:
            shutil.copy2(zip_path, ZIP_LATEST)
        except Exception:
            pass

        info = _write_manifest(
            "ready",
            {
                "verified": True,
                "built_at": _now_iso_z(),
                "version": os.getenv("APP_VERSION", "dev"),
                "commit": os.getenv("GIT_COMMIT", "local"),
                "exe_size": EXE_READY.stat().st_size,
                "exe_sha256": exe_sha,
                "bundle_dir": str(bundle_dir),
                "zip_path": str(zip_path),
                "zip_size": zip_path.stat().st_size,
                "zip_sha256": zip_sha,
                "zip_latest": str(ZIP_LATEST),
            },
        )
        return {"ok": True, "manifest": info}

    finally:
        _build_lock = False


@router.get("/build_status", include_in_schema=True)
def build_status(_admin: None = Depends(require_demo_builder_admin)):
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {"status": "none"}


@router.get("/download_exe", include_in_schema=True)
def download_exe(_admin: None = Depends(require_demo_builder_admin)):
    """
    Download the latest verified EXE (portable single-file).
    Intended workflow: build_exe -> build_status -> download_exe -> copy to USB.
    """
    if not EXE_READY.exists():
        raise HTTPException(status_code=404, detail="EXE not built yet")
    return FileResponse(
        path=str(EXE_READY),
        media_type="application/octet-stream",
        filename=EXE_READY.name,
    )


@router.get("/download_build_log", include_in_schema=True)
def download_build_log(_admin: None = Depends(require_demo_builder_admin)):
    """Download build.log for diagnostics."""
    if not BUILDLOG.exists():
        raise HTTPException(status_code=404, detail="No build log found")
    return FileResponse(
        path=str(BUILDLOG),
        media_type="text/plain",
        filename=BUILDLOG.name,
    )


@router.get("/download_manifest", include_in_schema=True)
def download_manifest(_admin: None = Depends(require_demo_builder_admin)):
    """Download the build manifest JSON."""
    if not MANIFEST.exists():
        raise HTTPException(status_code=404, detail="No manifest found")
    return FileResponse(
        path=str(MANIFEST),
        media_type="application/json",
        filename=MANIFEST.name,
    )


@router.get("/download_zip", include_in_schema=True)
def download_zip(_admin: None = Depends(require_demo_builder_admin)):
    """
    Download the latest USB bundle ZIP (EXE + all runtime data).
    """
    if ZIP_LATEST.exists():
        return FileResponse(
            path=str(ZIP_LATEST),
            media_type="application/zip",
            filename=ZIP_LATEST.name,
        )

    # Fallback: try newest zip in folder
    zips = sorted(BUNDLES.glob("CryptoTaxCalc_Demo_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not zips:
        raise HTTPException(status_code=404, detail="No bundle zip found")
    z = zips[0]
    return FileResponse(
        path=str(z),
        media_type="application/zip",
        filename=z.name,
    )

from __future__ import annotations
import subprocess, sys, shutil, json, os, time, hashlib, socket
from pathlib import Path
from typing import Literal, Optional
from fastapi import APIRouter, HTTPException, Query
from cryptotaxcalc.logging_setup import get_logger, _now_iso_z, _atomic_write_json
import importlib


router = APIRouter(prefix="/admin/demo", tags=["demo-build"])
logger = get_logger("demo.builder")

REPO_ROOT = Path(__file__).resolve().parents[2]

ARTIFACTS = (REPO_ROOT / "artifacts" / "demo")
ARTIFACTS.mkdir(parents=True, exist_ok=True)

EXE_READY = ARTIFACTS / "CryptoTaxCalc_Demo.exe"
EXE_TMP   = ARTIFACTS / "CryptoTaxCalc_Demo.tmp.exe"
MANIFEST  = ARTIFACTS / "demo_build_manifest.json"
BUILDLOG  = ARTIFACTS / "build.log"

_build_lock = False

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
    """
    Verify PyInstaller is available in the current interpreter.
    If not, either raise a clear HTTPException or auto-install when allowed.
    """
    try:
        importlib.import_module("PyInstaller")
        return
    except ImportError:
        pass

    # Optional auto-install if explicitly allowed (off by default)
    if os.getenv("ALLOW_DEMO_AUTO_INSTALL", "false").lower() == "true":
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
    else:
        raise HTTPException(
            status_code=400,
            detail="PyInstaller is not installed in this environment. "
                   "Activate your venv and run: "
                   "`python -m pip install pyinstaller==6.10.*` "
                   "Or set ALLOW_DEMO_AUTO_INSTALL=true to let the server install it."
        )

@router.post("/build_exe")
def build_exe(token: str = Query(..., description="Admin token")):
    # Security gate
    expected = os.getenv("ADMIN_TOKEN", "demo-admin")
    if token != expected:
        raise HTTPException(status_code=403, detail="Invalid token")
    global _build_lock
    if _build_lock:
        raise HTTPException(status_code=429, detail="A build is already running")
    _build_lock = True
    
    # ✅ Preflight: ensure PyInstaller present (and optionally auto-install)
    _ensure_pyinstaller()
    
    try:
        BUILDLOG.write_text("", encoding="utf-8")
        _write_manifest("queued", {"message": "Build queued"})

        # Run PyInstaller with our spec
        _write_manifest("building", {"message": "PyInstaller running"})

        # Windows requires ; as the separator for --add-data
        repo_root = REPO_ROOT
        src_dir = repo_root / "src"
        demo_dir = repo_root / "demo"
        logo_dir = repo_root / "logo"
        templates_dir = repo_root / "templates"
        static_dir = repo_root / "static"

        cmd = [
            sys.executable, "-m", "PyInstaller",
            "--noconfirm", "--clean", "--onefile",
            "--name", "CryptoTaxCalc_Demo",
            "--console",
            "--paths", str(src_dir),
            "--add-data", f"{demo_dir};demo",
            "--add-data", f"{logo_dir};logo",
            "--add-data", f"{templates_dir};templates",
            "--add-data", f"{static_dir};static",
        ]

        # Include FX seed file if present (improves demo reliability when DB is empty)
        fx_csv = (repo_root / "automation" / "fx_ecb.csv")
        if fx_csv.exists():
            cmd += ["--add-data", f"{fx_csv};automation"]

        cmd.append("src/cryptotaxcalc/demo_launcher.py")  # entrypoint

        with BUILDLOG.open("a", encoding="utf-8") as log_fp:
            with subprocess.Popen(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            ) as p:
                for line in p.stdout:
                    # append to build log incrementally
                    log_fp.write(line)
                    log_fp.flush()
                ret = p.wait()
                if ret != 0:
                    _write_manifest("failed", {"reason": "PyInstaller failed", "returncode": ret})
                    raise HTTPException(status_code=500, detail="Build failed")

        # After build, expect dist/CryptoTaxCalc_Demo.exe
        produced = Path("dist/CryptoTaxCalc_Demo.exe")
        if not produced.exists():
            _write_manifest("failed", {"reason": "EXE not found after build"})
            raise HTTPException(status_code=500, detail="No exe produced")

        # Verify: run exe with --smoke
        _write_manifest("verifying", {"message": "Launching smoke test"})
        shutil.copy2(produced, EXE_TMP)
        port = _find_free_port()
        with subprocess.Popen([str(EXE_TMP), "--smoke", f"--port={port}"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as p:
            # Wait a few seconds and poll /health
            time.sleep(3.0)
            # Offline smoke: we can't use requests; rely on process exit after smoke completes
            out, _ = p.communicate(timeout=60)
            ret = p.returncode
            if ret != 0:
                _write_manifest("failed", {"reason": "Smoke test failed", "ret": ret, "output_tail": (out or "")[-4000:]})
                raise HTTPException(status_code=500, detail="Smoke test failed")

        # Promote
        shutil.copy2(EXE_TMP, EXE_READY)
        sha = _sha256(EXE_READY)
        info = _write_manifest("ready", {
            "verified": True,
            "built_at": _now_iso_z(),
            "version": os.getenv("APP_VERSION", "dev"),
            "commit": os.getenv("GIT_COMMIT", "local"),
            "size": EXE_READY.stat().st_size,
            "sha256": sha,
            "path": str(EXE_READY),
        })
        return {"ok": True, "manifest": info}
    finally:
        _build_lock = False

@router.get("/build_status")
def build_status():
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {"status": "none"}

from __future__ import annotations

import os
import sys
import time
import subprocess
import re
from pathlib import Path as FSPath

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from .runtime_paths import PROJECT_ROOT, SUPPORT_BUNDLES_DIR, GIT_SCRIPT, LOG_DIR
from .security import require_admin_scripts, require_bundle_admin


router = APIRouter(tags=["admin"])


_SENSITIVE_TEXT_KEY_RE = re.compile(
    r"(DATABASE_URL|SQLALCHEMY_DATABASE_URL|DB_URL|TOKEN|SECRET|PASSWORD|PASS|KEY|AUTH|BEARER|COOKIE|SESSION)",
    re.IGNORECASE,
)
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?P<key>"
    r"(?:DATABASE_URL|SQLALCHEMY_DATABASE_URL|DB_URL)"
    r"|(?:[A-Za-z_][A-Za-z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASS|KEY|AUTH|BEARER|COOKIE|SESSION)[A-Za-z0-9_]*)"
    r")"
    r"(?P<sep>\s*[=:]\s*)"
    r"(?P<quote>['\"]?)"
    r"(?P<value>[^\s,'\"]+)",
    re.IGNORECASE,
)


def _redact_sensitive_text(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, dict):
        return {k: _redact_sensitive_text(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_sensitive_text(item) for item in value]

    text_value = str(value)

    def replace_assignment(match: re.Match[str]) -> str:
        return f"{match.group('key')}{match.group('sep')}{match.group('quote')}<redacted>"

    redacted = _SENSITIVE_ASSIGNMENT_RE.sub(replace_assignment, text_value)

    for key, env_value in os.environ.items():
        if not env_value or len(env_value) < 8:
            continue
        if _SENSITIVE_TEXT_KEY_RE.search(key):
            redacted = redacted.replace(env_value, "<redacted>")

    return redacted


def _latest_zip_path() -> str | None:
    zips = list((SUPPORT_BUNDLES_DIR).glob("support_bundle_*.zip"))
    if not zips:
        return None
    zips.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(zips[0])


def _latest_bundle_dir() -> str | None:
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


def _latest_log() -> FSPath | None:
    if not LOG_DIR.exists():
        return None
    files = sorted(LOG_DIR.glob("git_auto_push_*.log"))
    return files[-1] if files else None


@router.post("/admin/bundle", tags=["admin"])
def create_support_bundle(
    request: Request,
    _admin: None = Depends(require_bundle_admin),
):
    script = PROJECT_ROOT / "automation" / "collect_support_bundle.py"
    if not script.exists():
        raise HTTPException(status_code=500, detail=f"Bundle script not found: {script}")

    api_base = os.getenv("API_BASE", "http://127.0.0.1:8000")
    tail = 300
    cmd = [sys.executable, "-u", str(script), "--api-base", api_base, "--tail-lines", str(tail), "--keep-zips", "5"]

    env = os.environ.copy()
    env["RUN_CONTEXT"] = "api"
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    proc = subprocess.run(
        cmd,
        cwd=str(script.parent),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=600,
        env=env,
    )

    stdout = _redact_sensitive_text(proc.stdout or "")
    stderr = _redact_sensitive_text(proc.stderr or "")

    zip_path = None
    for line in stdout.splitlines():
        s = line.strip()
        if s.startswith("::zip::"):
            zip_path = s.split("::zip::", 1)[1].strip()
            break

    if not zip_path:
        time.sleep(0.5)
        zip_path = _latest_zip_path()

    # Windows locale guard: if printed path is mangled, re-resolve by basename.
    if zip_path:
        try:
            p = FSPath(zip_path)
            if not p.exists():
                candidate = SUPPORT_BUNDLES_DIR / FSPath(zip_path).name
                if candidate.exists():
                    zip_path = str(candidate)
        except Exception:
            pass

    zip_exists = bool(zip_path and os.path.exists(zip_path))

    if proc.returncode != 0 or not zip_exists:
        diag = {}
        latest_bundle_dir = _latest_bundle_dir()
        if latest_bundle_dir:
            def _read_if(p: str) -> str | None:
                try:
                    with open(p, "r", encoding="utf-8", errors="ignore") as f:
                        return _redact_sensitive_text(f.read()[:5000])
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
    }


@router.post("/admin/smoke", tags=["admin"])
def admin_smoke(_admin: None = Depends(require_admin_scripts)):
    runner = PROJECT_ROOT / "automation" / "run_smoke_and_email.py"
    if not runner.exists():
        raise HTTPException(status_code=500, detail=f"Runner not found: {runner}")

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
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

    latest_zip = _latest_zip_path()
    stdout_tail = (proc.stdout or "")[-8000:]
    stderr_tail = (proc.stderr or "")[-8000:]

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "return_code": proc.returncode,
        "latest_bundle_zip": latest_zip,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


@router.post("/admin/git-sync", tags=["admin"])
def admin_git_sync(_admin: None = Depends(require_admin_scripts)):
    proc = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy", "Bypass",
            "-File", str(GIT_SCRIPT),
        ],
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )

    log_path = _latest_log()
    log_tail = ""
    if log_path and log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="ignore")
            log_tail = text[-4000:]
        except Exception:
            log_tail = "<could not read log>"

    return {
        "status": "ok" if proc.returncode == 0 else "error",
        "script": str(GIT_SCRIPT),
        "return_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "log_path": str(log_path) if log_path else None,
        "log_tail": log_tail,
    }

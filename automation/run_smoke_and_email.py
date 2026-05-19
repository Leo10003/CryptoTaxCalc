#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
automation/run_smoke_and_email.py

Runs the smoke tests, collects artifacts into a ZIP under support_bundles/,
embeds a last_error_report.txt into that ZIP, and prints a JSON summary
for CI/log consumption.

Optional notifications (env-driven, off by default).
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from typing import Optional, Tuple
from pathlib import Path


# ------------------------------
# Paths & constants
# ------------------------------

def _project_root() -> pathlib.Path:
    """
    Try to anchor to the repo root by walking up until we see a familiar marker.
    Fallback to file's parent if markers aren't found (still deterministic).
    """
    here = pathlib.Path(__file__).resolve()
    for p in [here] + list(here.parents):
        if (p / "pyproject.toml").exists() or (p / ".git").exists() or (p / "tests").exists():
            return p if (p / "tests").exists() else p.parent if (p.name == "automation") else p
    # fallback: two levels up (common layout: repo/automation/run_smoke_and_email.py)
    return here.parents[1]


ROOT = _project_root()
TESTS_DIR = ROOT / "tests"
ARTIFACTS_DIR = ROOT / "artifacts"
SUPPORT_BUNDLES_DIR = ROOT / "support_bundles"

LAST_ERROR_REPORT_BASENAME = "last_error_report.txt"
LAST_ERROR_REPORT_PATH = SUPPORT_BUNDLES_DIR / LAST_ERROR_REPORT_BASENAME

# How many bytes of stdout/stderr we keep in the JSON for quick inspection
TAIL_MAX_BYTES = 4000

# ------------------------------
# Utilities
# ------------------------------

def _ensure_dirs() -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    SUPPORT_BUNDLES_DIR.mkdir(parents=True, exist_ok=True)


def _tail_bytes(s: str, limit: int = TAIL_MAX_BYTES) -> str:
    b = s.encode("utf-8", errors="replace")
    if len(b) <= limit:
        return s
    return b[-limit:].decode("utf-8", errors="replace")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _zip_write_text(zf: zipfile.ZipFile, arcname: str, text: str) -> None:
    zf.writestr(arcname, text.encode("utf-8"))


def _safe_glob(dirpath: pathlib.Path, pattern: str) -> list[pathlib.Path]:
    if not dirpath.exists():
        return []
    return sorted(dirpath.glob(pattern))


# ------------------------------
# Running pytest
# ------------------------------

def _run_pytest_smoke(tests_target: pathlib.Path, extra_args: Optional[list[str]] = None) -> Tuple[int, str, str]:
    """
    Run the smoke tests and capture stdout/stderr. Return (rc, out, err).
    """
    _ensure_dirs()

    cmd = [
        sys.executable, "-m", "pytest",
        str(tests_target),
        "-q",
        "--disable-warnings",
        "--maxfail=1",
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    # Ensure consistent output (e.g., no color codes)
    env["PYTEST_ADDOPTS"] = env.get("PYTEST_ADDOPTS", "") + " -s"
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except KeyboardInterrupt:
        # Pass up; main() will handle gracefully
        raise
    except Exception as e:
        return 2, "", f"[run_smoke_and_email] Exception while running pytest: {e!r}"


def _normalize_pytest_exit(rc: int, stdout: str) -> tuple[int, Optional[str]]:
    """
    Signal integrity policy:
      - Do NOT treat KeyboardInterrupt as success.
      - Do NOT treat non-zero exits as success based on regex heuristics.

    If you want to allow “manual Ctrl+C after tests finished” during local dev,
    set CTC_ALLOW_INTERRUPT_SUCCESS=1; then rc=130 is normalized to 0 only if
    the final summary clearly indicates a clean pass.
    """
    allow_interrupt = os.getenv("CTC_ALLOW_INTERRUPT_SUCCESS", "").strip().lower() in {"1", "true", "yes", "on"}

    if rc == 130 and allow_interrupt:
        summary_ok = bool(
            re.search(r"\b\d+\s+passed\b(?:,\s*\d+\s+deselected\b)?\s+in\s+\d+(\.\d+)?s", stdout)
        )
        no_fail = not re.search(r"\bfailed\b|\berror\b", stdout, flags=re.IGNORECASE)
        if summary_ok and no_fail:
            return 0, "Normalized exit: interrupt after completed pass (CTC_ALLOW_INTERRUPT_SUCCESS=1)."

    return rc, None


# ------------------------------
# Collect support bundle
# ------------------------------

def _gather_bundle_files(tmp_dir: pathlib.Path) -> list[tuple[pathlib.Path, str]]:
    """
    Decide which files to include. Returns list of (path, arcname).
    """
    files: list[tuple[pathlib.Path, str]] = []

    # Logs at repo root
    for p in _safe_glob(ROOT, "*.log"):
        files.append((p, f"logs/{p.name}"))

    # Artifacts (include everything once; don't explicitly re-add specific files)
    for p in _safe_glob(ARTIFACTS_DIR, "*"):
        if p.is_file():
            files.append((p, f"artifacts/{p.name}"))

    # Selected project sources for context (keep small)
    src_dir = ROOT / "src"
    if src_dir.exists():
        for p in _safe_glob(src_dir, "**/*.py"):
            try:
                rel = p.relative_to(ROOT)
            except Exception:
                continue
            if len(rel.parts) <= 6:  # shallow-ish
                files.append((p, f"src/{rel}"))

    # Tests (smoke file)
    smoke = TESTS_DIR / "smoke_test.py"
    if smoke.exists():
        files.append((smoke, "tests/smoke_test.py"))

    # Anything temp we created
    for p in _safe_glob(tmp_dir, "*"):
        if p.is_file():
            files.append((p, f"tmp/{p.name}"))

    return files


def _write_last_error_report(content: str) -> pathlib.Path:
    """
    Make sure last_error_report.txt lives under support_bundles/ and return its path.
    """
    _ensure_dirs()
    LAST_ERROR_REPORT_PATH.write_text(content, encoding="utf-8")
    return LAST_ERROR_REPORT_PATH


def _create_support_bundle_zip(note: Optional[str] = None) -> pathlib.Path:
    """
    Create a timestamped support bundle zip and embed the last_error_report.txt.
    Returns the created zip path.
    """
    _ensure_dirs()
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    zip_path = SUPPORT_BUNDLES_DIR / f"support_bundle_{ts}.zip"

    # Prepare a small temp area for additional files
    with tempfile.TemporaryDirectory() as td:
        tmpdir = pathlib.Path(td)

        # Build a basic manifest for the bundle itself
        bundle_manifest = {
            "bundle_created_at": _utc_now_iso(),
            "cwd": str(ROOT),
            "python": sys.version,
            "note": note or "",
        }
        (tmpdir / "BUNDLE_MANIFEST.json").write_text(
            json.dumps(bundle_manifest, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        # Write or refresh last_error_report.txt in the support_bundles folder
        # (The content may be updated later by caller; we ensure a file exists.)
        if not LAST_ERROR_REPORT_PATH.exists():
            _write_last_error_report("No error recorded.\n")

        files = _gather_bundle_files(tmpdir)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Always include last_error_report.txt at the bundle root
            if LAST_ERROR_REPORT_PATH.exists():
                zf.write(LAST_ERROR_REPORT_PATH, arcname=LAST_ERROR_REPORT_BASENAME)

            # Deduplicate by arcname to avoid duplicate warnings and flaky behavior
            seen_arcnames: set[str] = {LAST_ERROR_REPORT_BASENAME}

            for path, arc in files:
                if arc in seen_arcnames:
                    # Already added — skip silently
                    continue
                try:
                    zf.write(path, arcname=arc)
                    seen_arcnames.add(arc)
                except FileNotFoundError:
                    # Best-effort; keep going
                    pass

    return zip_path


# ------------------------------
# Optional notifications (no-ops unless configured)
# ------------------------------

def _maybe_send_email(subject: str, body: str) -> None:
    """
    Stub: Implement SMTP or API email by reading env vars if you want.
    Left as a no-op unless MAIL_* env vars are defined.
    """
    if not os.environ.get("MAIL_SMTP_HOST"):
        return
    # Implement your SMTP here if needed.
    # Intentionally left blank to avoid side effects for local runs.


def _maybe_send_telegram(message: str) -> None:
    """
    Stub: Implement Telegram bot notification by reading env vars if you want.
    Left as a no-op unless TG_* env vars are defined.
    """
    if not os.environ.get("TG_BOT_TOKEN") or not os.environ.get("TG_CHAT_ID"):
        return
    # Implement Telegram send here if needed.
    # Intentionally left blank to avoid side effects for local runs.

def _find_tests_start():
    """
    Return the best path to run pytest against:
    - Prefer <repo_root>/tests/smoke_test.py
    - else <repo_root>/tests
    - else raise with a helpful message
    """
    here = Path(__file__).resolve()
    # Heuristic repo root = parent containing either "tests" or "src"
    candidates = [here.parent, here.parent.parent, here.parent.parent.parent]
    tests_path = None
    for base in candidates:
        if not base or not base.exists():
            continue
        t1 = base / "tests" / "smoke_test.py"
        t2 = base / "tests"
        if t1.exists():
            tests_path = t1
            break
        if t2.exists():
            tests_path = t2
            # keep looking for smoke_test.py higher priority, but remember t2
    if tests_path is None:
        raise FileNotFoundError(
            "Could not locate tests. Expected 'tests/smoke_test.py' or a 'tests' folder "
            f"near {here}. Try running from your project root."
        )
    return tests_path

def _find_latest_bundle_zip() -> Optional[pathlib.Path]:
    zips = _safe_glob(SUPPORT_BUNDLES_DIR, "support_bundle_*.zip")
    return max(zips, key=lambda p: p.stat().st_mtime) if zips else None

# ------------------------------
# Main
# ------------------------------

def main():
    print("=== CryptoTaxCalc smoke runner ===")
    print(time.strftime("Started at %Y-%m-%dT%H:%M:%S%z"))
    _ensure_dirs()

    try:
        # Make sure tests exist early (so we can print a helpful error/bundle)
        tests_target = _find_tests_start()
        print(f"[smoke] using tests at: {tests_target}")

        # Use the subprocess runner so we capture output and can normalize exit codes.
        rc_raw, stdout, stderr = _run_pytest_smoke(tests_target)
        rc, note = _normalize_pytest_exit(rc_raw, stdout)

        # Write a concise last_error_report
        report_lines = [
            f"Exit code (raw): {rc_raw}",
            f"Exit code (normalized): {rc}",
            f"Started at: {_utc_now_iso()}",
        ]
        if note:
            report_lines.append(f"Note: {note}")
        _write_last_error_report("\n".join(report_lines) + "\n")

        # Always build a support bundle
        zip_path = _create_support_bundle_zip(note=note)

        payload = {
            "status": "ok" if rc == 0 else "error",
            "return_code": rc,
            "latest_bundle_zip": str(zip_path),
            "stdout_tail": _tail_bytes(stdout),
            "stderr_tail": _tail_bytes(stderr),
        }
        print(json.dumps(payload, indent=2))
        sys.exit(rc)

    except KeyboardInterrupt:
        allow_interrupt = os.getenv("CTC_ALLOW_INTERRUPT_SUCCESS", "").strip().lower() in {"1", "true", "yes", "on"}
        bundle_zip = _create_support_bundle_zip(note="interrupted")
        result = {
            "status": "ok" if allow_interrupt else "error",
            "message": "Interrupted",
            "latest_bundle_zip": str(bundle_zip),
            "return_code": 0 if allow_interrupt else 130,
        }
        print(json.dumps(result, indent=2))
        sys.exit(0 if allow_interrupt else 130)
    except FileNotFoundError as e:
        _write_last_error_report(f"FileNotFoundError: {e}\n")
        zip_path = _create_support_bundle_zip()
        print(json.dumps({
            "status": "error",
            "return_code": 4,
            "message": str(e),
            "latest_bundle_zip": str(zip_path),
        }, indent=2))
        sys.exit(4)
    except Exception as ex:
        _write_last_error_report(f"{type(ex).__name__}: {ex}\n")
        zip_path = _create_support_bundle_zip()
        print(json.dumps({
            "status": "error",
            "return_code": 2,
            "message": f"{type(ex).__name__}: {ex}",
            "latest_bundle_zip": str(zip_path),
        }, indent=2))
        sys.exit(2)

if __name__ == "__main__":
    main()

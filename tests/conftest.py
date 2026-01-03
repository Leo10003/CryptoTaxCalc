# tests/conftest.py
from __future__ import annotations

import os
import sys
import json
import traceback
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# Import exporter API
try:
    from cryptotaxcalc.exporter import build_export_zip, ExportOptions, ExportSettings
except Exception as e:
    # If import fails, we still let tests run; export step will report the issue.
    build_export_zip = None  # type: ignore[assignment]
    ExportOptions = None     # type: ignore[assignment]
    ExportSettings = None    # type: ignore[assignment]
    _import_error = e
else:
    _import_error = None

def _safe_export_zip(artifacts_dir: Path, files: dict[str, bytes], zip_name: str) -> str:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    zip_path = artifacts_dir / zip_name
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return str(zip_path)

def _truthy(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}

def _ts() -> str:
    # Filename-safe UTC timestamp for export artifacts
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

def pytest_sessionfinish(session, exitstatus: int) -> None:
    """
    After the entire test session:
      - If all tests passed and CTC_AUTO_EXPORT is truthy, build an export ZIP.
      - Always print clear status lines.
      - Emit manifest and last-export pointers; write a detailed error file on failure.
    """
    # Project root = parent of the tests directory
    root = Path(__file__).resolve().parent.parent
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    auto_export = _truthy(os.getenv("CTC_AUTO_EXPORT"))
    print(f"[EXPORT] auto_export={auto_export} exitstatus={exitstatus}")

    # If exporter couldn't be imported, log and quit gracefully
    if _import_error is not None:
        err_path = artifacts / "EXPORT_ERROR.txt"
        detail = (
            "[EXPORT] Failed to import 'exporter' module.\n\n"
            f"Exception: {_import_error!r}\n\n"
            f"Traceback:\n{traceback.format_exc()}\n"
        )
        err_path.write_text(detail, encoding="utf-8")
        print(f"[EXPORT] SKIP (import error). Details -> {err_path}")
        return

    if not auto_export:
        skip_path = artifacts / "EXPORT_SKIPPED.txt"
        skip_path.write_text(
            "CTC_AUTO_EXPORT is disabled. Set CTC_AUTO_EXPORT=1 (or true/yes/on) to enable.\n",
            encoding="utf-8",
        )
        print(f"[EXPORT] SKIP (feature flag). Details -> {skip_path}")
        return

    if exitstatus != 0:
        skip_path = artifacts / "EXPORT_SKIPPED.txt"
        skip_path.write_text(
            f"Tests did not pass (exitstatus={exitstatus}). Export skipped.\n",
            encoding="utf-8",
        )
        print(f"[EXPORT] SKIP (tests failed). Details -> {skip_path}")
        return

    # Build the export
    stamp = _ts()
    out_zip = artifacts / f"CryptoTaxCalc_Export_{stamp}.zip"
    manifest = {
        "created_at_utc": stamp.replace("_", " ").replace("-", ":"),
        "pytest_exitstatus": exitstatus,
        "root": str(root),
        "zip_name": out_zip.name,
        "zip_path": str(out_zip),
        "env": {
            # Non-sensitive toggles only; never log secrets
            "CTC_AUTO_EXPORT": os.getenv("CTC_AUTO_EXPORT", ""),
        },
    }

    try:
        print("[EXPORT] Building export ZIP…")
        # Customize options/settings if your exporter supports knobs
        opts = ExportOptions() if ExportOptions else None
        settings = ExportSettings() if ExportSettings else None

        data = build_export_zip(opts, settings)  # type: ignore[misc]
        out_zip.write_bytes(data)

        # Pointers & manifest
        (artifacts / "LATEST_EXPORT.txt").write_text(str(out_zip), encoding="utf-8")
        (artifacts / "EXPORT_MANIFEST.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )

        print(f"[EXPORT] OK -> {out_zip}")
    except Exception as e:
        err_path = artifacts / "EXPORT_ERROR.txt"
        manifest["error"] = {
            "type": type(e).__name__,
            "message": str(e),
        }
        (artifacts / "EXPORT_MANIFEST.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        err_path.write_text(
            f"[EXPORT] Exception while building export:\n\n{traceback.format_exc()}",
            encoding="utf-8",
        )
        print(f"[EXPORT] FAIL -> see {err_path}")

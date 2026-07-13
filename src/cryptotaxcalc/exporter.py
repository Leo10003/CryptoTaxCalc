# exporter.py
# CryptoTaxCalc — unified exports (support bundles + diagnostics)
from __future__ import annotations

import io
import os
import sys
import json
import time
import hashlib
import logging
import zipfile
import shutil
import re
import platform
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional, Dict, Any, List

# -----------------------------------------------------------------------------
# Public version for the module (bumped)
__version__ = "1.6.2"

log = logging.getLogger("cryptotaxcalc.export")

# -----------------------------------------------------------------------------
# Project root detection
def _find_project_root(start: Optional[Path] = None) -> Path:
    """
    Heuristically locate the project root. We try to find a directory that contains:
      - src/cryptotaxcalc
      - or top-level markers like .git, pyproject.toml, README.md
    Falls back to the parent of this file's parent if not found.
    """
    start = start or Path(__file__).resolve()
    candidates = list(start.parents)
    markers = {"pyproject.toml", "README.md", ".git"}

    for base in candidates:
        if (base / "src" / "cryptotaxcalc").exists():
            return base
        if any((base / m).exists() for m in markers):
            return base

    # default fallback: two levels up
    return Path(__file__).resolve().parents[2]

PROJECT_ROOT = _find_project_root()

# Default include sets
DEFAULT_INCLUDE_DIRS = [
    "src",          # full source tree with cryptotaxcalc/*
    "logo",         # icons/logos (e.g., icon_white.png, icon_black.png)
    "static",       # favicon, theme.css, glow.js
    "samples",      # sample.csv, sample2.csv
    "docs",         # (optional) Privacy & Terms
    "logs",         # runtime logs
]

DEFAULT_INCLUDE_FILES = [
    "README.md",
    "requirements.txt",
    "pyproject.toml",
    "setup.cfg",
    "demo_build_manifest.json",
    ".env.example",
]

# Exclusions to keep bundles lean
EXCLUDE_NAMES = {
    "__pycache__", ".pytest_cache", ".mypy_cache", ".venv", ".env",
    ".git", ".gitignore", ".DS_Store",
    "dist", "build", "artifacts",
}

# -----------------------------------------------------------------------------
# Dataclass for export options (compatible with prior calls)
@dataclass
class ExportOptions:
    # What to include
    include_source: bool = True
    include_logs: bool = True
    include_db: bool = True                 # kept for compatibility; DB may live under src
    include_manifest: bool = True
    include_env_example: bool = True
    include_samples: bool = True
    include_static: bool = True
    include_logo: bool = True
    include_docs: bool = True

    # Backward-compat: accept but ignore if passed by older code
    include_history: Optional[bool] = None

    # Output
    output_dir: Optional[Path] = None
    name_prefix: str = "diagnostics"        # or "support_bundle"

    # Extras
    extra_dirs: List[str] = field(default_factory=list)
    extra_files: List[str] = field(default_factory=list)

# Legacy alias for backward compatibility (older code imports ExportSettings)
ExportSettings = ExportOptions

# -----------------------------------------------------------------------------
# Helpers
def _iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if any(part in EXCLUDE_NAMES for part in p.parts):
            continue
        if p.is_file():
            yield p

def _sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def _rel(path: Path, base: Path) -> str:
    try:
        rel = path.relative_to(base)
    except ValueError:
        rel = path
    return str(rel).replace("\\", "/")

def _add_file(z: zipfile.ZipFile, file_path: Path, arcname: Optional[str] = None) -> bool:
    try:
        with file_path.open("rb") as fh:
            data = fh.read()
        z.writestr(arcname or _rel(file_path, PROJECT_ROOT), data, compress_type=zipfile.ZIP_DEFLATED)
        return True
    except Exception as e:
        log.warning("Skip file %s: %s", file_path, e)
        return False


_TEXT_DIAGNOSTIC_SUFFIXES = {
    ".txt",
    ".log",
    ".json",
    ".jsonl",
    ".csv",
    ".md",
}


def _add_issue_report_file(z: zipfile.ZipFile, file_path: Path, arcname: Optional[str] = None) -> bool:
    """
    Add a diagnostic file to an issue report.

    Text-like diagnostic files are redacted while being copied into the zip.
    The source file on disk is never modified.
    """
    try:
        final_arcname = arcname or _rel(file_path, PROJECT_ROOT)
        suffix = file_path.suffix.lower()

        if suffix in _TEXT_DIAGNOSTIC_SUFFIXES:
            raw = file_path.read_bytes()
            text = raw.decode("utf-8", errors="replace")
            redacted = _redact_issue_text(text)
            z.writestr(
                final_arcname,
                redacted.encode("utf-8"),
                compress_type=zipfile.ZIP_DEFLATED,
            )
            return True

        return _add_file(z, file_path, final_arcname)
    except Exception as e:
        log.warning("Skip issue report file %s: %s", file_path, e)
        return False


def _add_dir(z: zipfile.ZipFile, dir_path: Path) -> int:
    """Return number of files written"""
    count = 0
    if not dir_path.exists() or not dir_path.is_dir():
        return 0
    # Directory entry for readability
    arcdir = _rel(dir_path, PROJECT_ROOT).rstrip("/") + "/"
    z.writestr(arcdir, b"", compress_type=zipfile.ZIP_DEFLATED)
    for f in _iter_files(dir_path):
        if _add_file(z, f):
            count += 1
    return count

def _read_build_info() -> Dict[str, Any]:
    """
    Read demo_build_manifest.json if present to surface version/commit/build info in bundles.
    """
    manifest_path = PROJECT_ROOT / "demo_build_manifest.json"
    info: Dict[str, Any] = {
        "exporter_version": __version__,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if manifest_path.exists():
        try:
            with manifest_path.open("r", encoding="utf-8") as f:
                j = json.load(f)
            info.update({
                "demo_build_manifest": j,
            })
        except Exception as e:
            info["demo_build_manifest_error"] = str(e)
    return info

def _collect_manifest(added_paths: List[Path], zip_name: str) -> Dict[str, Any]:
    files = []
    for p in added_paths:
        try:
            files.append({
                "path": _rel(p, PROJECT_ROOT),
                "size": p.stat().st_size if p.exists() else None,
                "sha256": _sha256_file(p) if p.exists() else None,
            })
        except Exception as e:
            files.append({
                "path": _rel(p, PROJECT_ROOT),
                "size": None,
                "sha256": None,
                "error": str(e),
            })
    meta = {
        "bundle": zip_name,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "platform": {
            "python": sys.version,
            "os_name": os.name,
            "cwd": str(Path.cwd()),
        },
        "project_root": str(PROJECT_ROOT),
        "included_count": len(files),
        "files": files,
    }
    meta.update(_read_build_info())
    return meta

def _choose_dirs(opts: ExportOptions) -> List[str]:
    wanted = []
    if opts.include_source:
        wanted.append("src")
    if opts.include_logo:
        wanted.append("logo")
    if opts.include_static:
        wanted.append("static")
    if opts.include_samples:
        wanted.append("samples")
    if opts.include_docs:
        wanted.append("docs")
    if opts.include_logs:
        wanted.append("logs")
    # user extras
    wanted.extend(opts.extra_dirs or [])
    # de-dup while preserving order
    seen = set()
    out: List[str] = []
    for d in wanted:
        if d not in seen:
            out.append(d)
            seen.add(d)
    return out

def _choose_files(opts: ExportOptions) -> List[str]:
    wanted = []
    if opts.include_env_example:
        wanted.append(".env.example")
    if opts.include_manifest:
        wanted.append("demo_build_manifest.json")
    # always good to have these if present
    wanted.extend(["README.md", "pyproject.toml", "requirements.txt", "setup.cfg"])
    # user extras
    wanted.extend(opts.extra_files or [])
    # de-dup
    seen = set()
    out: List[str] = []
    for f in wanted:
        if f not in seen:
            out.append(f)
            seen.add(f)
    return out

def _safe_output_dir(path: Optional[Path]) -> Path:
    out = Path(path) if path else Path(os.getenv("TEMP", "."))  # TEMP or CWD
    out.mkdir(parents=True, exist_ok=True)
    return out

# -----------------------------------------------------------------------------
# PUBLIC: Issue report bundle
ISSUE_REPORT_DEFAULT_FILES = [
    "logs/latest_error_location.json",
    "logs/latest_error_location.txt",
    "logs/workspace/errors.txt",
    "logs/workspace/last_error.json",
    "logs/workspace/errors.jsonl",
    "logs/calc/last_run.json",
    "storage_raw/csv_sources/unsupported_structures.json",
]

ISSUE_REPORT_TRACE_GLOB = "logs/calc/runs/*/trace.json"


_SECRET_PATTERNS = [
    # Authorization headers / bearer tokens
    (
        re.compile(r"(?i)\b(authorization\s*:\s*bearer\s+)([A-Za-z0-9._~+/=-]{12,})"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(?i)\b(bearer\s+)([A-Za-z0-9._~+/=-]{12,})"),
        r"\1[REDACTED]",
    ),

    # Common key=value / key: value forms
    (
        re.compile(
            r"(?i)\b("
            r"admin_token|bundle_token|api_key|apikey|secret|password|passwd|pwd|token|access_token|refresh_token"
            r")(\s*[:=]\s*)([^\s,;\"']{4,})"
        ),
        r"\1\2[REDACTED]",
    ),

    # dotenv-style quoted values
    (
        re.compile(
            r"(?i)\b("
            r"admin_token|bundle_token|api_key|apikey|secret|password|passwd|pwd|token|access_token|refresh_token"
            r")(\s*[:=]\s*)([\"'])(.*?)([\"'])"
        ),
        r"\1\2\3[REDACTED]\5",
    ),
]


def _redact_issue_text(text: str) -> str:
    redacted = text
    for pattern, replacement in _SECRET_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _safe_issue_text(value: Optional[str], *, max_chars: int = 20_000) -> str:
    text = str(value or "").replace("\x00", "").strip()
    text = _redact_issue_text(text)
    if len(text) > max_chars:
        return text[:max_chars] + "\n...[truncated]"
    return text


_SUPPORT_SECRET_KEY_FRAGMENTS = (
    "secret",
    "token",
    "password",
    "passwd",
    "pwd",
    "credential",
    "credentials",
    "api_key",
    "apikey",
    "private_key",
)


def _is_support_secret_key(key: Any) -> bool:
    key_text = str(key or "").strip().lower()
    return any(fragment in key_text for fragment in _SUPPORT_SECRET_KEY_FRAGMENTS)


def _safe_issue_value(value: Any, *, key: Any = "") -> Any:
    if _is_support_secret_key(key):
        return "[REDACTED]"

    if isinstance(value, dict):
        return {
            str(child_key): _safe_issue_value(child_value, key=child_key)
            for child_key, child_value in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [_safe_issue_value(item) for item in value]

    if isinstance(value, str):
        return _safe_issue_text(value)

    if isinstance(value, (int, float, bool)) or value is None:
        return value

    return _safe_issue_text(str(value))


def _issue_report_environment_snapshot() -> Dict[str, Any]:
    """
    Return non-sensitive runtime metadata for debugging issue reports.

    Deliberately avoids:
      - full filesystem paths
      - environment variables
      - usernames
      - hostnames
      - database paths
      - raw imported data paths
    """
    diagnostics = {
        rel: (PROJECT_ROOT / rel).exists()
        for rel in ISSUE_REPORT_DEFAULT_FILES
    }

    trace_root = PROJECT_ROOT / "logs" / "calc" / "runs"
    trace_count = 0
    if trace_root.exists():
        trace_count = sum(1 for p in trace_root.glob("*/trace.json") if p.exists() and p.is_file())

    return {
        "exporter_version": __version__,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "os_name": os.name,
        "system": platform.system(),
        "machine": platform.machine(),
        "cwd_name": Path.cwd().name,
        "project_root_name": PROJECT_ROOT.name,
        "diagnostics_present": diagnostics,
        "calc_trace_count": trace_count,
    }


def _issue_report_payload(
    *,
    user_message: Optional[str] = None,
    contact: Optional[str] = None,
    app_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "kind": "issue_report",
        "user_message": _safe_issue_text(user_message),
        "contact": _safe_issue_text(contact, max_chars=500),
        "app_context": _safe_issue_value(app_context or {}),
        "environment": _issue_report_environment_snapshot(),
        "privacy_note": (
            "This issue report includes diagnostic logs and traces only by default. "
            "It does not intentionally include raw imported CSV files, database snapshots, "
            "environment files, virtual environments, or build artifacts."
        ),
    }
    return payload


def _issue_report_candidate_files() -> List[Path]:
    candidates: List[Path] = []

    for rel in ISSUE_REPORT_DEFAULT_FILES:
        p = PROJECT_ROOT / rel
        if p.exists() and p.is_file():
            candidates.append(p)

    for p in (PROJECT_ROOT / "logs" / "calc" / "runs").glob("*/trace.json"):
        if p.exists() and p.is_file():
            candidates.append(p)

    # De-dup while preserving order.
    seen: set[str] = set()
    out: List[Path] = []
    for p in candidates:
        key = str(p.resolve())
        if key in seen:
            continue
        seen.add(key)
        out.append(p)

    return out


def _issue_report_inventory(candidate_files: List[Path]) -> Dict[str, Any]:
    included = {_rel(p, PROJECT_ROOT) for p in candidate_files}

    expected_static = list(ISSUE_REPORT_DEFAULT_FILES)
    missing_static = [
        rel
        for rel in expected_static
        if rel not in included and not (PROJECT_ROOT / rel).exists()
    ]

    trace_root = PROJECT_ROOT / "logs" / "calc" / "runs"
    trace_files = sorted(
        _rel(p, PROJECT_ROOT)
        for p in trace_root.glob("*/trace.json")
        if p.exists() and p.is_file()
    ) if trace_root.exists() else []

    return {
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "kind": "diagnostics_inventory",
        "included_files": sorted(included),
        "missing_expected_files": sorted(missing_static),
        "trace_files": trace_files,
        "privacy_omissions": {
            "raw_import_csv_files": "excluded_by_default",
            "database_snapshots": "excluded_by_default",
            "environment_files": "excluded_by_default",
            "virtualenv": "excluded_by_default",
            "full_source_tree": "excluded_by_default",
            "build_artifacts": "excluded_by_default",
        },
    }


def _append_issue_report_index(
    *,
    zip_path: Path,
    inventory: Dict[str, Any],
) -> None:
    """
    Append a safe local audit row for generated issue reports.

    This index intentionally stores only bundle metadata, never user messages,
    contact details, raw CSV contents, database contents, or full environment data.
    """
    try:
        meta_dir = PROJECT_ROOT / "support_bundles" / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        row = {
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "kind": "issue_report_index_entry",
            "filename": zip_path.name,
            "path": str(zip_path.resolve()),
            "size_bytes": zip_path.stat().st_size if zip_path.exists() else 0,
            "sha256": _sha256_file(zip_path) if zip_path.exists() else None,
            "included_file_count": len(inventory.get("included_files") or []),
            "missing_expected_file_count": len(inventory.get("missing_expected_files") or []),
            "trace_file_count": len(inventory.get("trace_files") or []),
            "raw_data_included": False,
            "database_included": False,
        }

        with (meta_dir / "issue_reports.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as e:
        # Indexing must never break report generation.
        log.warning("Could not append issue report index entry for %s: %s", zip_path, e)


def build_issue_report_bundle(
    *,
    user_message: Optional[str] = None,
    contact: Optional[str] = None,
    app_context: Optional[Dict[str, Any]] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Build a small, privacy-conscious issue report bundle.

    Default contents:
      - issue_report.json with user-supplied description/contact/context
      - latest diagnostic pointers
      - workspace error logs
      - calculation last_run.json and per-run trace.json files
      - unsupported CSV structure registry
      - manifest with file hashes

    It deliberately avoids raw CSVs, DB files, .env, .venv, artifacts, and full source.
    """
    out_dir = _safe_output_dir(output_dir or (PROJECT_ROOT / "support_bundles"))
    ts = time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime())
    zip_name = f"issue_report_{ts}.zip"
    zip_path = out_dir / zip_name

    added_paths: List[Path] = []
    candidate_files = _issue_report_candidate_files()
    inventory = _issue_report_inventory(candidate_files)
    payload = _issue_report_payload(
        user_message=user_message,
        contact=contact,
        app_context=app_context,
    )

    log.info("Building issue report bundle at %s", zip_path)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(
            "issue_report.json",
            json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8"),
            compress_type=zipfile.ZIP_DEFLATED,
        )

        for fp in candidate_files:
            if _add_issue_report_file(z, fp, arcname=_rel(fp, PROJECT_ROOT)):
                added_paths.append(fp)

        z.writestr(
            "diagnostics_inventory.json",
            json.dumps(inventory, indent=2, ensure_ascii=False).encode("utf-8"),
            compress_type=zipfile.ZIP_DEFLATED,
        )

        readme = (
            "CryptoTaxCalc — Issue Report\n"
            "----------------------------\n"
            "This archive is intended for debugging a client-reported issue.\n\n"
            "Default contents:\n"
            "- issue_report.json: user message/contact/context\n"
            "- diagnostics_inventory.json: included/missing diagnostics and privacy omissions\n"
            "- logs/latest_error_location.*: pointer to the latest component error\n"
            "- logs/workspace/*: workspace calculation/import error logs\n"
            "- logs/calc/last_run.json: latest calculation diagnostics\n"
            "- logs/calc/runs/*/trace.json: per-run calculation traces\n"
            "- storage_raw/csv_sources/unsupported_structures.json: unknown CSV structures\n\n"
            "Privacy note:\n"
            "This default issue report does not intentionally include raw imported CSV files, "
            "database snapshots, .env files, virtual environments, or full source code.\n"
        ).encode("utf-8")
        z.writestr("README_ISSUE_REPORT.txt", readme, compress_type=zipfile.ZIP_DEFLATED)

        meta = _collect_manifest(added_paths, zip_name)
        meta["issue_report"] = {
            "included_diagnostic_files": [_rel(p, PROJECT_ROOT) for p in added_paths],
            "raw_data_included": False,
            "database_included": False,
        }
        z.writestr("_meta/bundle_manifest.json", json.dumps(meta, indent=2).encode("utf-8"))

    _append_issue_report_index(zip_path=zip_path, inventory=inventory)

    log.info("Issue report bundle ready: %s", zip_path)
    return zip_path

# -----------------------------------------------------------------------------
# PUBLIC: Diagnostics export (used by /demo/diagnostics/export)
def build_export_zip(opts: Optional[ExportOptions] = None) -> Path:
    """
    Build a diagnostics-focused zip. Safe when some folders are missing.
    Previously callers passed ExportOptions(include_history=...), which we accept.
    """
    opts = opts or ExportOptions(name_prefix="diagnostics")
    out_dir = _safe_output_dir(opts.output_dir)
    ts = time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime())
    zip_name = f"{opts.name_prefix}_{ts}.zip"
    zip_path = out_dir / zip_name

    added_paths: List[Path] = []
    chosen_dirs = _choose_dirs(opts)
    chosen_files = _choose_files(opts)

    log.info("Building export zip at %s", zip_path)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # Write directories (if exist)
        for d in chosen_dirs:
            dp = PROJECT_ROOT / d
            if not dp.exists():
                log.warning("Skip dir (missing): %s", dp)
                continue
            # record files we are adding for manifest
            for f in _iter_files(dp):
                if _add_file(z, f, arcname=_rel(f, PROJECT_ROOT)):
                    added_paths.append(f)
            # ensure a directory entry
            z.writestr(f"{d}/", b"", compress_type=zipfile.ZIP_DEFLATED)

        # Write files (if present)
        for f in chosen_files:
            fp = PROJECT_ROOT / f
            if fp.exists() and fp.is_file():
                if _add_file(z, fp, arcname=_rel(fp, PROJECT_ROOT)):
                    added_paths.append(fp)
            else:
                log.warning("Skip file (missing): %s", fp)

        # Human help readme
        readme = (
            "CryptoTaxCalc — Diagnostics Export\n"
            "----------------------------------\n"
            "This archive contains logs, source (if included), static assets, samples,\n"
            "and build manifest (if present). Use it to investigate issues or demo state.\n\n"
            "Quick start:\n"
            "- Run `uvicorn cryptotaxcalc.app:app --reload` from src/ to start locally.\n"
            "- Demo UI: /demo/dashboard\n"
            "- Support bundle variant also includes full project context.\n"
        ).encode("utf-8")
        z.writestr("README_DIAGNOSTICS.txt", readme, compress_type=zipfile.ZIP_DEFLATED)

        # Meta manifest
        meta = _collect_manifest(added_paths, zip_name)
        z.writestr("_meta/bundle_manifest.json", json.dumps(meta, indent=2).encode("utf-8"))

    log.info("Export zip ready: %s", zip_path)
    return zip_path

# -----------------------------------------------------------------------------
# PUBLIC: Support bundle (for continuing work in another chat)
def build_support_bundle(output_dir: Optional[Path] = None) -> Path:
    """
    Create a single zip that contains everything needed to resume development:
      - src/, logo/, static/, samples/, docs/, logs/
      - root files (README.md, requirements.txt, demo_build_manifest.json, .env.example)
      - _meta/bundle_manifest.json with hashes and build info
      - README_SUPPORT_BUNDLE.txt guide
    """
    opts = ExportOptions(
        include_source=True,
        include_logs=True,
        include_db=True,
        include_manifest=True,
        include_env_example=True,
        include_samples=True,
        include_static=True,
        include_logo=True,
        include_docs=True,
        output_dir=output_dir,
        name_prefix="support_bundle",
    )
    out_dir = _safe_output_dir(opts.output_dir)
    ts = time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime())
    zip_name = f"{opts.name_prefix}_{ts}.zip"
    zip_path = out_dir / zip_name

    added_paths: List[Path] = []
    chosen_dirs = _choose_dirs(opts)
    chosen_files = _choose_files(opts)

    log.info("Building support bundle at %s", zip_path)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # All chosen dirs
        for d in chosen_dirs:
            dp = PROJECT_ROOT / d
            if not dp.exists():
                log.warning("Skip dir (missing): %s", dp)
                continue
            for f in _iter_files(dp):
                if _add_file(z, f, arcname=_rel(f, PROJECT_ROOT)):
                    added_paths.append(f)
            # directory entry
            z.writestr(f"{d}/", b"", compress_type=zipfile.ZIP_DEFLATED)

        # All chosen single files
        for f in chosen_files:
            fp = PROJECT_ROOT / f
            if fp.exists() and fp.is_file():
                if _add_file(z, fp, arcname=_rel(fp, PROJECT_ROOT)):
                    added_paths.append(fp)
            else:
                log.warning("Skip file (missing): %s", fp)

        # Helpful how-to
        readme = (
            "CryptoTaxCalc — Support Bundle\n"
            "--------------------------------\n"
            "This bundle was generated to resume work seamlessly (even in another chat).\n\n"
            "Contains:\n"
            "- src/: full FastAPI backend + demo components\n"
            "- logo/, static/: UI assets (logos, CSS, JS)\n"
            "- samples/: CSVs used in demo\n"
            "- logs/: recent runtime logs\n"
            "- docs/: Terms & Privacy (if present)\n"
            "- demo_build_manifest.json: build metadata (if present)\n\n"
            "Quick start:\n"
            "1) `cd src` then `uvicorn cryptotaxcalc.app:app --reload`\n"
            "2) Open http://127.0.0.1:8000/demo/dashboard\n"
            "3) To package, install PyInstaller and use your existing build script.\n"
        ).encode("utf-8")
        z.writestr("README_SUPPORT_BUNDLE.txt", readme, compress_type=zipfile.ZIP_DEFLATED)

        # Meta manifest
        meta = _collect_manifest(added_paths, zip_name)
        z.writestr("_meta/bundle_manifest.json", json.dumps(meta, indent=2).encode("utf-8"))

    log.info("Support bundle ready: %s", zip_path)
    return zip_path

# -----------------------------------------------------------------------------
# Convenience (kept for compatibility if someone used the old name)
def build_export_archive(opts: Optional[ExportOptions] = None) -> Path:
    return build_export_zip(opts)

# -----------------------------------------------------------------------------
# Minimal CLI (optional)
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="CryptoTaxCalc exports")
    ap.add_argument("--support", action="store_true", help="Create a support bundle (full project context)")
    ap.add_argument("--out", type=str, default=None, help="Output directory for the zip")
    args = ap.parse_args()

    if args.support:
        out = build_support_bundle(output_dir=Path(args.out) if args.out else None)
        print(out)
    else:
        out = build_export_zip(ExportOptions(output_dir=Path(args.out) if args.out else None))
        print(out)

__all__ = [
    "ExportOptions",
    "ExportSettings",
    "build_export_zip",
    "build_support_bundle",
    "build_issue_report_bundle",
    "build_export_archive",
    "__version__",
]

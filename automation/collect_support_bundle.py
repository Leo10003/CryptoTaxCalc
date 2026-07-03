#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
automation/collect_support_bundle.py

Drop-in Python replacement for the PowerShell bundle collector.
- No external dependencies (stdlib only)
- Writes rich diagnostics into a temp bundle folder
- Zips the folder, keeps last N zips, prints "::zip:: <path>" to stdout

Usage (CLI):
  python automation/collect_support_bundle.py --api-base http://127.0.0.1:8000 --tail-lines 300 --keep-zips 5

This script is safe to call from your FastAPI endpoint. It prints the "::zip:: ..." marker to STDOUT.
Exit code: 0 on success, 2 on zip failure, 1 on other fatal errors.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import time
import traceback
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

# Ensure the ::zip:: marker is UTF-8 even on Windows consoles (prevents path mangling).
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# ------------- Config defaults -------------
EXPECTED_TABLES_DEFAULT = [
    "calc_runs",
    "transactions",
    "fx_rates",
    "fx_batches",
    "audit_log",
    "realized_events",
    "run_digests",
]
BASE_COLLECT_LIST = [
    "README.md",
    "LICENSE",
    "pyproject.toml",
    "requirements.txt",
    "Pipfile",
    "Pipfile.lock",
    "poetry.lock",
    "uv.lock",
    "automation",
    "samples",
    "src",
    "tests",
    "support_bundles/.keep",
]

SENSITIVE_COLLECT_LIST = [
    "storage_raw",
    "storage_normalized",
    "backups",
]

RAW_LOGS_COLLECT_LIST = [
    "logs",
]

# NOTE: we NEVER copy .env files into bundles. If present, we write a redacted snapshot into _meta.
DOTENV_BASENAMES = [
    ".env",
    ".env.local",
    ".env.production",
    ".env.development",
]

def build_collect_list(include_data: bool, include_raw_logs: bool) -> List[str]:
    items = list(BASE_COLLECT_LIST)
    if include_data:
        items.extend(SENSITIVE_COLLECT_LIST)
    if include_raw_logs:
        items.extend(RAW_LOGS_COLLECT_LIST)
    return items
API_ENDPOINTS = ["/health", "/version"]

# Inventory safeguards
INV_MAX_FILES      = 10_000   # stop after this many files
INV_HASH_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per-file hashing cap
INV_HASH_TIME_BUDGET_SEC = 2.0         # per-file hashing time budget
INV_SKIP_EXTS = {
    ".zip", ".7z", ".rar",
    ".db", ".sqlite", ".sqlite3", ".parquet",
    ".mp4", ".mov", ".avi", ".mp3", ".wav", ".iso",
}
# Skip subtrees that are noisy/huge/irrelevant inside the bundle copy
INV_SKIP_DIRNAMES = {
    "node_modules", ".git", "__pycache__", ".mypy_cache", ".pytest_cache"
}

# --- Log collection safeguards ---
LOG_MAX_FILES = 500                       # stop after this many log files
LOG_TAIL_MAX_BYTES = 2 * 1024 * 1024      # cap output per file ~2 MB
LOG_PER_FILE_TIME_BUDGET_SEC = 2.0        # time budget for tailing + writing
LOG_SKIP_EXTS = {".zip", ".7z", ".rar", ".gz", ".bz2", ".xz", ".lz4", ".zst", ".db", ".sqlite", ".sqlite3"}
LOG_SKIP_DIRNAMES = {"node_modules", ".git", "__pycache__", ".mypy_cache", ".pytest_cache"}

# Inventory safeguards (API-fast mode tweaks)
INV_API_MAX_FILES = 2_000   # when RUN_CONTEXT=api, stop after this many files

# --- Zip safeguards ---
ZIP_SKIP_EXTS = {
    ".zip", ".7z", ".rar", ".gz", ".bz2", ".xz", ".lz4", ".zst",
    ".iso", ".mp4", ".mov", ".avi", ".mp3", ".wav",
}
ZIP_API_MAX_FILE_BYTES = 50 * 1024 * 1024   # 50 MB cap per file in API context
ZIP_MAX_FILES = 20000                        # safety cap

# ------------------------------------------


def now_stamp() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def write_text_safe(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Normalize newlines for readability
    path.write_text(content.replace("\r\n", "\n"), encoding="utf-8")


def append_text_safe(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(content)


def first_existing(candidates: Iterable[Path]) -> Optional[Path]:
    for c in candidates:
        if c and c.exists():
            return c
    return None

# --- Redaction helpers (support bundles must be safe to share) ---
SENSITIVE_KEY_RE = re.compile(r"(TOKEN|SECRET|PASSWORD|PASS|KEY|AUTH|BEARER|COOKIE|SESSION)", re.IGNORECASE)
SAFE_ENV_VALUE_KEYS = {
    "API_BASE",
    "DB_PATH",
    "RUN_CONTEXT",
}
SAFE_ENV_VALUE_PREFIXES = ("CTC_",)
MAX_ENV_VALUE_CHARS = 400

def _truncate(s: str, max_len: int) -> str:
    if s is None:
        return ""
    s = str(s)
    return s if len(s) <= max_len else (s[: max_len - 3] + "...")

def redact_kv(key: str, value: str) -> str:
    if SENSITIVE_KEY_RE.search(key or ""):
        return "<redacted>"
    return _truncate(value, MAX_ENV_VALUE_CHARS)

def build_env_keys_snapshot(env: dict) -> str:
    keys = sorted((str(k) for k in env.keys()), key=str.lower)
    return "\n".join(keys) + "\n"

def build_env_config_snapshot(env: dict) -> str:
    """Include values only for explicitly allowed keys/prefixes; redact sensitive keys."""
    keys = sorted((str(k) for k in env.keys()), key=str.lower)
    out = []
    for k in keys:
        v = str(env.get(k, ""))
        allow_value = (k in SAFE_ENV_VALUE_KEYS) or any(k.startswith(p) for p in SAFE_ENV_VALUE_PREFIXES)
        if allow_value:
            out.append(f"{k}={redact_kv(k, v)}")
        else:
            out.append(f"{k}=<omitted>")
    return "\n".join(out) + "\n"

def redact_dotenv_text(text: str) -> str:
    out = []
    for raw in text.replace("\r\n", "\n").split("\n"):
        line = raw.rstrip("\r")
        if not line or line.lstrip().startswith("#") or "=" not in line:
            out.append(line)
            continue
        left, right = line.split("=", 1)
        left = left.strip()
        prefix = ""
        key = left
        if left.lower().startswith("export "):
            prefix = "export "
            key = left[7:].strip()
        val = right.strip()
        safe_val = redact_kv(key, val)
        out.append(f"{prefix}{key}={safe_val}")
    return "\n".join(out).strip() + "\n"


class StateLog:
    def __init__(self, meta_dir: Path):
        self.path = meta_dir / "states.log"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, label: str) -> None:
        line = f"[{dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}] {label}\n"
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line)


def get_project_root(script_path: Path) -> Path:
    # project root = parent of automation dir
    return script_path.resolve().parent.parent


def preflight(proj_root: Path, bundle_dir: Path, zip_path: Path, meta_dir: Path, api_base: str, tail_lines: int, include_data: bool, include_raw_logs: bool) -> None:
    lines = []
    lines.append("=== Preflight Report ===")
    lines.append(f"Timestamp: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Project Root: {proj_root}")
    lines.append(f"OS: {os.name} | Platform: {sys.platform}")
    lines.append(f"Python: {sys.version}")
    lines.append(f"ApiBase: {api_base}")
    lines.append(f"TailLines: {tail_lines}")
    lines.append(f"BundleDir: {bundle_dir}")
    lines.append(f"ZipTarget: {zip_path}")
    lines.append("")
    # Writeability check
    try:
        test = (proj_root / "support_bundles" / ".writetest")
        test.parent.mkdir(parents=True, exist_ok=True)
        test.write_text("ok", encoding="ascii")
        test.unlink(missing_ok=True)
        lines.append("OK: write test passed.")
    except Exception as e:
        lines.append(f"ERROR: Cannot write to {proj_root / 'support_bundles'} - zipping will likely fail. err={e!r}")
    write_text_safe(meta_dir / "preflight_report.txt", "\n".join(lines))

    # meta summary files
    # Bundle policy / safety notes (never include secrets by default)
    policy = []
    policy.append("=== Bundle Policy ===")
    policy.append(f"include_data={include_data}")
    policy.append(f"include_raw_logs={include_raw_logs}")
    policy.append("")
    policy.append("This bundle is intended for troubleshooting. Review contents before sharing.")
    policy.append("Secrets are redacted; .env files are not copied into the bundle.")
    if not include_data:
        policy.append("Raw storage/backups are excluded by default (use --include-data to include).")
    if not include_raw_logs:
        policy.append("Full log directories are excluded by default (use --include-raw-logs to include).")
        policy.append("Log tails are included under _logs/.")
    write_text_safe(meta_dir / "bundle_policy.txt", "\n".join(policy))

    # Environment snapshot (safe-by-default): keys + redacted config values
    try:
        write_text_safe(meta_dir / "env_keys.txt", build_env_keys_snapshot(os.environ))
        write_text_safe(meta_dir / "env_config.txt", build_env_config_snapshot(os.environ))
    except Exception as e:
        write_text_safe(meta_dir / "env_snapshot_error.txt", f"{e}\n{traceback.format_exc()}")

    # Redacted .env snapshot (if present) - never includes secrets
    try:
        dotenv_src = first_existing([proj_root / b for b in DOTENV_BASENAMES])
        if dotenv_src:
            redacted = redact_dotenv_text(dotenv_src.read_text(encoding="utf-8", errors="replace"))
            write_text_safe(meta_dir / "dotenv_redacted.env", redacted)
            write_text_safe(meta_dir / "dotenv_source.txt", str(dotenv_src))
    except Exception as e:
        write_text_safe(meta_dir / "dotenv_redacted_error.txt", f"{e}\n{traceback.format_exc()}")

    host_info = io.StringIO()
    host_info.write("=== Host Info (Python stdlib) ===\n")
    host_info.write(f"cwd={Path.cwd()}\n")
    host_info.write(f"argv={sys.argv}\n")
    write_text_safe(meta_dir / "host_info.txt", host_info.getvalue())

    # ps_version.json analogue (python)
    ps_version = {
        "python_version": sys.version,
        "executable": sys.executable,
        "platform": sys.platform,
    }
    write_text_safe(meta_dir / "ps_version.json", json.dumps(ps_version, indent=2))


def safe_copy_tree(proj_root: Path, bundle_dir: Path, copy_errors: Path, st: StateLog, include_data: bool, include_raw_logs: bool) -> None:
    collect_list = build_collect_list(include_data=include_data, include_raw_logs=include_raw_logs)

    def _ignore(dirpath: str, names: List[str]):
        ignore = set()
        # Never copy nested support bundles
        if "support_bundles" in Path(dirpath).parts:
            return set(names)

        # Never copy dotenv files (we include only a redacted snapshot in _meta)
        for n in names:
            if n in DOTENV_BASENAMES:
                ignore.add(n)

        # Skip raw logs by default (we include tails under _logs/)
        if not include_raw_logs and "logs" in names:
            ignore.add("logs")

        # Skip sensitive data directories by default
        if not include_data:
            for n in ("storage_raw", "storage_normalized", "backups"):
                if n in names:
                    ignore.add(n)

        # Skip noisy/huge dirs always
        for n in names:
            if n in INV_SKIP_DIRNAMES:
                ignore.add(n)
        return ignore

    for rel in collect_list:
        src = proj_root / rel
        dst = bundle_dir / rel
        try:
            if not src.exists():
                continue
            # never recurse into support_bundles
            if src.is_dir() and "support_bundles" in src.parts:
                continue
            if src.is_dir():
                # shutil.copytree with dirs_exist_ok + ignore rules
                shutil.copytree(src, dst, dirs_exist_ok=True, ignore=_ignore)
            else:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
        except Exception as e:
            append_text_safe(copy_errors, f"Failed to copy: {rel}\n{e}\n---\n")
    st.write("COPY_DONE")


def python_diag(meta_dir: Path, st: StateLog) -> None:
    """Collect Python diagnostics quickly and safely."""
    st.write("PYTHON_BEGIN")
    # 1) Basic interpreter info
    try:
        write_text_safe(meta_dir / "python_version.txt", sys.version)
        write_text_safe(meta_dir / "python_executable.txt", sys.executable)
    except Exception as e:
        write_text_safe(meta_dir / "python_version_error.txt", f"{e}\n{traceback.format_exc()}")

    # Skip pip diagnostics if running inside a web server to avoid blocking
    if "FASTAPI_ENV" in os.environ or os.getenv("RUN_CONTEXT") == "api":
        write_text_safe(meta_dir / "python_diag_skipped.txt",
                        "Skipped pip subprocess due to web context")
        st.write("PYTHON_DONE")
        return
    
    # 2) Try the fastest/cleanest: `pip list --format=freeze`
    #    Use the same interpreter that's running this script.
    try:
        import subprocess
        proc = subprocess.run(
            [sys.executable, "-m", "pip", "list", "--format=freeze"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=20,   # keep it snappy; avoid hanging
        )
        if proc.returncode == 0 and proc.stdout:
            write_text_safe(meta_dir / "pip_freeze.txt", proc.stdout.strip())
            st.write("PYTHON_DONE")
            return
        else:
            write_text_safe(meta_dir / "pip_list_error.txt",
                            f"return_code={proc.returncode}\nSTDERR:\n{proc.stderr}")
    except Exception as e:
        write_text_safe(meta_dir / "pip_list_error.txt", f"{e}\n{traceback.format_exc()}")

    # 3) Fallback A: pkg_resources (no external process)
    try:
        import pkg_resources  # type: ignore
        rows = [f"{d.project_name}=={d.version}" for d in pkg_resources.working_set]
        rows.sort(key=str.lower)
        write_text_safe(meta_dir / "pip_freeze.txt", "\n".join(rows))
        st.write("PYTHON_DONE")
        return
    except Exception as e:
        write_text_safe(meta_dir / "pip_freeze_error_pkg_resources.txt", f"{e}\n{traceback.format_exc()}")

    # 4) Fallback B: importlib.metadata but **never** touch dist.metadata
    try:
        import importlib.metadata as imd  # py3.8+
        rows = []
        for dist in imd.distributions():
            # Avoid dist.metadata which can be slow/problematic
            name = getattr(dist, "name", None) or "unknown"
            version = getattr(dist, "version", None) or "0"
            rows.append(f"{name}=={version}")
        rows.sort(key=str.lower)
        write_text_safe(meta_dir / "pip_freeze.txt", "\n".join(rows))
    except Exception as e:
        write_text_safe(meta_dir / "pip_freeze_error_importlib.txt", f"{e}\n{traceback.format_exc()}")

    st.write("PYTHON_DONE")


def _sqlite_tail(cur: sqlite3.Cursor, query: str):
    try:
        cur.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        return {"columns": cols, "rows": cur.fetchall()}
    except Exception as e:
        return {"error": str(e), "query": query}


def db_diag(proj_root: Path, db_meta_dir: Path, expected_tables: List[str], st: StateLog) -> None:
    st.write("DB_BEGIN")
    db_candidates = [
        proj_root / "cryptotaxcalc.db",
        proj_root / "data.db",
        proj_root / "src" / "cryptotaxcalc" / "cryptotaxcalc.db",
    ]
    db_path = first_existing(db_candidates)
    manifest_hint = {"db_path": str(db_path) if db_path else None}
    write_text_safe(db_meta_dir / "db_hint.json", json.dumps(manifest_hint, indent=2))

    if not db_path:
        write_text_safe(db_meta_dir / "db_diag_skipped.txt", "Skipped DB diag: no db found")
        # still write expected tables
        write_text_safe(db_meta_dir / "expected_tables.txt", "\n".join(expected_tables))
        write_text_safe(db_meta_dir / "missing_tables.txt", "db not found; cannot compute")
        st.write("DB_DONE")
        return

    rep = {}
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT name, type, sql FROM sqlite_master "
            "WHERE type in ('table','index') AND sql NOT NULL"
        )
        schema_rows = [dict(r) for r in cur.fetchall()]
        rep["schema"] = schema_rows

        # counts
        tables = [r["name"] for r in schema_rows if r["type"] == "table"]
        counts = {}
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                (n,) = cur.fetchone()
                counts[t] = n
            except Exception as e:
                counts[t] = f"err: {e}"
        rep["counts"] = counts

        rep["fx_batches_tail"] = _sqlite_tail(cur, "SELECT * FROM fx_batches ORDER BY id DESC LIMIT 10")
        rep["fx_rates_tail"] = _sqlite_tail(cur, "SELECT * FROM fx_rates ORDER BY date DESC LIMIT 10")
        rep["transactions_tail"] = _sqlite_tail(cur, "SELECT * FROM transactions ORDER BY ROWID DESC LIMIT 10")
        rep["transactions_stats"] = _sqlite_tail(cur, "SELECT COUNT(*) AS n, MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM transactions")

    except Exception as e:
        rep["db_error"] = f"{e}"
        rep["traceback"] = traceback.format_exc()
        try:
            db_preview_csv(conn, db_meta_dir)
            evolve_track_artifact(db_meta_dir, "preview/transactions_head.csv", "DB preview samples added")
            evolve_track_artifact(db_meta_dir, "preview/fx_rates_head.csv", "DB preview samples added")
        except Exception:
            pass
        # Alembic info
        try:
            alembic_info(get_project_root(Path(__file__)), db_meta_dir)
            evolve_track_artifact(db_meta_dir, "alembic_head.txt", "alembic state captured")
        except Exception:
            pass
    finally:
        with contextlib.suppress(Exception):
            conn.close()

    write_text_safe(db_meta_dir / "db_diag.json", json.dumps(rep, indent=2, default=str))

    # Missing tables
    write_text_safe(db_meta_dir / "expected_tables.txt", "\n".join(expected_tables))
    present = {r["name"] for r in rep.get("schema", []) if r.get("type") == "table"}
    missing = [t for t in expected_tables if t not in present]
    if missing:
        write_text_safe(db_meta_dir / "missing_tables.txt", "Missing tables:\n" + "\n".join(missing))
    else:
        write_text_safe(db_meta_dir / "missing_tables.txt", "OK: All expected tables are present.")
    st.write("DB_DONE")


def http_get(url: str, timeout: float = 2.0) -> Tuple[int, str]:
    import urllib.request
    import urllib.error
    import socket

    # hard per-call timeout
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            data = resp.read()
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = str(data)
        return code, text
    except (urllib.error.HTTPError) as e:
        return e.code, f"HTTPError: {e}"
    except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
        return 0, f"TIMEOUT/URLError: {e}"
    except BaseException as e:
        # catch KeyboardInterrupt or anything else and turn it into a non-fatal note
        return 0, f"ERROR:{e.__class__.__name__}: {e}"


def api_diag(api_base: str, api_dir: Path, st: StateLog) -> None:
    """Query /health and /version unless running inside API request."""
    st.write("API_BEGIN")

    # If we are running from the FastAPI endpoint, skip to avoid self-call deadlocks/hangs.
    if os.getenv("RUN_CONTEXT") == "api":
        write_text_safe(api_dir / "api_diag_skipped.txt",
                        "Skipped API checks in API context (RUN_CONTEXT=api).")
        st.write("API_SKIPPED_APICTX")
        st.write("API_DONE")
        return

    for ep in API_ENDPOINTS:
        url = api_base.rstrip("/") + ep
        code, text = http_get(url, timeout=2.0)  # short timeout
        base = ("GET" + ep.replace("/", "_")).strip("_")
        if 200 <= code < 400:
            write_text_safe(api_dir / f"{base}.json", text)
            write_text_safe(api_dir / f"{base}.status.txt", f"HTTP {code}")
        else:
            write_text_safe(api_dir / f"{base}.error.txt", text or "ERROR")

    st.write("API_DONE")


def tail_file_to(src: Path, dest: Path, tail_lines: int) -> None:
    """
    Tail a text-ish file safely with time/size caps. Never throws.
    - Skips binary-looking files.
    - Limits output bytes so we never create huge artifacts.
    - Enforces a per-file time budget across read+write.
    """
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return

    start = time.monotonic()

    # quick binary sniff (first 4 KB)
    try:
        with src.open("rb") as f:
            head = f.read(4096)
        if b"\x00" in head:
            # looks binary -> just copy nothing, but leave a stub marker
            dest.write_text("# skipped: binary-like content\n", encoding="utf-8")
            return
    except Exception:
        # if we can't even read the head, record minimal error and move on
        with contextlib.suppress(Exception):
            dest.write_text("# error: cannot read file head\n", encoding="utf-8")
        return

    # if tail_lines <= 0, we still don't want to copy entire files: cap by size/time
    max_bytes = LOG_TAIL_MAX_BYTES

    try:
        # Try fast byte-tail with caps
        with src.open("rb") as f:
            f.seek(0, io.SEEK_END)
            size = f.tell()
            block = 4096
            data = bytearray()
            nl = 0
            pos = size
            # If tail_lines is not positive, still keep a reasonable byte tail
            target_lines = max(1, tail_lines) if tail_lines > 0 else 10000  # high line cap so bytes cap dominates

            while pos > 0 and nl <= target_lines and len(data) < max_bytes:
                # time budget check (read side)
                if (time.monotonic() - start) > LOG_PER_FILE_TIME_BUDGET_SEC:
                    break
                read = block if pos >= block else pos
                pos -= read
                f.seek(pos, io.SEEK_SET)
                chunk = f.read(read)
                data[0:0] = chunk
                nl = data.count(b"\n")
                if len(data) >= max_bytes:
                    break

            # Reduce to last N lines (if requested), then bytes-cap again
            if tail_lines > 0:
                lines = data.splitlines()
                data = b"\n".join(lines[-tail_lines:])

            if len(data) > max_bytes:
                data = data[-max_bytes:]

        # time budget check before writing
        if (time.monotonic() - start) > LOG_PER_FILE_TIME_BUDGET_SEC:
            with contextlib.suppress(Exception):
                dest.write_text("# truncated: read time budget exceeded\n", encoding="utf-8")
            return

        # Write out; if writing is slow, chunk it
        try:
            with dest.open("wb") as w:
                view = memoryview(data)
                offset = 0
                chunk = 64 * 1024
                while offset < len(view):
                    if (time.monotonic() - start) > LOG_PER_FILE_TIME_BUDGET_SEC:
                        w.write(b"\n# truncated: write time budget exceeded\n")
                        break
                    w.write(view[offset:offset+chunk])
                    offset += chunk
        except Exception:
            with contextlib.suppress(Exception):
                dest.write_text("# error: write failed\n", encoding="utf-8")

    except Exception:
        with contextlib.suppress(Exception):
            dest.write_text("# error: tail failed\n", encoding="utf-8")
        return


def collect_logs(proj_root: Path, logs_out_dir: Path, tail_lines: int, copy_errors: Path, st: StateLog) -> None:
    st.write("LOGS_BEGIN")
    processed = 0
    roots = [proj_root / "automation" / "logs", proj_root / "logs"]
    for ld in roots:
        if not ld.exists():
            continue
        for root, dirnames, filenames in os.walk(ld):
            # prune noisy subtrees
            dirnames[:] = [d for d in dirnames if d not in LOG_SKIP_DIRNAMES]
            for fname in filenames:
                if processed >= LOG_MAX_FILES:
                    append_text_safe(copy_errors, f"Logs capped at {LOG_MAX_FILES} files\n")
                    st.write("LOGS_LIMIT_REACHED")
                    st.write("LOGS_DONE")
                    return

                src = Path(root) / fname
                ext = src.suffix.lower()
                if ext in LOG_SKIP_EXTS:
                    processed += 1
                    # mirror the path with a tiny marker so we know it existed
                    rel = src.relative_to(ld)
                    dest = logs_out_dir / rel
                    with contextlib.suppress(Exception):
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        dest.write_text(f"# skipped by extension {ext}\n", encoding="utf-8")
                    continue

                rel = src.relative_to(ld)
                dest = logs_out_dir / rel

                try:
                    tail_file_to(src, dest, tail_lines)
                except Exception as e:
                    append_text_safe(copy_errors, f"Failed to tail log: {src}\n{e}\n---\n")
                finally:
                    processed += 1
    st.write("LOGS_DONE")


def inventory(bundle_dir: Path, meta_dir: Path, st: StateLog) -> None:
    """Write a non-blocking inventory CSV.
       - In API context (RUN_CONTEXT=api): no hashing, cap file count.
       - In CLI context: bounded hashing per file (uses existing limits)."""
    st.write("INV_BEGIN")
    inv = meta_dir / "inventory.csv"
    inv.parent.mkdir(parents=True, exist_ok=True)

    run_ctx = os.getenv("RUN_CONTEXT")
    api_mode = (run_ctx == "api")
    max_files = INV_API_MAX_FILES if api_mode else INV_MAX_FILES
    do_hash = not api_mode  # disable hashing for API-fast mode

    count = 0
    with inv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Path", "Length", "LastWriteTime", "SHA256_or_Note"])

        for root, dirnames, filenames in os.walk(bundle_dir):
            # prune noisy subtrees
            dirnames[:] = [d for d in dirnames if d not in INV_SKIP_DIRNAMES]

            for fname in filenames:
                p = Path(root) / fname
                # guard: limit total files
                if count >= max_files:
                    w.writerow(["# LIMIT", "", "", f"Stopped after {max_files} files (ctx={'api' if api_mode else 'cli'})"])
                    st.write("INV_LIMIT_REACHED")
                    st.write("INV_DONE")
                    return

                count += 1

                # stat metadata (fast)
                try:
                    st_stat = p.stat()
                    size = st_stat.st_size
                    mtime_iso = dt.datetime.fromtimestamp(st_stat.st_mtime).isoformat(timespec="seconds")
                except BaseException as e:  # include KeyboardInterrupt safety
                    w.writerow([str(p), "", "", f"ERR:stat:{e}"])
                    continue

                # quick skips and API-fast: never hash large or problematic files
                ext = p.suffix.lower()
                if ext in INV_SKIP_EXTS:
                    w.writerow([str(p), size, mtime_iso, f"SKIPPED:ext:{ext}"])
                    continue

                if not do_hash:
                    # API-fast mode: don’t open/read the file at all
                    w.writerow([str(p), size, mtime_iso, "SKIPPED:api_no_hash"])
                    continue

                if size > INV_HASH_MAX_BYTES:
                    w.writerow([str(p), size, mtime_iso, f"SKIPPED:size>{INV_HASH_MAX_BYTES}"])
                    continue

                # bounded hashing with time budget (CLI mode only)
                sha = hashlib.sha256()
                start = time.monotonic()
                timed_out = False
                try:
                    with p.open("rb") as r:
                        for chunk in iter(lambda: r.read(8192), b""):
                            sha.update(chunk)
                            if (time.monotonic() - start) > INV_HASH_TIME_BUDGET_SEC:
                                timed_out = True
                                break
                    if timed_out:
                        w.writerow([str(p), size, mtime_iso, f"TIMEOUT>{INV_HASH_TIME_BUDGET_SEC}s"])
                    else:
                        w.writerow([str(p), size, mtime_iso, sha.hexdigest()])
                except BaseException as e:  # include KeyboardInterrupt safety
                    w.writerow([str(p), size, mtime_iso, f"ERR:read:{e}"])

    st.write("INV_DONE")


def build_manifest(proj_root: Path, bundle_dir: Path, zip_path: Path, api_base: str, tail_lines: int, expected_tables: List[str], meta_dir: Path, db_hint: Optional[str]) -> None:
    manifest = {
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "project_root": str(proj_root),
        "bundle_dir": str(bundle_dir),
        "zip_target": str(zip_path),
        "api_base": api_base,
        "tail_lines": tail_lines,
        "python_executable": sys.executable,
        "python_version": sys.version,
        "expected_tables": expected_tables,
        "db_hint": db_hint,
    }
    write_text_safe(bundle_dir / "manifest.json", json.dumps(manifest, indent=2))

    dbg = {
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "cwd": str(Path.cwd()),
        "user": os.getenv("USERNAME") or os.getenv("USER"),
        "proj_root": str(proj_root),
        "bundle_dir": str(bundle_dir),
        "zip_target": str(zip_path),
        "api_base": api_base,
        "tail_lines": tail_lines,
        "env_sample": {k: os.getenv(k) for k in ["USERNAME", "USER", "PATH", "VIRTUAL_ENV"]},
    }
    write_text_safe(meta_dir / "debug_snapshot.json", json.dumps(dbg, indent=2))


def prezip_summary(bundle_dir: Path, zip_path: Path, meta_dir: Path) -> None:
    count = sum(1 for _ in bundle_dir.rglob("*") if _.is_file())
    lines = [
        "=== Build Summary (pre-zip) ===",
        f"Time: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"BundleDir: {bundle_dir}",
        f"ZipTarget: {zip_path}",
        f"Pre-zip file count: {count}",
    ]
    lines.append(f"Inventory caps: max_files={INV_MAX_FILES}, hash_max_bytes={INV_HASH_MAX_BYTES}, per_file_time_budget={INV_HASH_TIME_BUDGET_SEC}s")
    write_text_safe(meta_dir / "build_summary.txt", "\n".join(lines))


def zip_bundle(source_dir: Path, zip_path: Path) -> bool:
    """
    Create a zip of source_dir contents with API-fast safeguards:
    - In RUN_CONTEXT=api: ZIP_STORED (no compression), skip huge/binary-ish files.
    - Never raises; on failure, produces a minimal zip with _meta and manifest.
    """
    run_ctx = os.getenv("RUN_CONTEXT")
    api_mode = (run_ctx == "api")

    # Choose compression
    compression = zipfile.ZIP_STORED if api_mode else zipfile.ZIP_DEFLATED
    compresslevel = None if compression == zipfile.ZIP_STORED else 6

    # Ensure parent exists & remove stale zip
    with contextlib.suppress(Exception):
        zip_path.parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(Exception):
        if zip_path.exists():
            zip_path.unlink()

    files_added = 0

    try:
        # Python 3.11+ supports compresslevel in ZipFile; on older, it's ignored
        with zipfile.ZipFile(zip_path, "w", compression=compression, compresslevel=compresslevel) as zf:
            for root, _, filenames in os.walk(source_dir):
                for fname in filenames:
                    full = Path(root) / fname
                    arcname = str(full.relative_to(source_dir))

                    # Safety cap on total entries
                    if files_added >= ZIP_MAX_FILES:
                        # add a small note file to explain truncation
                        note = f"# zip limit reached at {ZIP_MAX_FILES} files\n"
                        zf.writestr("_meta/zip_truncated.txt", note)
                        if (source_dir / "_meta" / "zip_truncated.txt").exists():
                            evolve_track_artifact(source_dir / "_meta", "zip_truncated.txt", "zip truncated due to limits")
                        return zip_path.exists() and zip_path.stat().st_size > 0

                    # Skip the zip file itself if source_dir == zip_path.parent
                    if full.resolve() == zip_path.resolve():
                        continue

                    # In API mode, skip big/compressed-like files
                    if api_mode:
                        try:
                            size = full.stat().st_size
                        except Exception:
                            size = 0
                        ext = full.suffix.lower()
                        if ext in ZIP_SKIP_EXTS or size > ZIP_API_MAX_FILE_BYTES:
                            # include a tiny placeholder so we know it existed
                            zf.writestr(f"_meta/skipped/{arcname}.txt",
                                        f"skipped in api-mode (ext={ext}, size={size})")
                            files_added += 1
                            continue

                    try:
                        # Let zipfile read & (optionally) compress; STORED is fast
                        zf.write(full, arcname)
                        files_added += 1
                    except BaseException as e:
                        # Do not abort: record error in zip and continue
                        zf.writestr(f"_meta/zip_write_errors/{arcname}.txt",
                                    f"{e.__class__.__name__}: {e}")
                        we = source_dir / "_meta" / "zip_write_errors"
                        if we.exists():
                            evolve_track_artifact(source_dir / "_meta", "zip_write_errors/", "zip write errors captured")
                        files_added += 1

        return zip_path.exists() and zip_path.stat().st_size > 0

    except BaseException as e:
        # Fallback: create a minimal zip containing _meta and manifest so callers have artifacts
        try:
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
                meta = source_dir / "_meta"
                if meta.exists():
                    for p in meta.rglob("*"):
                        if p.is_file():
                            zf.write(p, str(p.relative_to(source_dir)))
                manifest = source_dir / "manifest.json"
                if manifest.exists():
                    zf.write(manifest, "manifest.json")
                # include the error explanation
                zf.writestr("_meta/zip_error.txt",
                            f"Fallback minimal zip; original error:\n{e.__class__.__name__}: {e}\n")
        except Exception:
            pass
        return zip_path.exists() and zip_path.stat().st_size > 0


def retain_latest_zips(bundle_root: Path, keep: int) -> None:
    zips = sorted(bundle_root.glob("support_bundle_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in zips[keep:]:
        with contextlib.suppress(Exception):
            old.unlink()


def write_runtime_snapshot(proj_root: Path, meta_dir: Path, api_base: str, tail_lines: int) -> None:
    cfg = {
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "cwd": str(Path.cwd()),
        "proj_root": str(proj_root),
        "python": {"exe": sys.executable, "version": sys.version, "platform": sys.platform},
        "uvicorn": {
            "host": os.getenv("UVICORN_HOST"),
            "port": os.getenv("UVICORN_PORT"),
            "workers": os.getenv("UVICORN_WORKERS"),
            "http": os.getenv("UVICORN_HTTP"),
            "loop": os.getenv("UVICORN_LOOP"),
            "log_level": os.getenv("UVICORN_LOG_LEVEL"),
        },
        "env_flags": {
            "RUN_CONTEXT": os.getenv("RUN_CONTEXT"),
            "API_BASE": api_base,
            "BUNDLE_TOKEN_present": "BUNDLE_TOKEN" in os.environ,
        },
        "bundle_params": {
            "tail_lines": tail_lines,
        },
    }
    write_text_safe(meta_dir / "runtime.json", json.dumps(cfg, indent=2))


def write_git_fingerprints(proj_root: Path, meta_dir: Path) -> None:
    """Write a minimal git fingerprint if .git exists. Non-fatal."""
    git_dir = proj_root / ".git"
    if not git_dir.exists():
        write_text_safe(meta_dir / "git_status.txt", "No .git directory present.")
        return
    try:
        import subprocess
        def _run(cmd: list[str]) -> str:
            try:
                out = subprocess.run(cmd, cwd=str(proj_root), capture_output=True, text=True,
                                     encoding="utf-8", errors="ignore", timeout=5)
                return out.stdout.strip() or out.stderr.strip()
            except Exception as e:
                return f"ERR: {e}"
        write_text_safe(meta_dir / "git_status.txt", _run(["git", "status", "--porcelain=v1", "--branch"]))
        write_text_safe(meta_dir / "git_log.txt", _run(["git", "log", "-n", "20", "--pretty=oneline", "--decorate"]))
        write_text_safe(meta_dir / "git_remotes.txt", _run(["git", "remote", "-v"]))
        # include uncommitted changes as a small patch (won’t be huge)
        patch = _run(["git", "diff"])
        if patch:
            write_text_safe(meta_dir / "git_diff.patch", patch)
    except Exception as e:
        write_text_safe(meta_dir / "git_error.txt", f"{e}\n{traceback.format_exc()}")


def write_source_filelist(proj_root: Path, bundle_dir: Path) -> None:
    """List key source trees so we can confirm files exist."""
    targets = [
        proj_root / "src" / "cryptotaxcalc",
        proj_root / "rules",
        proj_root / "automation",
        proj_root / "tests",
    ]
    out = []
    for root in targets:
        if not root.exists():
            out.append(f"[missing] {root}")
            continue
        for p in root.rglob("*"):
            if p.is_file():
                out.append(str(p.relative_to(proj_root)))
    write_text_safe(bundle_dir / "_meta" / "filelist.txt", "\n".join(sorted(out)))


def write_repro_seed(meta_dir: Path, api_base: str, tail_lines: int, keep_zips: int, argv: list[str]) -> None:
    seed = {
        "trigger": "collect_support_bundle.py",
        "argv": argv,
        "api_base": api_base,
        "tail_lines": tail_lines,
        "keep_zips": keep_zips,
        "run_context": os.getenv("RUN_CONTEXT"),
    }
    write_text_safe(meta_dir / "repro.json", json.dumps(seed, indent=2))


def db_preview_csv(conn: sqlite3.Connection, db_meta_dir: Path) -> None:
    """Write small, safe CSV previews of key tables (first 50 rows)."""
    try:
        cur = conn.cursor()
        previews = {
            "transactions_head.csv": "SELECT * FROM transactions ORDER BY ROWID ASC LIMIT 50",
            "fx_rates_head.csv": "SELECT * FROM fx_rates ORDER BY date ASC LIMIT 50",
        }
        for name, query in previews.items():
            try:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                # mask potentially sensitive columns
                mask_cols = {"address", "wallet", "tx_hash", "note"}
                with (db_meta_dir / "preview" / name).open("w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(cols)
                    for r in rows:
                        out = []
                        for c, v in zip(cols, r):
                            if c in mask_cols and v is not None:
                                sv = str(v)
                                out.append(sv[:4] + "…" + sv[-4:] if len(sv) > 10 else "***")
                            else:
                                out.append(v)
                        w.writerow(out)
            except Exception as e:
                write_text_safe(db_meta_dir / f"preview_{name}.error.txt", f"{e}")
    except Exception:
        pass


def alembic_info(proj_root: Path, db_meta_dir: Path) -> None:
    """Capture Alembic head/history if env is present, non-fatal."""
    alembic_dir = proj_root / "alembic"
    if not alembic_dir.exists():
        write_text_safe(db_meta_dir / "alembic.txt", "No alembic/ directory.")
        return
    try:
        import subprocess
        def _run(args: list[str]) -> str:
            try:
                out = subprocess.run(
                    ["alembic"] + args, cwd=str(proj_root),
                    capture_output=True, text=True, encoding="utf-8", errors="ignore", timeout=5
                )
                return out.stdout.strip() or out.stderr.strip()
            except Exception as e:
                return f"ERR: {e}"
        write_text_safe(db_meta_dir / "alembic_head.txt", _run(["heads"]))
        write_text_safe(db_meta_dir / "alembic_history.txt", _run(["history", "--verbose", "-n", "10"]))
    except Exception as e:
        write_text_safe(db_meta_dir / "alembic_error.txt", f"{e}\n{traceback.format_exc()}")


def rules_snapshot(proj_root: Path, bundle_dir: Path) -> None:
    """
    Snapshot which rule modules exist and their hashes.
    We don’t import them (avoids side effects); we checksum file contents.
    """
    rules_dir = proj_root / "rules"
    out = []
    if rules_dir.exists():
        for p in sorted(rules_dir.rglob("*.py")):
            try:
                h = hashlib.sha256(p.read_bytes()).hexdigest()
            except Exception:
                h = "ERR"
            out.append({"path": str(p.relative_to(proj_root)), "sha256": h})

    # Write file and track it in EVOLVE_RULE.json under _meta
    write_text_safe(bundle_dir / "_rules" / "active_rules.json", json.dumps(out, indent=2))
    evolve_track_artifact(bundle_dir / "_meta", "_rules/active_rules.json", "rule hashes snapshot")


def write_last_exception(meta_dir: Path, e: BaseException) -> None:
    write_text_safe(meta_dir / "last_exception.txt", f"{e}\n{traceback.format_exc()}")


# ---------- AI HELPER ----------

def write_evolve_rule(meta_dir: Path) -> None:
    """Ensure EVOLVE_RULE.json exists and is normalized; no version bump here."""
    st = _evolve_load(meta_dir)
    _evolve_save(meta_dir, st)


def write_auto_evolve_md(bundle_dir: Path, proj_root: Path, meta_dir: Path) -> None:
    """Copy master AUTO_EVOLVE.md into bundle + _meta; track as evolvable artifact."""
    src = proj_root / "automation" / "AUTO_EVOLVE.md"
    if src.exists():
        try:
            text = src.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = None
    else:
        text = None

    if not text:
        text = (
            "# AUTO_EVOLVE.md — Fallback Copy\n"
            "Original not found in automation/. This is an auto-generated placeholder.\n"
        )

    write_text_safe(bundle_dir / "AUTO_EVOLVE.md", text)
    write_text_safe(bundle_dir / "_meta" / "AUTO_EVOLVE.md", text)

    # record once
    evolve_track_artifact(meta_dir, "AUTO_EVOLVE.md", "policy document embedded into bundles")


# ---------- EVOLUTION STATE / CHANGELOG HELPERS ----------

def _evolve_load(meta_dir: Path) -> dict:
    f = meta_dir / "EVOLVE_RULE.json"
    if f.exists():
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "policy": "auto-evolve",
        "description": (
            "Whenever a new failure class is detected (phase error, states stall, or missing artifact), "
            "extend the bundle with a new small artifact that makes the root cause obvious next time."
        ),
        "version": 1,
        "artifacts": [],   # list of strings (relative paths or logical names)
        "history": []      # list of {ts, artifact, note, version}
    }


def _evolve_save(meta_dir: Path, state: dict) -> None:
    state["last_updated"] = dt.datetime.now().isoformat(timespec="seconds")
    write_text_safe(meta_dir / "EVOLVE_RULE.json", json.dumps(state, indent=2))


def evolve_track_artifact(meta_dir: Path, artifact_relpath: str, note: str = "") -> None:
    """
    If this artifact hasn't been recorded before, record it, bump version,
    and append a line to evolve_changelog.txt. Idempotent across runs.
    """
    st = _evolve_load(meta_dir)
    artifacts = set(st.get("artifacts", []))
    if artifact_relpath not in artifacts:
        artifacts.add(artifact_relpath)
        st["artifacts"] = sorted(artifacts)
        st["version"] = int(st.get("version", 1)) + 1
        entry = {
            "ts": dt.datetime.now().isoformat(timespec="seconds"),
            "artifact": artifact_relpath,
            "note": note or "added",
            "version": st["version"],
        }
        hist = st.get("history", [])
        hist.append(entry)
        st["history"] = hist
        _evolve_save(meta_dir, st)
        # human-readable line
        line = f"[v{entry['version']}] {entry['ts']}  {artifact_relpath} — {entry['note']}\n"
        append_text_safe(meta_dir / "evolve_changelog.txt", line)
    else:
        # ensure EVOLVE_RULE.json has the current format even if no bump
        if "history" not in st:
            st["history"] = []
        if "artifacts" not in st:
            st["artifacts"] = sorted(artifacts)
        _evolve_save(meta_dir, st)
        

def main() -> int:
    parser = argparse.ArgumentParser(description="Collect support bundle (Python)")
    parser.add_argument("--api-base", dest="api_base", default=os.getenv("API_BASE", "http://127.0.0.1:8000"))
    parser.add_argument("--tail-lines", dest="tail_lines", type=int, default=400)
    parser.add_argument("--keep-zips", dest="keep_zips", type=int, default=5)
    parser.add_argument(
        "--include-data",
        dest="include_data",
        action="store_true",
        help="Include raw storage/backups (may contain sensitive financial data).",
    )
    parser.add_argument(
        "--include-raw-logs",
        dest="include_raw_logs",
        action="store_true",
        help="Include full log directories (may contain sensitive data).",
    )
    parser.add_argument("--expected", nargs="*", default=EXPECTED_TABLES_DEFAULT, help="Expected DB tables (override)")
    args = parser.parse_args()

    script_path = Path(__file__).resolve()
    proj_root = get_project_root(script_path)
    bundle_root = proj_root / "support_bundles"
    stamp = now_stamp()
    bundle_dir = bundle_root / f"bundle_{stamp}"
    meta_dir = bundle_dir / "_meta"
    db_meta_dir = bundle_dir / "_db"
    api_dir = bundle_dir / "_api"
    logs_out_dir = bundle_dir / "_logs"
    copy_errors = meta_dir / "copy_errors.txt"
    zip_path = bundle_root / f"support_bundle_{stamp}.zip"

    # Prepare dirs
    for d in (bundle_root, bundle_dir, meta_dir, db_meta_dir, api_dir, logs_out_dir):
        d.mkdir(parents=True, exist_ok=True)

    st = StateLog(meta_dir)
    st.write("START")
    st.write("DIRS_READY")
    
    # Embed most recent smoke output (if present)
    try:
        logs_dir = (Path(__file__).resolve().parent / "logs")
        if (logs_dir / "smoke_test_output.log").exists():
            shutil.copy2(logs_dir / "smoke_test_output.log", meta_dir / "smoke_test_output.log")
            evolve_track_artifact(meta_dir, "smoke_test_output.log", "embedded full smoke stdout/stderr")
    except Exception:
        pass

    try:
        preflight(proj_root, bundle_dir, zip_path, meta_dir, args.api_base, args.tail_lines, args.include_data, args.include_raw_logs)
        st.write("PREFLIGHT_DONE")
        
        write_evolve_rule(meta_dir)
        write_runtime_snapshot(proj_root, meta_dir, args.api_base, args.tail_lines)
        evolve_track_artifact(meta_dir, "_meta/runtime.json", "runtime snapshot added")
        write_git_fingerprints(proj_root, meta_dir)
        evolve_track_artifact(meta_dir, "_meta/git_status.txt", "git fingerprints added")
        write_source_filelist(proj_root, bundle_dir)
        evolve_track_artifact(meta_dir, "_meta/filelist.txt", "source tree fingerprint added")
        write_repro_seed(meta_dir, args.api_base, args.tail_lines, args.keep_zips, sys.argv)
        evolve_track_artifact(meta_dir, "_meta/repro.json", "reproduction seed recorded")
        write_auto_evolve_md(bundle_dir, proj_root, meta_dir)

        # Collect project files
        safe_copy_tree(proj_root, bundle_dir, copy_errors, st, include_data=args.include_data, include_raw_logs=args.include_raw_logs)
        st.write("AFTER_COPY")

        # Python diagnostics (non-fatal)
        python_diag(meta_dir, st)

        # DB diagnostics + missing tables
        db_hint_path = db_meta_dir / "db_hint.json"
        db_diag(proj_root, db_meta_dir, args.expected, st)
        db_hint = None
        if db_hint_path.exists():
            with contextlib.suppress(Exception), db_hint_path.open("r", encoding="utf-8") as f:
                db_hint = json.load(f).get("db_path")

        # API health/version
        try:
            api_diag(args.api_base, api_dir, st)
        except BaseException as e:
            write_text_safe(api_dir / "api_diag_fatal.txt", f"{e}\n{traceback.format_exc()}")
            st.write("API_FAIL_CONTINUE")

        # Logs tail + inventory
        collect_logs(proj_root, logs_out_dir, args.tail_lines, copy_errors, st)
        inventory(bundle_dir, meta_dir, st)

        # Manifest + debug snapshot
        build_manifest(proj_root, bundle_dir, zip_path, args.api_base, args.tail_lines, args.expected, meta_dir, db_hint)

        # Pre-zip summary
        prezip_summary(bundle_dir, zip_path, meta_dir)

        # Zip
        ok = zip_bundle(bundle_dir, zip_path)
        if ok:
            # Clean temp dir only after successful zip
            with contextlib.suppress(Exception):
                shutil.rmtree(bundle_dir)
            retain_latest_zips(bundle_root, args.keep_zips)
            # IMPORTANT: print marker to STDOUT for the FastAPI endpoint
            abs_zip = str(zip_path.resolve())
            print(f"::zip:: {abs_zip}", flush=True)
            return 0
        else:
            print(f"[bundle] zip failed; see {meta_dir / 'zip_error.txt'}", file=sys.stderr, flush=True)
            # Still print the path we tried to create
            print(f"::zip:: {str(zip_path.resolve())}", flush=True)
            return 2

    except Exception as e:
        # Fatal path: write error & try to make an emergency zip with whatever we have
        write_text_safe(meta_dir / "fatal_error.txt", f"{e}\n{traceback.format_exc()}")
        try:
            ok = zip_bundle(bundle_dir, zip_path)
            # do not delete temp on fatal
        except Exception:
            ok = False
        # still emit marker for the API
        print(f"::zip:: {str(zip_path.resolve())}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# =========================================================
#  CryptoTaxCalc – Logging and Diagnostics (Optimized)
# =========================================================
#  • Dual log stream: plaintext + JSON per event
#  • Self-healing log folders
#  • Atomic writes for integrity
#  • Compatible with existing get_logger(), setup_logging(), etc.
# =========================================================

# ----------------------------
# Configuration / paths
# ----------------------------

def get_logs_root() -> Path:
    """Determine and ensure the logs root directory."""
    env = os.getenv("CRYPTOTAXCALC_LOGS_DIR")
    root = Path(env) if env else Path.cwd() / "logs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def get_component_dir(component: str) -> Path:
    """Create a subfolder for a logical component (e.g., 'fx', 'calc')."""
    safe = component.strip().replace(os.sep, "_") or "app"
    p = get_logs_root() / safe
    p.mkdir(parents=True, exist_ok=True)
    return p


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """Atomically write a small JSON file (write to .tmp then replace)."""
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        # fallback to safe write
        sys.stderr.write(f"[WARN] Failed atomic write to {path}: {e}\n")


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# ----------------------------
# Logger factory
# ----------------------------

def get_logger(component: str = "app", level: int = logging.INFO) -> logging.Logger:
    """
    Return a configured logger bound to this component.
    Adds dual output (console + file) and writes JSON alongside human logs.
    """
    logger_name = f"cryptotaxcalc.{component}"
    logger = logging.getLogger(logger_name)
    if getattr(logger, "_ctc_configured", False):
        return logger

    comp_dir = get_component_dir(component)
    text_log = comp_dir / "events.log"
    json_log = comp_dir / "events.jsonl"

    # Human-readable formatter
    fmt = logging.Formatter(
        fmt="%(asctime)sZ [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Text file handler
    fh = logging.FileHandler(text_log, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler(stream=sys.stderr)
    ch.setLevel(level)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    # JSON line handler (append mode)
    class JSONHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                payload = {
                    "timestamp": _now_iso_z(),
                    "level": record.levelname,
                    "component": component,
                    "message": record.getMessage(),
                    "logger": record.name,
                }
                (json_log.parent).mkdir(parents=True, exist_ok=True)
                with open(json_log, "a", encoding="utf-8") as f:
                    f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            except Exception:
                sys.stderr.write("Failed to write JSON log line.\n")

    jh = JSONHandler(level=level)
    logger.addHandler(jh)

    logger.setLevel(level)
    logger.propagate = False
    logger._ctc_configured = True  # type: ignore[attr-defined]
    return logger


# ----------------------------
# Exception and health helpers
# ----------------------------

def log_exception_and_record_latest(
    component: str,
    exc: BaseException,
    *,
    context: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
) -> None:
    """Log an exception and write latest_error.json."""
    logger = get_logger(component)
    msg = message or f"{exc.__class__.__name__}: {exc}"
    logger.exception(msg)

    comp_dir = get_component_dir(component)
    latest_error = comp_dir / "latest_error.json"
    payload = {
        "timestamp": _now_iso_z(),
        "component": component,
        "message": msg,
        "exception_type": type(exc).__name__,
        "stacktrace": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        "context": context or {},
    }
    _atomic_write_json(latest_error, payload)


def log_success_and_clear_latest(
    component: str,
    message: str = "healthy",
    *,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log a success line and record health_ok.json."""
    logger = get_logger(component)
    logger.info(message)

    comp_dir = get_component_dir(component)
    latest_error = comp_dir / "latest_error.json"
    health_ok = comp_dir / "health_ok.json"
    try:
        if latest_error.exists():
            latest_error.unlink()
    except Exception as e:
        logger.warning("Could not remove latest_error.json: %s", e)

    payload = {
        "timestamp": _now_iso_z(),
        "component": component,
        "status": "ok",
        "message": message,
        "context": context or {},
    }
    _atomic_write_json(health_ok, payload)


def log_error_message(component: str, message: str, *, context: Optional[Dict[str, Any]] = None) -> None:
    """Record a non-exception error condition and write latest_error.json."""
    logger = get_logger(component)
    logger.warning(message)
    comp_dir = get_component_dir(component)
    latest_error = comp_dir / "latest_error.json"
    payload = {
        "timestamp": _now_iso_z(),
        "component": component,
        "message": message,
        "exception_type": None,
        "stacktrace": None,
        "context": context or {},
    }
    _atomic_write_json(latest_error, payload)


# ----------------------------
# Root setup and integration
# ----------------------------

def setup_logging(*, enable_console: bool = True, console_level: int = logging.INFO) -> None:
    """Initialize root logging; idempotent."""
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(console_level if enable_console else logging.WARNING)
    if enable_console:
        ch = logging.StreamHandler(stream=sys.stderr)
        ch.setLevel(console_level)
        ch.setFormatter(logging.Formatter(
            fmt="%(asctime)sZ [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))
        root.addHandler(ch)

    # Boot log record for diagnostics
    opt_log = get_logs_root() / "opt_pass.log"
    opt_log.write_text(f"{_now_iso_z()} INFO startup: Logging initialized\n", encoding="utf-8")


def integrate_uvicorn_logs(level: int = logging.INFO) -> None:
    """Route uvicorn logs through our handlers."""
    app_logger = get_logger("server", level=level)
    handlers = app_logger.handlers
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.handlers = handlers[:]
        lg.propagate = False

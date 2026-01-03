# audit_utils.py – Optimized and Auditable
from __future__ import annotations

import json
from sqlalchemy import text
from datetime import datetime, timezone
from pathlib import Path

from cryptotaxcalc.db import engine
from cryptotaxcalc.logging_setup import (
    get_logger,
    _atomic_write_json,
    _now_iso_z,
)

logger = get_logger("audit")

# ======================================================
#  Schema and setup
# ======================================================

def _ensure_audit_log_table(conn) -> None:
    """
    Ensure audit_log table exists and has required columns.
    Safe and idempotent; runs automatically before first insert.
    """
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor TEXT,
            action TEXT,
            target_type TEXT,
            target_id INTEGER,
            details_json TEXT,
            ip TEXT,
            ts TEXT
        )
    """)

# ======================================================
#  Main audit entry
# ======================================================

def audit(
    actor: str,
    action: str,
    target_type: str | None,
    target_id: int | None,
    details: dict | None,
    ip: str | None = None,
) -> None:
    """
    Record an audit log entry.
    Adds a structured record to the DB and a JSON file under logs/audit/.
    Fully safe to call even if DB temporarily locked or table missing.
    """
    ts = _now_iso_z()
    payload = {
        "timestamp": ts,
        "actor": actor,
        "action": action,
        "target_type": target_type,
        "target_id": target_id,
        "details": details or {},
        "ip": ip,
    }

    try:
        with engine.begin() as conn:
            _ensure_audit_log_table(conn)
            conn.execute(
                text("""
                    INSERT INTO audit_log (actor, action, target_type, target_id, details_json, ip, ts)
                    VALUES (:a, :b, :c, :d, :e, :f, :g)
                """),
                dict(
                    a=actor,
                    b=action,
                    c=target_type,
                    d=target_id,
                    e=json.dumps(details or {}, ensure_ascii=False),
                    f=ip,
                    g=ts,
                ),
            )
        logger.info(f"AUDIT {actor} → {action} [{target_type}:{target_id}]")
    except Exception as e:
        logger.warning(f"Audit insert failed: {e}")
        payload["error"] = str(e)

    # Write diagnostics JSON
    try:
        out_dir = Path("logs/audit")
        out_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(out_dir / "last_event.json", payload)
    except Exception as e:
        logger.warning(f"Could not write audit diagnostics: {e}")

# ======================================================
#  Optional: retention helper
# ======================================================

def prune_old_audit_logs(limit: int = 10000) -> None:
    """
    Keep the audit_log table from growing indefinitely.
    Retains the newest `limit` rows.
    """
    try:
        with engine.begin() as conn:
            _ensure_audit_log_table(conn)
            row_count = conn.execute(text("SELECT COUNT(*) FROM audit_log")).scalar() or 0
            if row_count > limit:
                cutoff = row_count - limit
                conn.execute(
                    text("DELETE FROM audit_log WHERE id IN (SELECT id FROM audit_log ORDER BY id ASC LIMIT :n)"),
                    {"n": cutoff},
                )
                logger.info(f"Pruned {cutoff} old audit rows (kept {limit}).")
    except Exception as e:
        logger.warning(f"Audit log pruning failed: {e}")

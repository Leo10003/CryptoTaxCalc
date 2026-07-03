# db.py – Optimized and Safe
from __future__ import annotations

import sqlite3
import uuid
import os
import re
from datetime import datetime, date
from contextlib import contextmanager
from typing import Dict, Set, Iterator, Generator

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from .models import Base  # single declarative base for all ORM tables

# Logging integration
from cryptotaxcalc.logging_setup import (
    get_logger,
    log_exception_and_record_latest,
    log_success_and_clear_latest,
)

logger = get_logger("db")

# --------------------------------------------------
# SQLite adapters (Python 3.12 compatibility)
# --------------------------------------------------
# Python 3.12 deprecates sqlite3's *default* datetime adapter.
# Register explicit adapters so datetime/date values are bound deterministically.
try:
    sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))
    sqlite3.register_adapter(date, lambda d: d.isoformat())
except Exception:
    # Safe fallback: if registration fails for any reason, the app still runs.
    pass


# --------------------------------------------------
# Engine / Session configuration with safe PRAGMAs
# --------------------------------------------------
# Primary DB URL:
# - Production / real customers → set SQLALCHEMY_DATABASE_URL in env
# - Demo EXE → demo_launcher sets SQLITE_URL="sqlite:///demo/demo.sqlite"
# - Fallback → local cryptotaxcalc.db
DEFAULT_DB_URL = "sqlite:///./cryptotaxcalc.db"
SQLALCHEMY_DATABASE_URL = (
    os.getenv("SQLALCHEMY_DATABASE_URL")
    or os.getenv("SQLITE_URL")
    or DEFAULT_DB_URL
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False} if SQLALCHEMY_DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def _set_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """Apply consistent performance + safety PRAGMAs (idempotent)."""
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception as e:
        log_exception_and_record_latest("db", e, message="Failed to apply SQLite PRAGMAs")

@contextmanager
def sqlite_connection() -> Iterator[sqlite3.Connection]:
    """Direct connection context (for low-level checks)."""
    db_path = SQLALCHEMY_DATABASE_URL.replace("sqlite:///", "")
    conn = sqlite3.connect(db_path)
    _set_sqlite_pragmas(conn)
    try:
        yield conn
    finally:
        conn.close()

def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency that yields a Session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --------------------------------------------------
# Schema helpers (idempotent + additive)
# --------------------------------------------------

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _assert_sql_ident(name: str, what: str) -> str:
    """
    Validate a SQL identifier used in DDL (table/index/column).
    Prevents accidental injection if future refactors ever route user input here.
    """
    n = (name or "").strip()
    if not n or not _IDENT_RE.match(n):
        raise ValueError(f"Invalid SQL identifier for {what}: {name!r}")
    return n

def _ensure_index(conn, name: str, table: str, expr: str) -> None:
    name = _assert_sql_ident(name, "index")
    table = _assert_sql_ident(table, "table")
    hit = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='index' AND name=:n;"),
        {"n": name},
    ).fetchone()
    if not hit:
        conn.execute(text(f"CREATE INDEX {name} ON {table} {expr};"))

def _ensure_column(conn, table: str, name: str, decl: str) -> None:
    table = _assert_sql_ident(table, "table")
    name = _assert_sql_ident(name, "column")
    cols = {str(r[1]) for r in conn.execute(text(f"PRAGMA table_info({table});")).fetchall()}
    if name not in cols:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {decl};"))
        logger.info(f"Added column {table}.{name} {decl}")

def _ensure_table(conn, ddl_sql: str) -> None:
    conn.execute(text(ddl_sql))

# --------------------------------------------------
# Schema compatibility + auto-heal logic
# --------------------------------------------------

def _ensure_compatibility_objects(conn) -> None:
    """Ensure all tables and columns exist (idempotent)."""
    # fx_batches
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fx_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            created_at TEXT,
            imported_at TEXT,
            source TEXT,
            rates_hash TEXT
        );
    """))
    # Legacy DBs may not have these columns; add them additively.
    _ensure_column(conn, "fx_batches", "date", "TEXT")
    _ensure_column(conn, "fx_batches", "created_at", "TEXT")
    _ensure_index(conn, "ix_fx_batches_date", "fx_batches", "(date)")
    
    _ensure_index(conn, "ix_fx_batches_imported_at", "fx_batches", "(imported_at)")

    # fx_rates (legacy-compatible; canonical migration handled by db_migrations.ensure_fx_schema)
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            date TEXT,
            base TEXT,
            quote TEXT,
            rate TEXT,
            batch_id INTEGER
        );
    """))

    # If this DB was created with an older fx_rates shape, ensure required columns exist
    # so queries don’t fail before migrations complete.
    _ensure_column(conn, "fx_rates", "base", "TEXT")
    _ensure_column(conn, "fx_rates", "quote", "TEXT")
    _ensure_column(conn, "fx_rates", "rate", "TEXT")
    _ensure_column(conn, "fx_rates", "batch_id", "INTEGER")

    _ensure_index(conn, "idx_fx_rates_date", "fx_rates", "(date)")
    _ensure_index(conn, "idx_fx_rates_date_base_quote", "fx_rates", "(date, base, quote)")

    # calc_runs
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS calc_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            jurisdiction TEXT,
            rule_version TEXT,
            lot_method TEXT,
            fx_set_id INTEGER,
            params_json TEXT,
            run_id TEXT
        );
    """))
    _ensure_index(conn, "ix_calc_runs_started_at", "calc_runs", "(started_at)")
    _ensure_index(conn, "ux_calc_runs_run_id", "calc_runs", "(run_id)")

    # calc_audit
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS calc_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            created_at TEXT NOT NULL,
            actor TEXT,
            action TEXT,
            meta_json TEXT
        );
    """))
    _ensure_index(conn, "ix_calc_audit_run_id", "calc_audit", "(run_id)")

    # run_digests
    # Target schema (matches models.RunDigest):
    #   id INTEGER PK AUTOINCREMENT
    #   run_id INTEGER NOT NULL UNIQUE
    #   input_hash TEXT
    #   output_hash TEXT
    #   manifest_hash TEXT
    #   manifest_json TEXT
    #   created_at TEXT
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS run_digests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL UNIQUE,
            input_hash TEXT,
            output_hash TEXT,
            manifest_hash TEXT,
            manifest_json TEXT,
            created_at TEXT
        );
    """))

    # If an older deployment created run_digests without 'id' (run_id as PK),
    # rebuild it into the modern shape and copy the data across.
    cols = conn.execute(text("PRAGMA table_info('run_digests');")).fetchall()
    colnames = {str(c[1]) for c in cols}
    if "id" not in colnames:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS run_digests_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL UNIQUE,
                input_hash TEXT,
                output_hash TEXT,
                manifest_hash TEXT,
                manifest_json TEXT,
                created_at TEXT
            );
        """))

        # Copy over whatever columns exist; we expect at least run_id + hashes.
        existing = colnames
        copy_cols = [
            c for c in ["run_id", "input_hash", "output_hash", "manifest_hash", "manifest_json", "created_at"]
            if c in existing
        ]
        if copy_cols:
            col_list = ", ".join(copy_cols)
            conn.execute(text(f"""
                INSERT INTO run_digests_new ({col_list})
                SELECT {col_list} FROM run_digests
            """))

        conn.execute(text("DROP TABLE run_digests"))
        conn.execute(text("ALTER TABLE run_digests_new RENAME TO run_digests"))

def ensure_calc_runs_run_id(engine: Engine) -> None:
    """Guarantee unique run_id for all rows and enforce unique index."""
    try:
        with engine.begin() as conn:
            insp = inspect(conn)
            cols = [c["name"] for c in insp.get_columns("calc_runs")]
            if "run_id" not in cols:
                conn.execute(text("ALTER TABLE calc_runs ADD COLUMN run_id TEXT"))
                logger.info("Added missing column calc_runs.run_id")

            rows = conn.execute(text("SELECT id, run_id FROM calc_runs")).fetchall()
            used = set()
            for rid, run_id in rows:
                val = (run_id or "").strip()
                if not val:
                    new_val = f"legacy-{rid}"
                    conn.execute(text("UPDATE calc_runs SET run_id=:v WHERE id=:rid"), {"v": new_val, "rid": rid})
                    used.add(new_val)
                elif val in used:
                    new_val = f"{val}-{uuid.uuid4().hex[:6]}"
                    conn.execute(text("UPDATE calc_runs SET run_id=:v WHERE id=:rid"), {"v": new_val, "rid": rid})
                    used.add(new_val)
                else:
                    used.add(val)
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_calc_runs_run_id ON calc_runs(run_id)"))
    except Exception as e:
        log_exception_and_record_latest("db", e, message="Failed ensure_calc_runs_run_id")

# --------------------------------------------------
# Initialization entrypoint
# --------------------------------------------------

def init_db(engine: Engine) -> None:
    """
    Safe to run at every startup.
    Creates tables, enforces idempotent repairs, applies PRAGMAs, and logs health.
    """
    try:
        with engine.begin() as conn:
            _set_sqlite_pragmas(conn.connection)
            _ensure_compatibility_objects(conn)
            ensure_calc_runs_run_id(engine)
            
        # Canonical FX schema/migrations (single source of truth)
        from .db_migrations import ensure_fx_schema
        ensure_fx_schema(engine)

        Base.metadata.create_all(engine)

        # Performance indexes (Phase 2): pagination-friendly realized_events query
        # Query pattern: WHERE run_id = ? ORDER BY timestamp, id LIMIT/OFFSET
        if SQLALCHEMY_DATABASE_URL.startswith("sqlite"):
            with engine.begin() as conn:
                _ensure_index(
                    conn,
                    "ix_realized_events_run_asset_ts_id",
                    "realized_events",
                    "(run_id, asset, timestamp, id)",
                )

        log_success_and_clear_latest("db", "Database initialized and schema verified.")
    except Exception as e:
        log_exception_and_record_latest("db", e, message="Database initialization failed")
        raise


def auto_repair_migrations() -> None:
    """
    Legacy-compatible helper used by smoke tests and startup tooling.

    It runs the same safe, idempotent initialization path as the FastAPI
    startup (init_db + compatibility objects), but swallows exceptions
    after logging so callers can treat failures as a "soft" signal instead
    of crashing the process.
    """
    try:
        init_db(engine)
    except Exception as e:
        # We already logged inside init_db; record a concise marker here too.
        log_exception_and_record_latest("db", e, message="auto_repair_migrations failed (soft)")
        # Do NOT re-raise: smoke tests expect this helper not to blow up.
        return

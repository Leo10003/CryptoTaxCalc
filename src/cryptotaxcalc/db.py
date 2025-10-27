from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
import decimal
from sqlalchemy.engine import Connection

try:
    import sqlite3
    # Store as string to preserve precision and avoid float rounding surprises
    sqlite3.register_adapter(decimal.Decimal, lambda v: str(v))
except Exception:
    pass

# -----------------------------------------------------------------------------
# Single source of truth for the SQLAlchemy base/engine/session
# -----------------------------------------------------------------------------
Base = declarative_base()

DB_URL = os.getenv("CTC_DB_URL", "sqlite:///./data.db")

engine = create_engine(
    DB_URL,
    future=True,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# -----------------------------------------------------------------------------
# DDL helpers (CREATE IF NOT EXISTS) for tables used by raw SQL utilities
# -----------------------------------------------------------------------------

def _ensure_transactions_table(conn) -> None:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        timestamp TEXT NOT NULL,
        type TEXT NOT NULL,
        base_asset TEXT NOT NULL,
        base_amount NUMERIC NOT NULL,
        quote_asset TEXT,
        quote_amount NUMERIC,
        fee_asset TEXT,
        fee_amount NUMERIC,
        exchange TEXT,
        memo TEXT,
        fair_value NUMERIC,
        raw_event_id INTEGER,
        created_at TEXT DEFAULT (CURRENT_TIMESTAMP)
    )
    """)
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_timestamp ON transactions(timestamp)")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_type ON transactions(type)")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_base_asset ON transactions(base_asset)")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_quote_asset ON transactions(quote_asset)")


def _ensure_fx_batches_table(conn) -> None:
    # Must match what fx_utils writes: imported_at, source, rates_hash
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS fx_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT,
        source TEXT,
        rates_hash TEXT
    )
    """)


def _ensure_calc_runs_table(conn):
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS calc_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            jurisdiction TEXT,
            rule_version TEXT,
            lot_method TEXT,
            fx_set_id INTEGER,
            params_json TEXT,
            finished_at TEXT
        )
    """)


def _ensure_compatibility_views(conn) -> None:
    # Drop either a view or a mistakenly created table named transactions_rows, then recreate a view
    conn.exec_driver_sql("DROP VIEW IF EXISTS transactions_rows")
    conn.exec_driver_sql("DROP TABLE IF EXISTS transactions_rows")
    conn.exec_driver_sql("""
        CREATE VIEW transactions_rows AS
        SELECT
            t.id          AS id,
            t.hash        AS hash,
            t.timestamp   AS timestamp,
            t.type        AS type,
            t.base_asset  AS base_asset,
            t.base_amount AS base_amount,
            t.quote_asset AS quote_asset,
            t.quote_amount AS quote_amount,
            t.fee_asset   AS fee_asset,
            t.fee_amount  AS fee_amount,
            t.exchange    AS exchange,
            t.memo        AS memo,
            t.fair_value  AS fair_value,
            t.raw_event_id AS raw_event_id
        FROM transactions t
    """)
  
    
def _ensure_compatibility_objects(conn):
    conn.exec_driver_sql("""
        CREATE VIEW IF NOT EXISTS transactions_rows AS
         SELECT id, hash, timestamp, type, base_asset, base_amount,
                quote_asset, quote_amount, fee_asset, fee_amount,
                exchange, memo, fair_value, raw_event_id
        FROM transactions
    """)
    # table used by fx_utils.get_or_create_current_fx_batch_id()
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS fx_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)


def _ensure_realized_events_table(conn):
    # Base table (create if not exists)
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS realized_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            tx_id INTEGER,
            timestamp TEXT NOT NULL,
            asset TEXT NOT NULL,
            qty_sold TEXT NOT NULL,
            proceeds TEXT NOT NULL,
            cost_basis TEXT NOT NULL,
            gain TEXT NOT NULL,
            quote_asset TEXT,
            -- not all code paths set fee columns, keep nullable
            fee_asset TEXT,
            fee_amount TEXT,
            -- some paths store applied fee or fx info separately
            fee_applied TEXT,
            matches_json TEXT
        )
    """)
    # Backfill for older DBs
    for ddl in [
        "ALTER TABLE realized_events ADD COLUMN tx_id INTEGER",
        "ALTER TABLE realized_events ADD COLUMN fee_asset TEXT",
        "ALTER TABLE realized_events ADD COLUMN fee_amount TEXT",
        "ALTER TABLE realized_events ADD COLUMN fee_applied TEXT",
        "ALTER TABLE realized_events ADD COLUMN matches_json TEXT",
    ]:
        try:
            conn.exec_driver_sql(ddl)
        except Exception:
            pass


def _ensure_run_digests_table(conn) -> None:
    """
    Table used by history/tests to store hashes for a calculation run.
    """
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS run_digests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            -- NEW: tests expect this column to exist
            input_hash TEXT,
            -- Optional future fields you might populate
            manifest_hash TEXT,
            summary_hash TEXT,
            events_hash TEXT,
            created_at TEXT
        )
    """)


def _ensure_calc_audit_table(conn) -> None:
    """
    Minimal audit table used by history/audit endpoints & tests.
    """
    conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS calc_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            created_at TEXT,
            manifest_hash TEXT,
            summary_hash TEXT,
            events_hash TEXT,
            -- NEW: tests expect this column to exist
            actor TEXT
        )
    """)


def _sqlite_table_has_column(conn: Connection, table: str, column: str) -> bool:
    rows = conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()
    # PRAGMA table_info result: (cid, name, type, notnull, dflt_value, pk)
    return any(r[1] == column for r in rows)


def _sqlite_add_column_if_missing(conn, table: str, column: str, ddl: str) -> None:
    # ddl is the column definition after ADD COLUMN, e.g. "manifest_json TEXT"
    cols = {r[1] for r in conn.exec_driver_sql(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_compatibility_columns(conn) -> None:
    # run_digests columns expected by tests
    _sqlite_add_column_if_missing(conn, "run_digests", "input_hash", "TEXT")
    _sqlite_add_column_if_missing(conn, "run_digests", "output_hash", "TEXT")
    _sqlite_add_column_if_missing(conn, "run_digests", "manifest_json", "TEXT")

    # audit_log columns expected by tests
    _sqlite_add_column_if_missing(conn, "audit_log", "actor", "TEXT")
    _sqlite_add_column_if_missing(conn, "audit_log", "action", "TEXT")
    _sqlite_add_column_if_missing(conn, "audit_log", "meta_json", "TEXT")


def _ensure_new_columns(engine):
    with engine.begin() as conn:
        def has_col(table, col):
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
            return any(r[1] == col for r in rows)

        # run_digests: ensure manifest_json exists
        if not has_col("run_digests", "manifest_json"):
            conn.execute(text("ALTER TABLE run_digests ADD COLUMN manifest_json TEXT"))

        # audit_log: ensure meta_json exists
        if not has_col("audit_log", "meta_json"):
            conn.execute(text("ALTER TABLE audit_log ADD COLUMN meta_json TEXT"))


_CREATED = False

# --- lightweight migration helpers ---
def _column_exists(conn, table, column) -> bool:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return any(r[1] == column for r in rows)

# -----------------------------------------------------------------------------
# One-time initializer (safe to call multiple times)
# -----------------------------------------------------------------------------

_tables_ready = False

def init_db() -> None:
    """
    Create ORM tables and compatibility tables/views in a single transaction.
    Safe to call multiple times; uses CREATE TABLE IF NOT EXISTS.
    """
    # Import here to avoid circular import at module import time
    from .models import Base

    # IMPORTANT: do everything inside ONE begin() context to avoid nested-begin issues
    with engine.begin() as conn:  # 'engine' must already be defined in this module
        # Create all ORM-mapped tables
        Base.metadata.create_all(bind=conn)

        # Ensure extra tables needed by endpoints/tests
        # If you have these helpers defined elsewhere in this file, they’ll be called safely.
        if ' _ensure_transactions_table' in globals():
            _ensure_transactions_table(conn)  # optional, if you kept it
        if '_ensure_fx_batches_table' in globals():
            _ensure_fx_batches_table(conn)    # optional, if you kept it
        if '_ensure_calc_runs_table' in globals():
            _ensure_calc_runs_table(conn)     # optional, if you kept it
        if '_ensure_realized_events_table' in globals():
            _ensure_realized_events_table(conn)  # optional, if you kept it

        # New: ALWAYS ensure these two (fixes the failing tests)
        _ensure_run_digests_table(conn)
        _ensure_calc_audit_table(conn)

        # If you maintain compatibility objects/views, call them *after* base tables exist
        if '_ensure_compatibility_objects' in globals():
            _ensure_compatibility_objects(conn)  # optional
        if '_ensure_compatibility_views' in globals():
            _ensure_compatibility_views(conn)    # optional
            
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS run_digests (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY,
                details_json TEXT,
                ip TEXT,
                ts TEXT NOT NULL
            )
        """)
        _ensure_compatibility_columns(conn)
        _ensure_new_columns(engine)

        
    _CREATED = True
    _tables_ready = True

if os.getenv("CTC_EAGER_INIT", "1") == "1":
    try:
        init_db()
    except Exception:
        pass
    

from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# -----------------------------------------------------------------------------
# Single source of truth for the SQLAlchemy base/engine/session
# -----------------------------------------------------------------------------
Base = declarative_base()

DB_URL = os.getenv("CTC_DB_URL", "sqlite:///./cryptotaxcalc.db")

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
    
def _ensure_compatibility_objects(engine):
    # legacy view used by older code
    with engine.begin() as conn:
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

_CREATED = False

# -----------------------------------------------------------------------------
# One-time initializer (safe to call multiple times)
# -----------------------------------------------------------------------------

_tables_ready = False

def init_db() -> None:
    global _tables_ready
    global _CREATED
    
    if _CREATED:
        return
    if _tables_ready:
        return

    # Import models INSIDE this function so SQLAlchemy registers them
    # before we call Base.metadata.create_all(...)
    from . import models  # noqa: F401  (do not remove)

    # Create ORM tables
    Base.metadata.create_all(bind=engine)

    # Create raw-SQL tables/views that the code uses directly
    with engine.begin() as conn:
        _ensure_transactions_table(conn)
        _ensure_fx_batches_table(conn)
        _ensure_calc_runs_table(conn)
        _ensure_compatibility_views(conn)


    _CREATED = True
    _tables_ready = True

if os.getenv("CTC_EAGER_INIT", "1") == "1":
    try:
        init_db()
    except Exception:
        pass
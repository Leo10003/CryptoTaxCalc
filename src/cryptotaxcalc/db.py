# db.py
"""
Database bootstrap:
- Creates a SQLAlchemy Engine bound to a local SQLite file (data.db).
- Exposes SessionLocal for short-lived DB sessions per request.

Notes:
- SQLite is perfect for local dev. Later, swap DATABASE_URL to Postgres
  (e.g., "postgresql+psycopg://user:pass@host/dbname") without changing the rest of the code.
"""

from __future__ import annotations
from sqlalchemy import create_engine, MetaData, text
from pathlib import Path
from typing import Iterator
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import threading


# SQLite DB file in the project directory. For Postgres later, change this string.
DATABASE_URL = "sqlite:///data.db"
DB_PATH = Path(__file__).resolve().parents[2] / "data.db"
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(
    DB_URL,
    future=True,
    connect_args={"check_same_thread": False},
)

# --- ORM registry/base/metadata ---
metadata = MetaData()
Base = declarative_base(metadata=metadata)

# --- Session factory ---
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

_INIT_LOCK = threading.Lock()
_INIT_DONE = False

# --- ADD: idempotent DDL for audit/provenance ---

DDL = [
    # Original upload provenance (append-only)
    """
    CREATE TABLE IF NOT EXISTS raw_events (
        id INTEGER PRIMARY KEY,
        source_filename TEXT NOT NULL,
        file_sha256 TEXT NOT NULL,
        mime_type TEXT,
        importer TEXT,
        received_at TEXT NOT NULL,
        notes TEXT,
        blob_path TEXT
    );
    """,

    # FX import batches; link rates to a batch for reproducibility
    """
    CREATE TABLE IF NOT EXISTS fx_batches (
        id INTEGER PRIMARY KEY,
        imported_at TEXT NOT NULL,
        source TEXT NOT NULL,
        rates_hash TEXT
    );
    """,

    # Ensure fx_rates table exists (if your ORM already creates it, this is harmless)
    """
    CREATE TABLE IF NOT EXISTS fx_rates (
        date TEXT PRIMARY KEY,
        usd_per_eur TEXT NOT NULL,
        batch_id INTEGER
    );
    """,

    # Calculation runs metadata (freeze params/versions/fx set)
    """
    CREATE TABLE IF NOT EXISTS calc_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at TIMESTAMP NOT NULL,
        jurisdiction TEXT,
        rule_version TEXT,
        lot_method TEXT,
        fx_set_id INTEGER,
        params_json TEXT,
        finished_at TIMESTAMP,
        status TEXT,
        error TEXT
    )
    """,


    # Persisted realized events per run (for explainability)
    """
    CREATE TABLE IF NOT EXISTS realized_events (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL,
        tx_id INTEGER,
        timestamp TEXT NOT NULL,
        asset TEXT NOT NULL,
        qty_sold TEXT NOT NULL,
        proceeds TEXT NOT NULL,
        cost_basis TEXT NOT NULL,
        gain TEXT NOT NULL,
        quote_asset TEXT,
        fee_applied TEXT,
        matches_json TEXT NOT NULL
    );
    """,

    # Append-only audit log
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY,
        actor TEXT,
        action TEXT NOT NULL,
        target_type TEXT,
        target_id INTEGER,
        details_json TEXT,
        ip TEXT,
        ts TEXT NOT NULL
    );
    """,

        # Hashes & canonical manifest per calculation run (one row per run)
    """
    CREATE TABLE IF NOT EXISTS run_digests (
        id INTEGER PRIMARY KEY,
        run_id INTEGER UNIQUE NOT NULL,
        input_hash TEXT NOT NULL,
        output_hash TEXT NOT NULL,
        manifest_hash TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    """,

        # Add a small new table for lightweight audit lookup if it doesn’t already exist.
    """
    CREATE TABLE IF NOT EXISTS calc_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        actor TEXT,
        action TEXT,
        meta_json TEXT,
        created_at TEXT,
        FOREIGN KEY(run_id) REFERENCES calc_runs(id)
    );
    """,
]

# --- ensure transactions table exists for SQLite dev/test ---
TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY,
    hash TEXT,
    timestamp TEXT,
    type TEXT,
    base_asset TEXT,
    base_amount TEXT,
    quote_asset TEXT,
    quote_amount TEXT,
    fee_asset TEXT,
    fee_amount TEXT,
    exchange TEXT,
    memo TEXT,
    fair_value TEXT,
    raw_event_id INTEGER
);
"""


def _sqlite_object_exists(conn, name: str, type_: str) -> bool:
    """
    Return True if a SQLite object exists (table/view) in current database.
    `conn` must be a SQLAlchemy Connection.
    """
    row = conn.exec_driver_sql(
        "SELECT name FROM sqlite_master WHERE type = ? AND name = ?",
        (type_, name),
    ).fetchone()
    return row is not None


def _ensure_compatibility_views(conn) -> None:
    """
    Create or replace compatibility views needed by the app/tests.
    `conn` must be a SQLAlchemy Connection.
    """
    # Example: transactions_rows compatibility view
    # Drop if exists, then create
    if _sqlite_object_exists(conn, "transactions_rows", "view") or \
       _sqlite_object_exists(conn, "transactions_rows", "table"):
        try:
            conn.exec_driver_sql("DROP VIEW IF EXISTS transactions_rows")
        except Exception:
            # In case a TABLE with that name exists in some dev DBs
            conn.exec_driver_sql("DROP TABLE IF EXISTS transactions_rows")

    # Recreate the view mapping to your actual table/columns
    conn.exec_driver_sql(
        """
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
        """
    )


def _ensure_transactions_table(conn) -> None:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hash TEXT UNIQUE,
        timestamp TEXT NOT NULL,
        type TEXT NOT NULL,                      -- e.g. BUY/SELL/TRANSFER/etc (TxType)
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
        created_at TEXT DEFAULT (CURRENT_TIMESTAMP),
        FOREIGN KEY(raw_event_id) REFERENCES raw_events(id)
    );
    """)

# Useful indexes (safe if they already exist)
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_timestamp ON transactions(timestamp);")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_type ON transactions(type);")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_base_asset ON transactions(base_asset);")
    conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS ix_transactions_quote_asset ON transactions(quote_asset);")


def init_db(force: bool = False) -> None:
    global _INIT_DONE
    with _INIT_LOCK:
        if _INIT_DONE and not force:
            return

        # 1) ORM tables
        Base.metadata.create_all(bind=engine)

        # 2) Low-level SQLite tables needed by the app/tests
        with engine.begin() as conn:
            conn.exec_driver_sql("""
                CREATE TABLE IF NOT EXISTS calc_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    jurisdiction TEXT,             
                    rule_version TEXT NOT NULL,
                    lot_method TEXT NOT NULL,
                    fx_set_id INTEGER,
                    params_json TEXT,
                    FOREIGN KEY(fx_set_id) REFERENCES fx_batches(id)
                )
            """)
            # Execute idempotent DDLs
            for ddl in DDL:
                conn.exec_driver_sql(ddl)

            # 3) Recreate compatibility views
            _ensure_compatibility_views(conn)

            _ensure_transactions_table(conn)

        _INIT_DONE = True

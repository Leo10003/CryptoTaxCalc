# db.py
"""
Database bootstrap:
- Creates a SQLAlchemy Engine bound to a local SQLite file (data.db).
- Exposes SessionLocal for short-lived DB sessions per request.

Notes:
- SQLite is perfect for local dev. Later, swap DATABASE_URL to Postgres
  (e.g., "postgresql+psycopg://user:pass@host/dbname") without changing the rest of the code.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# SQLite DB file in the project directory. For Postgres later, change this string.
DATABASE_URL = "sqlite:///data.db"

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
        id INTEGER PRIMARY KEY,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        jurisdiction TEXT,
        rule_version TEXT NOT NULL,
        lot_method TEXT NOT NULL,
        fx_set_id INTEGER,
        params_json TEXT NOT NULL
    );
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

def init_db():
    # Run DDL and ensure helpful columns/indexes exist
    with engine.begin() as conn:
        for ddl in DDL:
            conn.exec_driver_sql(ddl)

        # Ensure base tables exist (especially transactions, required for indexes)
        conn.exec_driver_sql(TRANSACTIONS_DDL)

        # --- Add any missing columns (SQLite >= 3.35 supports IF NOT EXISTS) ---
        missing_cols = [
            "hash TEXT",
            "timestamp TEXT",
            "type TEXT",
            "base_asset TEXT",
            "base_amount TEXT",
            "quote_asset TEXT",
            "quote_amount TEXT",
            "fee_asset TEXT",
            "fee_amount TEXT",
            "exchange TEXT",
            "memo TEXT",
            "fair_value TEXT",
            "raw_event_id INTEGER",
        ]
        for coldef in missing_cols:
            try:
                conn.exec_driver_sql(f"ALTER TABLE transactions ADD COLUMN IF NOT EXISTS {coldef}")
            except Exception:
                # Older SQLite (very rare on Py3.12) doesn’t support IF NOT EXISTS; ignore if column exists
                pass
        
        # Indexes for speed
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_fx_rates_date ON fx_rates(date)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_fx_rates_pair ON fx_rates(date, usd_per_eur)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_realized_events_run ON realized_events(run_id)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_transactions_raw_evt ON transactions(raw_event_id)")
        conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_run_digests_run ON run_digests(run_id)")

# echo=False hides SQL logs; set True if you want to see SQL statements during debugging.
engine = create_engine(DATABASE_URL, echo=False, future=True)

# "SessionLocal" is a factory to create new Session objects.
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


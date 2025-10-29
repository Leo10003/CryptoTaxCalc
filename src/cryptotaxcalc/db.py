from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

# ---------- Engine / Session ----------
DB_URL = os.getenv("CRYPTO_TAXCALC_DB_URL", "sqlite:///./cryptotaxcalc.db")

# echo=False to keep tests quiet
_engine: Engine = create_engine(DB_URL, future=True, echo=False)
# Expose the engine so other modules can import it
engine = _engine


SessionLocal = sessionmaker(
    bind=_engine,
    autoflush=False,
    autocommit=False,
    future=True,
)

# ---------- Init helpers ----------

def _ensure_table(conn, ddl: str) -> None:
    """CREATE TABLE IF NOT EXISTS …"""
    conn.exec_driver_sql(ddl)

def _ensure_columns(conn, table: str, required: dict[str, str]) -> None:
    """
    Ensure each column in `required` exists on `table`.  For each missing col,
    perform ALTER TABLE … ADD COLUMN with the provided SQL snippet (type/default/constraints).
    """
    rows = conn.exec_driver_sql(f"PRAGMA table_info('{table}')").fetchall()
    existing = {row[1] for row in rows}  # row[1] = name

    for col_name, col_spec in required.items():
        if col_name not in existing:
            conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_spec}")

def _ensure_compatibility_objects(engine: Engine) -> None:
    """
    Create/repair minimal tables/columns that are used by app paths which the
    smoke tests touch, without requiring Alembic.
    """
    with engine.begin() as conn:
        # fx_batches: used by fx_utils.get_or_create_current_fx_batch_id()
        _ensure_table(
            conn,
            """
            CREATE TABLE IF NOT EXISTS fx_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imported_at TEXT NOT NULL,
                source TEXT NOT NULL,
                rates_hash TEXT
            )
            """,
        )
        _ensure_columns(
            conn,
            "fx_batches",
            {
                "imported_at": "TEXT NOT NULL",
                "source": "TEXT NOT NULL",
                "rates_hash": "TEXT",
            },
        )

        # calc_audit: used by /audit/history (keep it minimal for the tests)
        _ensure_table(
            conn,
            """
            CREATE TABLE IF NOT EXISTS calc_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                actor TEXT,
                action TEXT,
                meta_json TEXT
            )
            """,
        )
        _ensure_columns(
            conn,
            "calc_audit",
            {
                "created_at": "TEXT NOT NULL",
                "actor": "TEXT",
                "action": "TEXT",
                "meta_json": "TEXT",
            },
        )

        # run_digests: used by history endpoints / calculate flow
        _ensure_table(
            conn,
            """
            CREATE TABLE IF NOT EXISTS run_digests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_hash TEXT,
                output_hash TEXT,
                manifest_json TEXT
            )
            """,
        )
        _ensure_columns(
            conn,
            "run_digests",
            {
                "input_hash": "TEXT",
                "output_hash": "TEXT",
                "manifest_json": "TEXT",
            },
        )

def init_db() -> None:
    
    def ensure_calc_runs_table(engine):
        ddl = text("""
        CREATE TABLE IF NOT EXISTS calc_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            jurisdiction TEXT NOT NULL,
            rule_version TEXT NOT NULL,
            lot_method TEXT NOT NULL,
            fx_set_id INTEGER NOT NULL,
            params_json TEXT NOT NULL
        )
        """)
        with engine.begin() as conn:
            conn.execute(ddl)
    
    """
    Create ORM tables and ensure compatibility tables/columns exist.
    """
    # Import models here to avoid circular imports
    from .models import Base  # noqa: WPS433 (import inside function)

    # Create ORM-managed tables
    Base.metadata.create_all(bind=_engine)  # no-ops on existing

    # Create/repair compatibility tables/columns used in the app/tests
    _ensure_compatibility_objects(_engine)

# convenience context manager used sometimes in app code
@contextmanager
def db_session() -> Iterator:
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


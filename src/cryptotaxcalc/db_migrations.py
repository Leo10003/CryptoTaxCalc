from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_fx_schema_v2(engine: Engine) -> None:
    """Backward-compatible wrapper; use ensure_fx_schema() as the single authority."""
    ensure_fx_schema(engine)


def migrate_fx_schema(engine: Engine) -> None:
    """
    Bring FX tables to the expected shape.

    Expected shape:
      fx_rates(date TEXT, base TEXT, quote TEXT, rate NUMERIC, batch_id INTEGER, [optional id PK])
      fx_batches(id INTEGER PK, date TEXT UNIQUE, created_at TEXT, imported_at TEXT, source TEXT, rates_hash TEXT)

    Idempotent and safe on SQLite.
    """
    with engine.begin() as conn:
        # ---- fx_rates ----
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS fx_rates (
                date TEXT,
                base TEXT,
                quote TEXT,
                rate NUMERIC,
                batch_id INTEGER
            )
            """
        )

        cols = [r[1] for r in conn.execute(text("PRAGMA table_info(fx_rates)")).fetchall()]

        def _add_fx_rates_col(sql: str) -> None:
            conn.execute(text(f"ALTER TABLE fx_rates ADD COLUMN {sql}"))

        if "date" not in cols:
            _add_fx_rates_col("date TEXT")
        if "base" not in cols:
            _add_fx_rates_col("base TEXT")
        if "quote" not in cols:
            _add_fx_rates_col("quote TEXT")
        if "rate" not in cols:
            _add_fx_rates_col("rate NUMERIC")
        if "batch_id" not in cols:
            _add_fx_rates_col("batch_id INTEGER")

        # Indexes (non-unique for legacy compatibility; avoids failing on duplicates)
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_fx_rates_date_base_quote ON fx_rates(date, base, quote)")
        )

        # Deduplicate legacy rows before enforcing uniqueness (prevents startup failures)
        try:
            conn.exec_driver_sql(
                """
                DELETE FROM fx_rates
                WHERE rowid NOT IN (
                    SELECT MAX(rowid)
                    FROM fx_rates
                    GROUP BY date, base, quote
                )
                """
            )
        except Exception:
            pass

        conn.execute(
            text("CREATE UNIQUE INDEX IF NOT EXISTS uq_fx_rates_date_pair ON fx_rates(date, base, quote)")
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_fx_rates_batch_date ON fx_rates(batch_id, date)")
        )
        
         # Ensure fx_batches exists before we ALTER TABLE it (prevents migration failures on fresh DBs).
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS fx_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            )
            """
        )

        bcols = [r[1] for r in conn.execute(text("PRAGMA table_info(fx_batches)")).fetchall()]

        def _add_fx_batches_col(sql: str) -> None:
            conn.execute(text(f"ALTER TABLE fx_batches ADD COLUMN {sql}"))

        if "date" not in bcols:
            _add_fx_batches_col("date TEXT")
        if "created_at" not in bcols:
            _add_fx_batches_col("created_at TEXT")
        if "imported_at" not in bcols:
            _add_fx_batches_col("imported_at TEXT")
        if "source" not in bcols:
            _add_fx_batches_col("source TEXT")
        if "rates_hash" not in bcols:
            _add_fx_batches_col("rates_hash TEXT")

        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_fx_batches_date ON fx_batches(date)"))

        # Ensure at least one batch exists (legacy compatibility)
        row = conn.execute(text("SELECT id FROM fx_batches ORDER BY id LIMIT 1")).fetchone()
        if row is None:
            now_iso = _utc_now_iso()
            conn.execute(
                text(
                    """
                    INSERT INTO fx_batches (date, created_at, imported_at, source, rates_hash)
                    VALUES (:d, :c, :i, :s, :h)
                    """
                ),
                {
                    "d": "1970-01-01",
                    "c": now_iso,
                    "i": now_iso,
                    "s": "legacy-bootstrap",
                    "h": None,
                },
            )
            row = conn.execute(text("SELECT id FROM fx_batches ORDER BY id LIMIT 1")).fetchone()

        first_id = row[0] if row else None
        if first_id is not None:
            conn.execute(
                text("UPDATE fx_rates SET batch_id = :bid WHERE batch_id IS NULL"),
                {"bid": first_id},
            )


def migrate_fx_rates_add_id(engine: Engine) -> None:
    """Ensure fx_rates has an autoincrement id column. Safe to call at startup."""
    migrate_fx_schema(engine)

    with engine.begin() as conn:
        cols = conn.exec_driver_sql("PRAGMA table_info('fx_rates')").fetchall()
        colnames = {c[1] for c in cols}
        if "id" in colnames:
            return

        # Create new table with the correct schema
        conn.exec_driver_sql(
            """
            CREATE TABLE IF NOT EXISTS fx_rates_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                base TEXT NOT NULL,
                quote TEXT NOT NULL,
                rate NUMERIC NOT NULL,
                batch_id INTEGER
            )
            """
        )

        # Copy existing data with legacy compatibility
        if {"date", "usd_per_eur", "batch_id"}.issubset(colnames):
            conn.exec_driver_sql(
                """
                INSERT INTO fx_rates_new (date, base, quote, rate, batch_id)
                SELECT date, 'USD', 'EUR', (1.0 / CAST(usd_per_eur AS REAL)), batch_id FROM fx_rates
                """
            )
        elif {"date", "usd_per_eur"}.issubset(colnames):
            conn.exec_driver_sql(
                """
                INSERT INTO fx_rates_new (date, base, quote, rate, batch_id)
                SELECT date, 'USD', 'EUR', (1.0 / CAST(usd_per_eur AS REAL)), NULL FROM fx_rates
                """
            )
        elif {"date", "rate", "batch_id", "base", "quote"}.issubset(colnames):
            conn.exec_driver_sql(
                """
                INSERT INTO fx_rates_new (date, base, quote, rate, batch_id)
                SELECT date, base, quote, CAST(rate AS REAL), batch_id FROM fx_rates
                """
            )
        elif {"date", "rate", "base", "quote"}.issubset(colnames):
            conn.exec_driver_sql(
                """
                INSERT INTO fx_rates_new (date, base, quote, rate, batch_id)
                SELECT date, base, quote, CAST(rate AS REAL), NULL FROM fx_rates
                """
            )
        elif {"date", "rate"}.issubset(colnames):
            conn.exec_driver_sql(
                """
                INSERT INTO fx_rates_new (date, base, quote, rate, batch_id)
                SELECT date, 'USD', 'EUR', CAST(rate AS REAL), NULL FROM fx_rates
                """
            )

        # Swap tables
        conn.exec_driver_sql("DROP TABLE fx_rates")
        conn.exec_driver_sql("ALTER TABLE fx_rates_new RENAME TO fx_rates")

        # Recreate indexes
        conn.exec_driver_sql(
            """
            CREATE INDEX IF NOT EXISTS ix_fx_rates_date_base_quote
            ON fx_rates(date, base, quote)
            """
        )
        conn.exec_driver_sql(
            """
            CREATE INDEX IF NOT EXISTS ix_fx_rates_batch_date
            ON fx_rates(batch_id, date)
            """
        )


def ensure_fx_schema(engine: Engine) -> None:
    """
    Single source of truth for FX schema/migrations.
    Safe and idempotent on SQLite.
    """
    migrate_fx_schema(engine)
    migrate_fx_rates_add_id(engine)
    # Re-apply schema/indexes + legacy batch_id repair after potential rebuild
    migrate_fx_schema(engine)

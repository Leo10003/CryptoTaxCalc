# src/cryptotaxcalc/fx_utils.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Tuple, Optional

import logging
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

# Pull Session factory from your models so this module can work with/without an injected db.
try:
    from .models import SessionLocal  # typical pattern in this codebase
except Exception:  # pragma: no cover
    SessionLocal = None  # type: ignore

logger = logging.getLogger("cryptotaxcalc.fx")

_SessionFactory: Optional[sessionmaker] = None  # set from app.py on startup

def set_session_factory(factory: sessionmaker) -> None:
    global _SessionFactory
    _SessionFactory = factory

def _maybe_session(db: Optional[Session]) -> tuple[Session, bool]:
    """
    Return (session, created_here). If db is provided, reuse it.
    If not, try to create one via _SessionFactory; created_here=True means we must close it.
    """
    if db is not None:
        return db, False
    if _SessionFactory is None:
        return None, False  # signals "no session available"
    sess = _SessionFactory()
    return sess, True

def ensure_fx_rates_schema(db: Session) -> None:
    """
    Ensure FX tables exist and are migrated for the *current* DB bind.

    Important:
    A process-global guard alone is unsafe in tests/tooling that create multiple engines
    (e.g., sqlite:///:memory:). We therefore verify the table exists for this session
    before fast-returning.
    """
    global _FX_SCHEMA_READY

    # Fast path: if we already ran schema ensure in this process, verify the table exists
    # for this session/bind. If not, fall through and ensure again.
    if _FX_SCHEMA_READY:
        try:
            db.execute(text("SELECT 1 FROM fx_rates LIMIT 1"))
            return
        except Exception:
            _FX_SCHEMA_READY = False

    try:
        from sqlalchemy.engine import Engine
        from .db_migrations import ensure_fx_schema

        bind = db.get_bind()
        engine = bind if isinstance(bind, Engine) else bind.engine
        ensure_fx_schema(engine)

        _FX_SCHEMA_READY = True
        return

    except Exception:
        # Last-resort fallback for legacy DBs: ensure the table exists, then add missing columns.
        try:
            bind = db.get_bind()
            dialect_name = getattr(getattr(bind, "dialect", None), "name", "")
            if dialect_name == "sqlite":
                db.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS fx_rates (
                            date TEXT NOT NULL,
                            base TEXT NOT NULL DEFAULT 'USD',
                            quote TEXT NOT NULL DEFAULT 'EUR',
                            rate TEXT NOT NULL,
                            batch_id INTEGER,
                            PRIMARY KEY (date, base, quote)
                        )
                        """
                    )
                )
                cols = db.execute(text("PRAGMA table_info(fx_rates)")).fetchall()
                names = {c[1] for c in cols}

                if "batch_id" not in names:
                    db.execute(text("ALTER TABLE fx_rates ADD COLUMN batch_id INTEGER"))
                if "base" not in names:
                    db.execute(text("ALTER TABLE fx_rates ADD COLUMN base TEXT DEFAULT 'USD'"))
                if "quote" not in names:
                    db.execute(text("ALTER TABLE fx_rates ADD COLUMN quote TEXT DEFAULT 'EUR'"))

                db.commit()
        finally:
            _FX_SCHEMA_READY = True

# Small in-proc cache to avoid repeated SELECTs within a single request
# Keyed by (base, quote, day_iso)
_last_rate_cache: Dict[Tuple[str, str, str], Decimal] = {}

# Full lookup cache to avoid repeated weekend/holiday lookups for the same requested day.
# Keyed by (base, quote, requested_day_iso)
_lookup_cache: Dict[Tuple[str, str, str], FxRateLookup] = {}

# Ensure FX schema only once per process (schema is already enforced at startup).
_FX_SCHEMA_READY = False


@dataclass(frozen=True)
class FxRateLookup:
    rate: Decimal
    matched_date: str | None
    looked_back_days: int
    used_fallback: bool


def get_rate_for_date_lookup(
    db: Session,
    day: date,
    *,
    base: str = "USD",
    quote: str = "EUR",
    max_lookback_days: int = 7,
    default_rate: Decimal = Decimal("1.0"),
) -> FxRateLookup:
    ensure_fx_rates_schema(db)

    base = (base or "USD").upper().strip()
    quote = (quote or "EUR").upper().strip()

    req_key = (base, quote, day.isoformat())
    cached = _lookup_cache.get(req_key)
    if cached is not None:
        return cached

    looked_back = 0
    probe = day

    for _ in range(max_lookback_days + 1):
        k = (base, quote, probe.isoformat())

        if k in _last_rate_cache:
            res = FxRateLookup(
                rate=_last_rate_cache[k],
                matched_date=probe.isoformat(),
                looked_back_days=looked_back,
                used_fallback=False,
            )
            _lookup_cache[req_key] = res
            return res

        row = db.execute(
            text("""
                SELECT rate
                FROM fx_rates
                WHERE date = :d AND base = :b AND quote = :q
                ORDER BY rowid DESC
                LIMIT 1
            """),
            {"d": probe.isoformat(), "b": base, "q": quote},
        ).first()

        if row and row[0] is not None:
            found_rate = Decimal(str(row[0]))
            _last_rate_cache[k] = found_rate
            if looked_back > 0:
                logger.info(
                    "FX lookback used: %s/%s for %s not found, using %s rate.",
                    base, quote, day.isoformat(), probe.isoformat()
                )
            res = FxRateLookup(
                rate=found_rate,
                matched_date=probe.isoformat(),
                looked_back_days=looked_back,
                used_fallback=False,
            )
            _lookup_cache[req_key] = res
            return res

        probe = probe - timedelta(days=1)
        looked_back += 1

    logger.warning(
        "No FX rate for %s/%s in the last %d days ending %s; using default_rate=%s.",
        base, quote, max_lookback_days, day.isoformat(), str(default_rate),
    )
    # Cache fallback only in the lookup cache (NOT in _last_rate_cache).
    #
    # Why: storing default fallback rates in _last_rate_cache would make subsequent
    # lookups appear as "non-fallback" (because _last_rate_cache hits set used_fallback=False),
    # which can silently bypass strict_fx enforcement.
    res = FxRateLookup(
        rate=default_rate,
        matched_date=None,
        looked_back_days=max_lookback_days,
        used_fallback=True,
    )
    _lookup_cache[req_key] = res
    return res


def get_rate_for_date(
    db: Session,
    day: date,
    *,
    base: str = "USD",
    quote: str = "EUR",
    max_lookback_days: int = 7
) -> Decimal:
    return get_rate_for_date_lookup(
        db,
        day,
        base=base,
        quote=quote,
        max_lookback_days=max_lookback_days,
        default_rate=Decimal("1.0"),
    ).rate


def clear_fx_cache() -> None:
    """Clear in-process FX caches."""
    _last_rate_cache.clear()
    _lookup_cache.clear()


# -----------------------------------------
# Legacy shims kept for backward-compat
# -----------------------------------------
def ensure_rate_or_default_lookup(
    db: Session,
    day: date,
    *,
    base: str = "USD",
    quote: str = "EUR",
    default_rate: Decimal | str | float = Decimal("1.0"),
    max_lookback_days: int = 7,
) -> FxRateLookup:
    dr = default_rate if isinstance(default_rate, Decimal) else Decimal(str(default_rate))
    return get_rate_for_date_lookup(
        db,
        day,
        base=base,
        quote=quote,
        max_lookback_days=max_lookback_days,
        default_rate=dr,
    )

def ensure_rate_or_default(*args,
                           base: str = "USD",
                           quote: str = "EUR",
                           default_rate: Decimal | str | float = Decimal("1.0"),
                           db: Optional[Session] = None) -> Decimal:
    """
    Backward-compatible helper.

    Accepts EITHER:
      1) (day, *, base="USD", quote="EUR", default_rate=..., db=None)
      2) (db, day, *, base="USD", quote="EUR", default_rate=...)

    Returns Decimal FX rate using weekend/holiday fallback.
    """
    # ---- Parse positional args for both historical call styles ----
    if len(args) == 0:
        raise TypeError("ensure_rate_or_default requires at least a date or (db, date).")

    _db: Optional[Session]
    _day: date

    if len(args) >= 2 and isinstance(args[0], Session):
        # Legacy style: (db, day)
        _db = args[0]
        _day = args[1]
    else:
        # New style: (day,)
        _db = db
        _day = args[0]

    default_rate = Decimal(str(default_rate))

    # Open a session if none provided and we can
    needs_close = False
    if _db is None:
        if SessionLocal is None:
            logger.warning("ensure_rate_or_default: no db available; returning default=%s", default_rate)
            return default_rate
        _db = SessionLocal()
        needs_close = True

    try:
        rate = get_rate_for_date(_db, _day, base=base, quote=quote)
        return rate if isinstance(rate, Decimal) else Decimal(str(rate))
    except Exception:
        logger.exception("ensure_rate_or_default: falling back to default=%s for %s", default_rate, _day)
        return default_rate
    finally:
        if needs_close and _db is not None:
            _db.close()


def usd_to_eur(
    amount_usd: Decimal | str | float,
    day: date,
    *,
    db: Optional[Session] = None,
) -> Decimal:
    """
    Backward-compatible helper: convert USD->EUR using the day’s (or prior business day’s) rate.
    Accepts optional `db`. If not provided, opens its own session via SessionLocal.
    """
    needs_close = False
    if db is None:
        if SessionLocal is None:
            logger.warning("usd_to_eur called without db and no SessionLocal available; returning amount unchanged.")
            return Decimal(str(amount_usd))
        db = SessionLocal()
        needs_close = True

    try:
        rate = get_rate_for_date(db, day, base="USD", quote="EUR")
        return (Decimal(str(amount_usd)) * rate).quantize(Decimal("0.00000001"))
    finally:
        if needs_close:
            db.close()
            

def usd_to_eur_strict(
    amount_usd: Decimal | str | float,
    day: date,
    *,
    db: Optional[Session] = None,
) -> Decimal:
    """
    Strict helper: convert USD->EUR and REFUSE silent fallback.

    - Uses get_rate_for_date (with weekend/holiday lookback).
    - If the only available rate path is the 1.0 fallback, it raises
      instead of silently accepting it.

    Intended for strict FX mode in production tax runs.
    """
    needs_close = False
    if db is None:
        if SessionLocal is None:
            raise RuntimeError(
                "usd_to_eur_strict: no DB session available and SessionLocal is not configured."
            )
        db = SessionLocal()
        needs_close = True

    try:
        lookup = get_rate_for_date_lookup(db, day, base="USD", quote="EUR", default_rate=Decimal("1.0"))
        rate_dec = lookup.rate if isinstance(lookup.rate, Decimal) else Decimal(str(lookup.rate))
        # In strict mode, refuse explicit fallback
        if lookup.used_fallback:
            raise ValueError(
                f"Strict FX mode: missing FX rate for {day.isoformat()} "
                "in fx_rates; import HNB/ECB FX CSV before running."
            )
        return (Decimal(str(amount_usd)) * rate_dec).quantize(Decimal("0.00000001"))
    finally:
        if needs_close:
            db.close()


def get_or_create_current_fx_batch_id(db: Optional[Session] = None) -> int:
    """
    Ensure an fx_batches row for today exists; return its id.
    Works with legacy tables (without `date`) by self-migrating.
    """
    sess, created_here = _maybe_session(db)
    if sess is None:
        logger.warning("get_or_create_current_fx_batch_id: SessionLocal unavailable; returning 1.")
        return 1

    try:
        # 1) Ensure table exists (legacy-compatible)
        sess.execute(text("""
            CREATE TABLE IF NOT EXISTS fx_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT
                -- legacy deployments may have only imported_at/source/rates_hash
                -- new deployments will have date/created_at too
            )
        """))

        # 2) Check columns; add any missing
        cols = [r[1] for r in sess.execute(text("PRAGMA table_info(fx_batches)")).fetchall()]  # r[1] = name
        needs_commit = False

        def _add_col(col_sql: str):
            nonlocal needs_commit
            sess.execute(text(f"ALTER TABLE fx_batches ADD COLUMN {col_sql}"))
            needs_commit = True

        if "date" not in cols:
            _add_col("date TEXT")
        if "created_at" not in cols:
            _add_col("created_at TEXT")
        if "imported_at" not in cols:
            # keep legacy compatibility (some code may read this)
            _add_col("imported_at TEXT")
        if "source" not in cols:
            _add_col("source TEXT")
        if "rates_hash" not in cols:
            _add_col("rates_hash TEXT")

        if needs_commit:
            try:
                sess.commit()
            except Exception:
                sess.rollback()

        # 3) Create a UNIQUE index on date if it doesn't exist yet (ignore errors if it exists)
        try:
            sess.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_fx_batches_date ON fx_batches(date)"))
            sess.commit()
        except Exception:
            sess.rollback()

        # 4) Upsert today's batch
        today = date.today().isoformat()
        rec = sess.execute(text("SELECT id FROM fx_batches WHERE date = :d"), {"d": today}).first()
        if rec:
            return int(rec[0])

        now_iso = datetime.now(timezone.utc).isoformat()
        sess.execute(
            text("""
                INSERT INTO fx_batches (date, created_at, imported_at, source)
                VALUES (:d, :ts, :ts, :s)
            """),
            {"d": today, "ts": now_iso, "s": "runtime-self-heal"}
        )
        sess.commit()
        rec = sess.execute(text("SELECT id FROM fx_batches WHERE date = :d"), {"d": today}).first()
        return int(rec[0]) if rec else 1

    except Exception:
        sess.rollback()
        logger.exception("get_or_create_current_fx_batch_id failed; returning 1 as fallback.")
        return 1
    finally:
        if created_here:
            sess.close()

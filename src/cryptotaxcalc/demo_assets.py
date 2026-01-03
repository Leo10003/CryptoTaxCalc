# src/cryptotaxcalc/demo_assets.py
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import os
from pathlib import Path
from typing import List

from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from .db import SessionLocal, engine as default_engine
from .models import Base, Transaction


# ---------- Helpers ----------

def _utc(dt: datetime) -> datetime:
    """Return timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _project_root() -> Path:
    """
    Resolve project root (…/CryptoTaxCalc). This module lives in src/cryptotaxcalc/.
    parents[0] = cryptotaxcalc, [1] = src, [2] = project root.
    """
    return Path(__file__).resolve().parents[2]


def _seed_rows(session) -> None:
    """
    Seed a minimal but meaningful dataset for the demo:
    - Two BUYs (ADA and ETH)
    - Two SELLs (disposals) so /calculate produces non-zero results
    """
    rows: List[Transaction] = [
        # BUYs
        Transaction(
            timestamp=_utc(datetime(2021, 4, 10, 10, 0, 0)),
            type="BUY",
            base_asset="ADA",
            base_amount=Decimal("100.0"),
            quote_asset="USDT",
            quote_amount=Decimal("50.0"),
            exchange="Binance",
            memo="Demo seed BUY (ADA)",
        ),
        Transaction(
            timestamp=_utc(datetime(2024, 4, 15, 10, 0, 0)),
            type="BUY",
            base_asset="ETH",
            base_amount=Decimal("0.25"),
            quote_asset="USDT",
            quote_amount=Decimal("550.0"),
            exchange="Binance",
            memo="Demo seed BUY (ETH)",
        ),
        # SELLs (disposals)
        Transaction(
            timestamp=_utc(datetime(2024, 4, 20, 10, 0, 0)),
            type="SELL",
            base_asset="ETH",
            base_amount=Decimal("0.25"),
            quote_asset="USDT",
            quote_amount=Decimal("699.0"),
            exchange="Binance",
            memo="Demo seed SELL (ETH) – ensures non-zero lots",
        ),
        Transaction(
            timestamp=_utc(datetime(2024, 4, 25, 10, 0, 0)),
            type="SELL",
            base_asset="ADA",
            base_amount=Decimal("50.0"),
            quote_asset="USDT",
            quote_amount=Decimal("40.0"),
            exchange="Binance",
            memo="Demo seed SELL (ADA) – ensures non-zero lots",
        ),
    ]

    session.add_all(rows)
    session.commit()


# ---------- Public API expected by demo_mode.py ----------

def is_demo_mode_enabled() -> bool:
    """
    Return True if DEMO_MODE environment variable is truthy.
    Accepted truthy values: '1', 'true', 'yes', 'on' (case-insensitive).
    """
    val = os.getenv("DEMO_MODE", "")
    return val.lower() in {"1", "true", "yes", "on"}


def ensure_demo_env(bind_engine=None) -> None:
    """
    Ensure demo environment is ready:
    - Create basic runtime directories if useful (e.g., logs/)
    - Create DB schema if missing
    - If DB is empty, seed demo rows (BUY+SELL)
    Idempotent and safe to call multiple times.
    """
    eng = bind_engine or default_engine

    # Optional: ensure a logs directory under project root (for demo assets)
    try:
        ( _project_root() / "logs" ).mkdir(parents=True, exist_ok=True)
    except Exception:
        # Non-fatal if we can't create this
        pass

    # Create tables if they don't exist
    Base.metadata.create_all(bind=eng)

    # Seed only if there are no transactions yet
    session = SessionLocal(bind=eng)
    try:
        has_any = session.execute(select(Transaction.id).limit(1)).first() is not None
        if not has_any:
            _seed_rows(session)
    except SQLAlchemyError:
        session.rollback()
        raise
    finally:
        session.close()

    # Light SQLite tune for smoother demo behavior (non-fatal if not supported)
    with eng.connect() as conn:
        try:
            conn.execute(text("PRAGMA journal_mode=WAL;"))
            conn.execute(text("PRAGMA synchronous=NORMAL;"))
        except Exception:
            pass


def reset_demo_db(bind_engine=None) -> None:
    """
    Fully reset the demo DB schema and seed with deterministic data.

    - Ensures clean state even if existing indexes/tables persist
    - Disposes pooled connections before DDL
    - Uses drop_all() + create_all() safely for SQLite
    - Seeds demo transactions for immediate calculation use
    """
    eng = bind_engine or default_engine

    # 1️⃣ Close pooled connections before schema changes
    try:
        eng.dispose()
    except Exception:
        pass

    # 2️⃣ Explicitly disable foreign_keys before dropping (SQLite quirk)
    with eng.connect() as conn:
        try:
            conn.execute(text("PRAGMA foreign_keys = OFF;"))
        except Exception:
            pass

    # 3️⃣ Drop and recreate schema in a clean transaction
    Base.metadata.drop_all(bind=eng)
    Base.metadata.create_all(bind=eng)

    # 4️⃣ Reseed minimal deterministic dataset
    session = SessionLocal(bind=eng)
    try:
        _seed_rows(session)
    except SQLAlchemyError as e:
        session.rollback()
        print(f"[Demo Reset] Seeding failed: {e}")
        raise
    finally:
        session.close()

    # 5️⃣ Light PRAGMA tuning for smoother demo behavior
    with eng.connect() as conn:
        try:
            conn.execute(text("PRAGMA foreign_keys = ON;"))
            conn.execute(text("PRAGMA journal_mode=WAL;"))
            conn.execute(text("PRAGMA synchronous=NORMAL;"))
        except Exception:
            pass

    print("[Demo Reset] Demo database successfully reset and reseeded.")

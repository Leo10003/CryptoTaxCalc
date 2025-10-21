# fx_utils.py
from __future__ import annotations
from decimal import Decimal
from datetime import date
from sqlalchemy.orm import Session
from .models import FxRate
from sqlalchemy import text
from .db import engine
import datetime

def usd_to_eur(amount_usd: Decimal, usd_per_eur: Decimal) -> Decimal:
    """
    Convert USD → EUR given a daily EURUSD (usd_per_eur).
    If EURUSD = 1.085, then 108.5 USD → 100 EUR (108.5 / 1.085).
    """
    if usd_per_eur <= 0:
        return Decimal("0")
    return (amount_usd / usd_per_eur).quantize(Decimal("0.00000001"))  # 8 dp for safety

def get_rate_for_date(session: Session, day: date) -> Decimal | None:
    """
    Return the best EURUSD rate for 'day'.
    If exact date missing, use the latest available date <= day (previous business day).
    """
    # exact match first
    row = session.query(FxRate).filter(FxRate.date == day).first()
    if row:
        return Decimal(row.usd_per_eur)

    # fallback: latest prior rate
    row = (
        session.query(FxRate)
        .filter(FxRate.date <= day)
        .order_by(FxRate.date.desc())
        .first()
    )
    return Decimal(row.usd_per_eur) if row else None

def get_or_create_current_fx_batch_id() -> int:
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM fx_batches ORDER BY id DESC LIMIT 1")).fetchone()
        if row:
            return row[0]
        now = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        res = conn.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t, :s, :h)"),
            dict(t=now, s="ECB CSV", h=None)
        )
        return res.lastrowid
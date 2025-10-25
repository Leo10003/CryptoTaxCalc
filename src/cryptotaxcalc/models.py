# models.py
"""
SQLAlchemy ORM models (database tables).
- Keep them separate from Pydantic schemas.
- We store numeric amounts as strings in SQLite to preserve exact values.
  (SQLite has no native Decimal type. We'll convert to Decimal in Python when needed.)
"""

from __future__ import annotations
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, DateTime, Integer, Index, Date, Text, Column, JSON, text, Numeric, ForeignKey, Enum, UniqueConstraint
from datetime import datetime, date as dt_date, timezone
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON
from typing import Any, Optional
import enum
from decimal import Decimal
from sqlalchemy.sql import func

# If your Base comes from db.py, keep using that
from .db import Base

class TransactionRow(Base):
    """
    Database representation of a transaction. Fields mirror schemas.Transaction.

    Fields:
      id: surrogate primary key (internal).
      timestamp, type, base_asset, base_amount: required.
      quote_asset, quote_amount, fee_asset, fee_amount, exchange, memo: optional.

    Important: amounts are strings for exactness in SQLite. Convert to Decimal in code.
    """
    __tablename__ = "transactions_row"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    base_amount: Mapped[str] = mapped_column(String(64), nullable=False)

    quote_asset: Mapped[str | None] = mapped_column(String(32), nullable=True)
    quote_amount: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fee_asset: Mapped[str | None] = mapped_column(String(32), nullable=True)
    fee_amount: Mapped[str | None] = mapped_column(String(64), nullable=True)
    exchange: Mapped[str | None] = mapped_column(String(64), nullable=True)
    memo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fair_value: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

class FxRate(Base):
    """
    Daily EUR↔USD rate from ECB (or equivalent).
    We store USD per 1 EUR (EURUSD). Example: on a day EURUSD=1.08500.
    To convert USD → EUR: amount_usd / usd_per_eur.
    """
    __tablename__ = "fx_rates"

    # One row per calendar day
    date: Mapped[dt_date] = mapped_column(Date, primary_key=True)
    usd_per_eur: Mapped[str] = mapped_column(String(32), nullable=False)
    batch_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

Index("idx_transaction_hash", TransactionRow.hash)

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CalcRun(Base):
    __tablename__ = "calc_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # lifecycle
    status: Mapped[str] = mapped_column(default="running", index=True)   # running|ok|error
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)

    # parameters / environment
    rule_version: Mapped[str | None] = mapped_column(nullable=True)
    lot_method: Mapped[str | None] = mapped_column(nullable=True)        # e.g., FIFO
    fx_set_id: Mapped[int | None] = mapped_column(nullable=True)

    params_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # integrity artifacts
    input_hash: Mapped[str | None] = mapped_column(nullable=True, index=True)
    output_hash: Mapped[str | None] = mapped_column(nullable=True, index=True)
    manifest_hash: Mapped[str | None] = mapped_column(nullable=True, index=True)
    manifest_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # persisted outputs (lightweight summary only; events stay on-demand)
    summary_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(nullable=True)

    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=text("(datetime('now'))"))


class CalcAudit(Base):
    __tablename__ = "calc_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    actor: Mapped[str] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)

    # ✅ Python type on the left; SQLAlchemy DB type on the right
    meta_json: Mapped[Optional[dict[str, Any]]] = mapped_column(SQLiteJSON, nullable=True)

    created_at: Mapped[str] = mapped_column(String)  # or DateTime if you prefer


class TxType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER = "TRANSFER"
    FEE = "FEE"
    INCOME = "INCOME"
    OTHER = "OTHER"


class RawEvent(Base):
    __tablename__ = "raw_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="manual")
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Transaction(Base):
    __tablename__ = "transactions"

    # Primary key (your code sorts by timestamp ASC, id ASC)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Event/tx identity (hash was referenced by prior queries and digests)
    hash: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    # Core business fields
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=False), index=True, nullable=False)
    type: Mapped[TxType] = mapped_column(Enum(TxType), nullable=False, index=True)

    base_asset: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    base_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(38, 18), nullable=True)

    quote_asset: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    quote_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(38, 18), nullable=True)

    fee_asset: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    fee_amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(38, 18), nullable=True)

    exchange: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    memo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Optional: store valuation used during calc
    fair_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(38, 18), nullable=True)

    # Raw event join (your code created index on raw_event_id)
    raw_event_id: Mapped[int | None] = mapped_column(
        ForeignKey("raw_events.id"), nullable=True, index=True
    )
    raw_event: Mapped["RawEvent"] = relationship("RawEvent", backref="transactions")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), default=datetime.utcnow, nullable=False)

    __table_args__ = (
        # Frequent lookups:
        Index("idx_transactions_ts", "timestamp"),
        Index("idx_transactions_type", "type"),
        Index("idx_transactions_exchange", "exchange"),
        Index("idx_transactions_raw_evt", "raw_event_id"),
        # If your data source guarantees unique (hash, exchange), keep it; otherwise you can remove.
        UniqueConstraint("hash", "exchange", name="uq_transactions_hash_exchange"),
    )

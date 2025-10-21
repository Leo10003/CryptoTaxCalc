# models.py
"""
SQLAlchemy ORM models (database tables).
- Keep them separate from Pydantic schemas.
- We store numeric amounts as strings in SQLite to preserve exact values.
  (SQLite has no native Decimal type. We'll convert to Decimal in Python when needed.)
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, DateTime, Integer, Index, Date, Text, Column
from datetime import datetime, date

class Base(DeclarativeBase):
    """Base class required by SQLAlchemy's ORM to register models."""
    pass


class TransactionRow(Base):
    """
    Database representation of a transaction. Fields mirror schemas.Transaction.

    Fields:
      id: surrogate primary key (internal).
      timestamp, type, base_asset, base_amount: required.
      quote_asset, quote_amount, fee_asset, fee_amount, exchange, memo: optional.

    Important: amounts are strings for exactness in SQLite. Convert to Decimal in code.
    """
    __tablename__ = "transactions"

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
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    usd_per_eur: Mapped[str] = mapped_column(String(32), nullable=False)
    batch_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

Index("idx_transaction_hash", TransactionRow.hash)
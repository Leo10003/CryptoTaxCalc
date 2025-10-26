from __future__ import annotations

import enum
from datetime import datetime, date as dt_date, timezone
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, DateTime, Integer, Date, Index
from .db import Base

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class TxType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER = "TRANSFER"
    AIRDROP = "AIRDROP"
    STAKING_REWARD = "STAKING_REWARD"

class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hash: Mapped[str | None] = mapped_column(String(64), unique=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)

    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    base_amount: Mapped[str] = mapped_column(String(64), nullable=False)

    quote_asset: Mapped[str | None] = mapped_column(String(32))
    quote_amount: Mapped[str | None] = mapped_column(String(64))
    fee_asset: Mapped[str | None] = mapped_column(String(32))
    fee_amount: Mapped[str | None] = mapped_column(String(64))
    exchange: Mapped[str | None] = mapped_column(String(64))
    memo: Mapped[str | None] = mapped_column(String(255))
    fair_value: Mapped[str | None] = mapped_column(String(64))
    raw_event_id: Mapped[int | None] = mapped_column(Integer)

Index("idx_transaction_hash", Transaction.hash)

class FxRate(Base):
    __tablename__ = "fx_rates"
    date: Mapped[dt_date] = mapped_column(Date, primary_key=True)
    usd_per_eur: Mapped[str] = mapped_column(String(32), nullable=False)
    batch_id: Mapped[int | None] = mapped_column(Integer)

TransactionRow = Transaction
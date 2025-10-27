from __future__ import annotations

import enum
from datetime import datetime, date as dt_date, timezone
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, DateTime, Integer, Date, Index, event, Column, Text, Enum, ForeignKey, Numeric, func
from .db import Base
from decimal import Decimal
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

class TxType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"
    STAKING_REWARD = "STAKING_REWARD"

class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    hash = Column(String, unique=True, index=True, nullable=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    type = Column(Enum(TxType), nullable=False)

    base_asset = Column(String, nullable=False)
    base_amount = Column(Numeric(28, 8, asdecimal=True), nullable=False)

    quote_asset = Column(String, nullable=True)
    quote_amount = Column(Numeric(28, 8, asdecimal=True), nullable=True)

    fee_asset = Column(String, nullable=True)
    fee_amount = Column(Numeric(28, 8, asdecimal=True), nullable=True)

    exchange = Column(String, nullable=True)
    memo = Column(Text, nullable=True)
    fair_value = Column(Numeric(28, 8, asdecimal=True), nullable=True)

    raw_event_id = Column(Integer, ForeignKey("raw_events.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

Index("idx_transaction_hash", Transaction.hash)

class FxRate(Base):
    __tablename__ = "fx_rates"
    date: Mapped[dt_date] = mapped_column(Date, primary_key=True)
    usd_per_eur: Mapped[str] = mapped_column(String(32), nullable=False)
    batch_id: Mapped[int | None] = mapped_column(Integer)

TransactionRow = Transaction

@event.listens_for(Transaction, "before_insert")
@event.listens_for(Transaction, "before_update")

class RunDigest(Base):
    __tablename__ = "run_digests"
    id = Column(Integer, primary_key=True)
    run_id = Column(String, index=True, nullable=False)
    input_hash = Column(String, nullable=True)
    output_hash = Column(String, nullable=True)
    manifest_json = Column(Text, nullable=True)  # <-- new column
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True)
    actor = Column(String, nullable=True)
    action = Column(String, nullable=True)
    target_type = Column(String, nullable=True)
    target_id = Column(Integer, nullable=True)
    details_json = Column(Text, nullable=True)
    meta_json = Column(Text, nullable=True)  # <-- new column
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

def convert_decimal_to_str(mapper, connection, target):
    """
    Converts Decimal fields to strings before saving to SQLite,
    since SQLite does not natively support the Decimal type.
    """
    for field in ("base_amount", "quote_amount", "fee_amount", "fair_value"):
        val = getattr(target, field, None)
        if isinstance(val, Decimal):
            setattr(target, field, str(val))
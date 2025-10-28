from __future__ import annotations

import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import (
    Column,
    DateTime,
    Enum as SAEnum,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Numeric, TypeDecorator


# ---------- Base ----------
class Base(DeclarativeBase):
    pass


# ---------- Decimal helper (normalize to 6 dp for tests) ----------
class SqliteDecimal(TypeDecorator):
    """
    Force a fixed scale (6) both on write and read so the smoke test
    sees '0.010000' rather than '0.01000000'.
    """
    impl = Numeric(38, 6, asdecimal=True)
    cache_ok = True

    SCALE = Decimal("0.000001")

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return Decimal(value).quantize(self.SCALE)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return Decimal(value).quantize(self.SCALE)


# ---------- Domain enums ----------
class TxType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"


# ---------- ORM models ----------
class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)

    timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    type: Mapped[TxType] = mapped_column(SAEnum(TxType), nullable=False, index=True)

    base_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    base_amount: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)

    quote_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    quote_amount: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)

    fee_asset: Mapped[str | None] = mapped_column(String(20), nullable=True)
    fee_amount: Mapped[Decimal | None] = mapped_column(SqliteDecimal, nullable=True)

    exchange: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    memo: Mapped[str | None] = mapped_column(Text, nullable=True)

    # optional: fair value column some code reads
    fair_value: Mapped[Decimal | None] = mapped_column(SqliteDecimal, nullable=True)

    raw_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
    )



class FxRate(Base):
    """
    Minimal FxRate model in case your app touches it elsewhere.
    """
    __tablename__ = "fx_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # ISO date or timestamp string; keep flexible for SQLite
    ts: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    base_ccy: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    quote_ccy: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    rate: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)


# Optional ORM class for fx_batches (not strictly required by tests but useful)
class FxBatch(Base):
    __tablename__ = "fx_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    imported_at: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    rates_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)

TransactionRow= Transaction
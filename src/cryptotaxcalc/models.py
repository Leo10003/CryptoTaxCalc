from __future__ import annotations
import datetime
from decimal import Decimal
from sqlalchemy import Column, Date, DateTime, Integer, String, Text, ForeignKey, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Numeric, TypeDecorator

# ---------- Base ----------
class Base(DeclarativeBase):
    pass

# ---------- Decimal helper (fixed 6 dp) ----------
class SqliteDecimal(TypeDecorator):
    impl = Numeric(38, 6, asdecimal=True)
    cache_ok = True
    SCALE = Decimal("0.000001")
    def process_bind_param(self, value, dialect):
        if value is None: return None
        return Decimal(value).quantize(self.SCALE)
    def process_result_value(self, value, dialect):
        if value is None: return None
        return Decimal(value).quantize(self.SCALE)

# ---------- ORM models ----------
class CalcRun(Base):
    __tablename__ = "calc_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[str] = mapped_column(String, nullable=False)
    jurisdiction: Mapped[str] = mapped_column(String, nullable=False)
    rule_version: Mapped[str] = mapped_column(String, nullable=False)
    lot_method: Mapped[str] = mapped_column(String, nullable=False)
    fx_set_id: Mapped[int] = mapped_column(Integer, nullable=False)
    params_json: Mapped[str] = mapped_column(String, nullable=False)
    # used by app.py after finishing FIFO
    finished_at: Mapped[str | None] = mapped_column(String, nullable=True)

class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)

    # Store as naive UTC datetimes in SQLite
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=False), nullable=False, index=True)
    # Use String to avoid Enum friction with CSV/parser
    type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    base_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    base_amount: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)

    # Make quote fields nullable to match import code paths
    quote_asset: Mapped[str | None] = mapped_column(String(20), nullable=True)
    quote_amount: Mapped[Decimal | None] = mapped_column(SqliteDecimal, nullable=True)

    fee_asset: Mapped[str | None] = mapped_column(String(20), nullable=True)
    fee_amount: Mapped[Decimal | None] = mapped_column(SqliteDecimal, nullable=True)

    exchange: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    memo: Mapped[str | None] = mapped_column(Text, nullable=True)
    fair_value: Mapped[Decimal | None] = mapped_column(SqliteDecimal, nullable=True)

    raw_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        default=lambda: datetime.datetime.utcnow(),
    )

Index("idx_transactions_ts", Transaction.timestamp)

# Fx rates aligned with /fx/upload (daily EURUSD)
class FxRate(Base):
    __tablename__ = "fx_rates"
    # Use the calendar date as the logical key
    date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    usd_per_eur: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)
    batch_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

class FxBatch(Base):
    __tablename__ = "fx_batches"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    imported_at: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    rates_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)

# Stores original uploads (provenance)
class RawEvent(Base):
    __tablename__ = "raw_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_filename: Mapped[str | None] = mapped_column(String(256), nullable=True)
    file_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    importer: Mapped[str | None] = mapped_column(String(64), nullable=True)
    received_at: Mapped[str | None] = mapped_column(String(32), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    blob_path: Mapped[str | None] = mapped_column(String(512), nullable=True)

# Persisted realized events per calculation run
class RealizedEvent(Base):
    __tablename__ = "realized_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    tx_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    asset: Mapped[str] = mapped_column(String(32), nullable=False)
    qty_sold: Mapped[str] = mapped_column(String(64), nullable=False)
    proceeds: Mapped[str] = mapped_column(String(64), nullable=False)
    cost_basis: Mapped[str] = mapped_column(String(64), nullable=False)
    gain: Mapped[str] = mapped_column(String(64), nullable=False)
    quote_asset: Mapped[str | None] = mapped_column(String(16), nullable=True)
    fee_applied: Mapped[str | None] = mapped_column(String(64), nullable=True)
    matches_json: Mapped[str | None] = mapped_column(Text, nullable=True)

class CalcAudit(Base):
    __tablename__ = "calc_audit"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

class RunDigest(Base):
    __tablename__ = "run_digests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True, unique=True)
    input_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    output_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manifest_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manifest_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

# Alias used by app.py
TransactionRow = Transaction

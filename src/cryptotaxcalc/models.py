from __future__ import annotations
"""
SQLAlchemy ORM models for CryptoTaxCalc.

Goals of optimization pass:
- Preserve schema and table compatibility 100%.
- Enforce deterministic Decimal precision (6dp).
- Ensure consistent UTC timestamps.
- Add docstrings and type hints for maintainability.
- Add lightweight indexing for frequently filtered columns.
"""

from decimal import Decimal, InvalidOperation
from enum import Enum
from datetime import datetime, date, timezone
from typing import Optional

from sqlalchemy import (
    Column, Date, DateTime, Integer, String, Text, Index, JSON, UniqueConstraint, Float, ForeignKey, 
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Numeric, TypeDecorator, String
from uuid import uuid4

# =========================================================
# Core helpers
# =========================================================

def utcnow() -> datetime:
    """Return a UTC datetime (naive for SQLite consistency)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _uuid_str() -> str:
    """Return a short UUID string."""
    return str(uuid4())


# =========================================================
# Enum
# =========================================================

class TxType(str, Enum):
    """Enumerated transaction types (mirrors CSV parser and schemas)."""
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    STAKE = "STAKE"
    REWARD = "REWARD"
    AIRDROP = "AIRDROP"
    FEE = "FEE"


# =========================================================
# Base declarative class
# =========================================================

class Base(DeclarativeBase):
    """Declarative SQLAlchemy base class."""
    pass


# =========================================================
# Custom Decimal type
# =========================================================

class SqliteDecimal(TypeDecorator):
    """
    SQLite-safe Decimal storage:
      - Bind as canonical string to avoid sqlite3 Decimal binding errors.
      - Return Decimal on reads.
    """
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, Decimal):
            # normalize and keep full scale from your app logic
            return format(value, 'f')
        try:
            return format(Decimal(value), 'f')
        except (InvalidOperation, TypeError, ValueError):
            return None

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        try:
            return Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            return None
        

# =========================================================
# ORM Tables
# =========================================================

class CalcRun(Base):
    """Metadata for a full calculation run."""
    __tablename__ = "calc_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Tax metadata
    jurisdiction: Mapped[str | None] = mapped_column(String(8), default="HR")
    rule_version: Mapped[str | None] = mapped_column(String(32), default="2025.1")
    tax_year: Mapped[int | None] = mapped_column(Integer, default=2025)

    lot_method: Mapped[str | None] = mapped_column(String(16))
    fx_set_id: Mapped[int | None] = mapped_column(Integer)
    params_json: Mapped[dict | None] = mapped_column(JSON)

    # Human-readable external run reference
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)

    input_hash: Mapped[str | None] = mapped_column(String(64))
    output_hash: Mapped[str | None] = mapped_column(String(64))
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    summary_json: Mapped[dict | None] = mapped_column(JSON)
    
    __table_args__ = (
        # One external run_id must correspond to exactly one CalcRun row.
        UniqueConstraint("run_id", name="uq_calc_runs_run_id"),
        # Speed up year/jurisdiction-based queries and reports.
        Index("ix_calc_runs_juris_year", "jurisdiction", "tax_year"),
    )


class Transaction(Base):
    """User-imported transaction events."""
    __tablename__ = "transactions"
    
    __table_args__ = (
        Index("ix_transactions_asset_ts", "base_asset", "timestamp"),
        Index("ix_transactions_type_ts", "type", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hash: Mapped[str | None] = mapped_column(String(128), index=True)

    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    base_asset: Mapped[str] = mapped_column(String(20), nullable=False)
    base_amount: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)

    quote_asset: Mapped[str | None] = mapped_column(String(20))
    quote_amount: Mapped[Decimal | None] = mapped_column(SqliteDecimal)

    fee_asset: Mapped[str | None] = mapped_column(String(20))
    fee_amount: Mapped[Decimal | None] = mapped_column(SqliteDecimal)

    exchange: Mapped[str | None] = mapped_column(String(64), index=True)
    memo: Mapped[str | None] = mapped_column(Text)
    fair_value: Mapped[Decimal | None] = mapped_column(SqliteDecimal)

    raw_event_id: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

Index("idx_transactions_ts", Transaction.timestamp)
Index("ix_transactions_asset_type", "base_asset", "type"),


class FXRate(Base):
    __tablename__ = "fx_rates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    base = Column(String, nullable=False, default="USD")
    quote = Column(String, nullable=False, default="EUR")
    rate = Column(Float, nullable=False)
    batch_id = Column(Integer, ForeignKey("fx_batches.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint("date", "base", "quote", name="uq_fx_rates_date_pair"),
        Index("ix_fx_rates_batch_date", "batch_id", "date"),
    )


class FxBatch(Base):
    """Batch metadata for FX imports."""
    __tablename__ = "fx_batches"
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Used by fx_utils.get_or_create_current_fx_batch_id (per-day batching)
    date = Column(String, nullable=True, index=True)        # ISO date (YYYY-MM-DD)
    created_at = Column(String, nullable=True)              # ISO timestamp
    
    imported_at = Column(String, nullable=False)   # ISO string is fine
    source = Column(String, nullable=True)
    rates_hash = Column(String, nullable=True)


class RawEvent(Base):
    """Stored metadata for original CSV uploads."""
    __tablename__ = "raw_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_filename: Mapped[str | None] = mapped_column(String(256))
    file_sha256: Mapped[str | None] = mapped_column(String(64))
    mime_type: Mapped[str | None] = mapped_column(String(64))
    importer: Mapped[str | None] = mapped_column(String(64))
    received_at: Mapped[str | None] = mapped_column(String(32))
    notes: Mapped[str | None] = mapped_column(Text)
    blob_path: Mapped[str | None] = mapped_column(String(512))


class RealizedEvent(Base):
    """Calculated disposal record (gain/loss per asset lot)."""
    __tablename__ = "realized_events"
    
    __table_args__ = (
        Index("ix_realized_events_run_asset_ts", "run_id", "asset", "timestamp"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    tx_id: Mapped[int | None] = mapped_column(Integer)
    timestamp: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(32), nullable=False)

    # Core numerics as Decimals (stored as text underneath via SqliteDecimal)
    qty_sold: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)
    proceeds: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)
    cost_basis: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)
    gain: Mapped[Decimal] = mapped_column(SqliteDecimal, nullable=False)

    quote_asset: Mapped[str | None] = mapped_column(String(16))
    fee_applied: Mapped[Decimal | None] = mapped_column(SqliteDecimal)
    matches_json: Mapped[str | None] = mapped_column(Text)


class CalcAudit(Base):
    """Audit trail for calculation runs."""
    __tablename__ = "calc_audit"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(Integer, index=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), index=True)


class RunDigest(Base):
    """Hashes and manifest summary for each completed run."""
    __tablename__ = "run_digests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True, unique=True)
    input_hash: Mapped[str | None] = mapped_column(String(64))
    output_hash: Mapped[str | None] = mapped_column(String(64))
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    manifest_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), index=True)


class AuditLog(Base):
    """Generic audit log for system-level events."""
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[int | None] = mapped_column(Integer)
    details_json: Mapped[str | None] = mapped_column(Text)
    ip: Mapped[str | None] = mapped_column(String(64))
    ts: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

class RunInput(Base):
    """Frozen input transaction hash set for a given run (stability for audit manifests)."""
    __tablename__ = "run_inputs"

    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("calc_runs.id"), primary_key=True)
    tx_hash: Mapped[str] = mapped_column(String(128), primary_key=True)

    __table_args__ = (
        Index("ix_run_inputs_run", "run_id"),
        Index("ix_run_inputs_hash", "tx_hash"),
    )

# =========================================================
# Aliases
# =========================================================

TransactionRow = Transaction

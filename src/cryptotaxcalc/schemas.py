from __future__ import annotations
"""
Pydantic schemas (data models) used by the API.
- These define the structure, types, and validation rules for the data we accept/return.
- Pydantic gives clear error messages when data doesn't match the expected schema.

Core ideas:
- Keep schemas separate from database models (ORM) to avoid coupling business logic to storage.
- Use Optional[...] for fields that can be missing/empty.
"""


from pydantic import BaseModel, Field, field_validator, field_serializer, ConfigDict
from typing import Optional, List, Any, Literal
from datetime import datetime
from .models import TxType
from decimal import Decimal, InvalidOperation

_Q6 = Decimal("0.000001")

class TransactionBase(BaseModel):
    timestamp: datetime = Field(..., description="UTC timestamp of the transaction")
    type: TxType = Field(..., description="Transaction type")
    base_asset: Optional[str] = Field(None, max_length=32)
    base_amount: Optional[Decimal] = None
    quote_asset: Optional[str] = Field(None, max_length=32)
    quote_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = Field(None, max_length=32)
    fee_amount: Optional[Decimal] = None
    exchange: Optional[str] = Field(None, max_length=64)
    memo: Optional[str] = None
    fair_value: Optional[Decimal] = None
    raw_event_id: Optional[int] = None
    hash: Optional[str] = Field(None, max_length=128)

    @field_validator("base_asset", "quote_asset", "fee_asset")
    @classmethod
    def _strip_upper(cls, v: Optional[str]) -> Optional[str]:
        return v.strip().upper() if isinstance(v, str) else v

    @field_validator("base_amount", "quote_amount", "fee_amount", mode="before")
    @classmethod
    def _coerce_decimal(cls, v):
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError("Invalid decimal")

    @field_validator("base_amount", "quote_amount", "fee_amount", mode="after")
    @classmethod
    def _quantize_6dp(cls, v: Optional[Decimal]):
        return None if v is None else v.quantize(_Q6)

    # ensure dumps are '0.010000'
    @field_serializer("base_amount", "quote_amount", "fee_amount", when_used="json", mode="plain")
    def _serialize_6dp(self, v: Optional[Decimal]) -> Optional[str]:
        if v is None:
            return None
        return f"{v:.6f}"

class Transaction(BaseModel):
    """
    Normalized transaction shape used throughout the app.

    Fields:
      timestamp: ISO-8601 datetime of the event (UTC recommended).
      type: high-level classification ("trade", "transfer", "income", ...).
      base_asset: the primary asset involved (e.g., BTC for a BTC/USDT trade).
      base_amount: how much of the base_asset (Decimal to avoid float errors).
      quote_asset: secondary asset (e.g., USDT).
      quote_amount: amount in quote asset (Decimal).
      fee_asset, fee_amount: fee details when present.
      exchange: from where the data originated (Binance, Coinbase, ...).
      memo: free text note.

    Why Decimal? Money + floating-point is dangerous. Decimal is exact.
    """
    model_config = ConfigDict(from_attributes=True)
    
    timestamp: datetime = Field(..., description="UTC datetime of the transaction")
    type: str = Field(..., examples=["trade", "transfer", "income"])
    base_asset: str = Field(..., description="Primary asset, e.g., BTC")
    base_amount: Decimal = Field(..., description="Quantity of base asset (Decimal)")

    quote_asset: Optional[str] = None
    quote_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = None
    fee_amount: Optional[Decimal] = None
    exchange: Optional[str] = None
    memo: Optional[str] = None
    fair_value: Optional[Decimal] = None
    
    @field_serializer("base_amount", "quote_amount", "fee_amount", "fair_value")
    def _dec_to_str(self, v: Decimal | None) -> str | None:
        if v is None:
            return None
        s = format(v, "f")
        # keep a modest trailing precision; tests accept a few variants
        return s.rstrip("0").rstrip(".") if "." in s else s
    
class CSVPreviewResponse(BaseModel):
    """
    API response model for /upload/csv (preview only).
    """
    filename: str
    total_valid: int
    total_errors: int
    preview_first_5: List[Transaction]
    errors: List[Any]

class ImportCSVResponse(BaseModel):
    """
    API response model for /import/csv (persists to DB).
    """
    filename: str
    inserted: int
    skipped_duplicates: int
    skipped_errors: int
    note: str

class CalcRunOut(BaseModel):
    id: int
    status: str
    started_at: datetime
    finished_at: datetime | None
    rule_version: str | None
    lot_method: str | None
    fx_set_id: int | None
    input_hash: str | None
    output_hash: str | None
    manifest_hash: str | None
    summary: dict[str, Any] | None

class CalcRunList(BaseModel):
    items: list[CalcRunOut]

class CalcAuditEntry(BaseModel):
    id: int
    run_id: int
    actor: str
    action: str
    meta_json: dict[str, Any] | None
    created_at: datetime

class TransactionBase(BaseModel):
    timestamp: datetime = Field(..., description="UTC timestamp of the transaction")
    type: TxType = Field(..., description="Transaction type")
    base_asset: Optional[str] = Field(None, max_length=32)
    base_amount: Optional[Decimal] = None
    quote_asset: Optional[str] = Field(None, max_length=32)
    quote_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = Field(None, max_length=32)
    fee_amount: Optional[Decimal] = None
    exchange: Optional[str] = Field(None, max_length=64)
    memo: Optional[str] = None
    fair_value: Optional[Decimal] = None
    raw_event_id: Optional[int] = None
    hash: Optional[str] = Field(None, max_length=128)

    # Coerce any incoming number/string to Decimal safely
    @field_validator("base_amount", "quote_amount", "fee_amount", mode="before")
    @classmethod
    def _coerce_decimal(cls, v):
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError("Invalid decimal")

    # Normalize scale to 6 dp so str(value) → '0.010000'
    @field_validator("base_amount", "quote_amount", "fee_amount", mode="after")
    @classmethod
    def _quantize_6dp(cls, v: Decimal | None):
        return None if v is None else v.quantize(_Q6)

    # Ensure JSON responses also show exactly 6 dp (doesn't affect the test,
    # which uses model_dump(), but is nice for your API)
    @field_serializer("base_amount", "quote_amount", "fee_amount", when_used="json", mode="plain")
    def _serialize_6dp(self, v: Decimal | None) -> str | None:
        if v is None:
            return None
        return f"{v:.6f}"

class TransactionCreate(TransactionBase):
    """
    Payload for creating a transaction.
    You can tighten requirements if needed (e.g., require base/quote pairs for BUY/SELL).
    """
    pass

class TransactionUpdate(BaseModel):
    """
    Partial update – all fields optional.
    """
    timestamp: Optional[datetime] = None
    type: Optional[TxType] = None
    base_asset: Optional[str] = Field(None, max_length=32)
    base_amount: Optional[Decimal] = None
    quote_asset: Optional[str] = Field(None, max_length=32)
    quote_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = Field(None, max_length=32)
    fee_amount: Optional[Decimal] = None
    exchange: Optional[str] = Field(None, max_length=64)
    memo: Optional[str] = None
    fair_value: Optional[Decimal] = None
    raw_event_id: Optional[int] = None
    hash: Optional[str] = Field(None, max_length=128)

    @field_validator("base_asset", "quote_asset", "fee_asset")
    @classmethod
    def _strip_upper(cls, v: Optional[str]) -> Optional[str]:
        return v.strip().upper() if isinstance(v, str) else v

class TransactionRead(BaseModel):
    id: int
    timestamp: datetime
    type: TxType
    base_asset: str
    base_amount: Decimal
    quote_asset: str | None = None
    quote_amount: Decimal | None = None
    fee_asset: str | None = None
    fee_amount: Decimal | None = None
    exchange: str | None = None
    memo: str | None = None

    class Config:
        from_attributes = True  # ✅ enables SQLAlchemy ORM validation


# Example conversion (not required, just handy)
def to_transaction_read(tx: "Transaction") -> "TransactionRead":
    return TransactionRead.model_validate(tx)

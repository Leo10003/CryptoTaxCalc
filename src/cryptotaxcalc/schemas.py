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
from decimal import Decimal, InvalidOperation

# Enum used in schemas (aligned with tests)
from enum import Enum


class TxType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    STAKE = "STAKE"
    REWARD = "REWARD"
    AIRDROP = "AIRDROP"
    FEE = "FEE"


_Q6 = Decimal("0.000001")


class TransactionBase(BaseModel):
    hash: Optional[str] = None
    timestamp: str
    type: str  # use string; we normalize to UPPER_SNAKE
    base_asset: str
    base_amount: str
    quote_asset: Optional[str] = None
    quote_amount: Optional[str] = None
    fee_asset: Optional[str] = None
    fee_amount: Optional[str] = None
    exchange: Optional[str] = None
    memo: Optional[str] = None
    fair_value: Optional[str] = None

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, v: str) -> str:
        if v is None:
            raise ValueError("type is required")
        s = str(v).strip().upper().replace("-", "_").replace(" ", "_")
        # allow letters, digits and underscore only
        import re

        if not re.fullmatch(r"[A-Z0-9_]+", s):
            raise ValueError(f"invalid transaction type: {v!r}")
        return s

    @field_validator("base_asset", "quote_asset", "fee_asset", mode="before")
    @classmethod
    def _upper_assets(cls, v):
        return None if v is None else str(v).strip().upper()

    @field_validator("base_amount", "quote_amount", "fee_amount", "fair_value", mode="before")
    @classmethod
    def _quantize_6dp(cls, v):
        if v is None or v == "":
            return None
        from decimal import Decimal, ROUND_HALF_UP

        d = Decimal(str(v))
        return str(d.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


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


class CalcConfig(BaseModel):
    jurisdiction: Literal["HR", "IT"] = "HR"
    rule_version: str = "2025.1"
    lot_method: Literal["FIFO"] = "FIFO"
    fx_source: Literal["HNB", "ECB"] = "HNB"  # HR=HNB, IT=ECB (defaults)
    holding_exemption_days: int | None = None  # e.g. HR: 730 for >2y exemption
    it_threshold_eur: Decimal | None = Decimal("51645.69")
    round_dp: int = 2


class RunTotals(BaseModel):
    proceeds_eur: Decimal = Decimal("0")
    cost_eur: Decimal = Decimal("0")
    gain_eur: Decimal = Decimal("0")
    taxable_gain_eur: Decimal = Decimal("0")


class RunSummary(BaseModel):
    run_id: int
    jurisdiction: str
    rule_version: str
    tax_year: int
    fx_batch_id: int | None = None
    lots_processed: int = 0
    totals: RunTotals


for model in list(globals().values()):
    if isinstance(model, type) and issubclass(model, BaseModel):
        try:
            model.model_rebuild()
        except Exception:
            pass

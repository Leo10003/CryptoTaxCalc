"""
Pydantic schemas (data models) used by the API.
- These define the structure, types, and validation rules for the data we accept/return.
- Pydantic gives clear error messages when data doesn't match the expected schema.

Core ideas:
- Keep schemas separate from database models (ORM) to avoid coupling business logic to storage.
- Use Optional[...] for fields that can be missing/empty.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime
from decimal import Decimal

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


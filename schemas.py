# schemas.py
# Pydantic models define and validate the structure of data we accept/return.

from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from datetime import datetime
from decimal import Decimal

class Transaction(BaseModel):
    """
    Our standard (normalized) transaction format.
    We'll start simple and expand later.
    """
    timestamp: datetime = Field(..., description="UTC datetime of the transaction")
    type: str = Field(..., examples=["trade", "transfer", "income"])
    base_asset: str
    base_amount: Decimal
    quote_asset: Optional[str] = None
    quote_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = None
    fee_amount: Optional[Decimal] = None
    exchange: Optional[str] = None
    memo: Optional[str] = None

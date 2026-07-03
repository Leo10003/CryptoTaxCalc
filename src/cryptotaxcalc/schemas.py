from __future__ import annotations
"""
Pydantic schemas (data models) used by the API.

Goals of this optimized pass:
- Preserve full backward compatibility with existing endpoints and DB usage.
- Ensure deterministic Decimal handling in JSON (stable string formatting).
- Normalize/canonize common fields (assets uppercased, types safe).
- Keep schemas independent of ORM models.
"""

from typing import Optional, List, Any, Literal
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum

from pydantic import BaseModel, Field, field_validator, field_serializer, ConfigDict


# =========================================================
# Enums / constants
# =========================================================

class TxType(str, Enum):
    # NOTE: Keep enum values as-is to avoid breaking existing responses/tests.
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    STAKE = "STAKE"
    REWARD = "REWARD"
    AIRDROP = "AIRDROP"
    FEE = "FEE"


# Default quantization for stringified numeric inputs on TransactionBase
_Q6 = Decimal("0.000001")


# =========================================================
# Helpers for safe numeric parsing/serialization
# =========================================================

def _to_decimal_or_none(v: Any) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None

def _q6_str(v: Any) -> Optional[str]:
    """
    Quantize inbound string-ish numbers to 6 dp for CSV preview / base payloads.
    Returns a string or None (kept for TransactionBase which is stringly-typed).
    """
    d = _to_decimal_or_none(v)
    if d is None:
        return None
    return str(d.quantize(_Q6, rounding=ROUND_HALF_UP))

def _dec_to_stable_str(d: Optional[Decimal]) -> Optional[str]:
    """
    Stable Decimal → string for JSON output:
    - No scientific notation
    - No trailing zeros after decimal point
    - Keep '0' or integer-like strings clean
    """
    if d is None:
        return None
    s = format(d, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


# =========================================================
# Base (stringly) Transaction payload for CSV/payload parsing
# =========================================================

class TransactionBase(BaseModel):
    hash: Optional[str] = None
    timestamp: str
    type: str  # normalized to UPPER_SNAKE; engine still accepts lowercase internally elsewhere
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
        # normalize incoming marker while allowing digits/underscore
        if v is None:
            raise ValueError("type is required")
        s = str(v).strip().upper().replace("-", "_").replace(" ", "_")
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
        # Keep as strings (to avoid early float coercion), but quantize consistently.
        return _q6_str(v)


# =========================================================
# Normalized Transaction used internally (Decimals & datetime)
# =========================================================

class Transaction(BaseModel):
    """
    Normalized transaction shape used throughout the app.

    Fields:
      timestamp: ISO-8601 datetime of the event (UTC recommended).
      type: high-level classification ("trade", "transfer", "income", ...).
      base_asset: primary asset (e.g., BTC for a BTC/USDT trade).
      base_amount: Decimal amount of the base asset.
      quote_asset / quote_amount: secondary leg (if present).
      fee_asset / fee_amount: fee details (if present).
      fair_value: optional Decimal fair value for income-like events.

    NOTE: We keep 'type' as str to stay compatible with existing DB/app logic,
          where the engine performs its own normalization.
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

    # Normalize asset tickers to uppercase for consistency at this layer too
    @field_validator("base_asset", "quote_asset", "fee_asset", mode="before")
    @classmethod
    def _upper_assets(cls, v):
        return None if v is None else str(v).strip().upper()

    # Ensure any non-Decimal numeric inputs are safely coerced
    @field_validator("base_amount", "quote_amount", "fee_amount", "fair_value", mode="before")
    @classmethod
    def _to_decimals(cls, v):
        d = _to_decimal_or_none(v)
        if v is not None and d is None:
            raise ValueError(f"Invalid decimal value: {v!r}")
        return d

    # Deterministic JSON output for Decimals
    @field_serializer("base_amount", "quote_amount", "fee_amount", "fair_value")
    def _dec_to_str(self, v: Decimal | None) -> str | None:
        return _dec_to_stable_str(v)


# =========================================================
# API shapes
# =========================================================

class CSVPreviewResponse(BaseModel):
    """Response for /upload/csv (preview only)."""
    filename: str
    total_valid: int
    total_errors: int
    preview_first_5: List[Transaction]
    errors: List[Any]

    # CSV source recognition (non-breaking additions)
    recognized_source_id: str | None = None
    recognized_source_name: str | None = None
    recognized_source_status: str | None = None  # "supported" | "unsupported"
    recognized_source_confidence: float | None = None
    recognized_source_signature: str | None = None



class ImportCSVResponse(BaseModel):
    """Response for /import/csv (persist to DB)."""
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
    (Stringly-typed variant used during CSV/import flows.)
    """
    pass


class TransactionUpdate(BaseModel):
    """Partial update – all fields optional."""
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

    @field_validator("base_amount", "quote_amount", "fee_amount", "fair_value", mode="before")
    @classmethod
    def _upd_to_decimals(cls, v):
        return _to_decimal_or_none(v)


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

    model_config = ConfigDict(from_attributes=True)

    @field_serializer("base_amount", "quote_amount", "fee_amount")
    def _dec_to_str(self, v: Decimal | None) -> str | None:
        return _dec_to_stable_str(v)


# Convenience converter (kept for compatibility)
def to_transaction_read(tx: "Transaction") -> "TransactionRead":
    return TransactionRead.model_validate(tx)


class CalcConfig(BaseModel):
    jurisdiction: str = "HR"
    rule_version: str = "2025.1"
    lot_method: Literal["FIFO"] = "FIFO"
    fx_source: Literal["HNB", "ECB"] = "HNB"  # HR=HNB, IT=ECB (defaults)
    holding_exemption_days: int | None = None  # e.g., HR: 730 for >2y exemption
    it_threshold_eur: Decimal | None = None
    round_dp: int = 2
    # Dashboard toggles
    include_tax_helpers: bool = True
    include_audit_appendix: bool = True
    strict_fx: bool = False
    
    @field_validator("jurisdiction")
    @classmethod
    def _normalize_jurisdiction(cls, v: str) -> str:
        s = str(v or "").strip().upper()
        if not s or len(s) != 2 or not s.isalpha():
            raise ValueError("jurisdiction must be a 2-letter country code (e.g., HR, IT, XX)")
        return s


class RunTotals(BaseModel):
    proceeds_eur: Decimal = Decimal("0")
    cost_eur: Decimal = Decimal("0")
    gain_eur: Decimal = Decimal("0")
    taxable_gain_eur: Decimal = Decimal("0")
    exempt_gain_eur: Decimal = Decimal("0")

    # Phase 3: filing-facing tax fields (still informational; not advice)
    taxable_base_eur: Decimal = Decimal("0")
    national_rate: Decimal = Decimal("0")
    local_rate: Decimal = Decimal("0")
    effective_rate: Decimal = Decimal("0")
    tax_due_eur: Decimal = Decimal("0")

    @field_serializer(
        "proceeds_eur",
        "cost_eur",
        "gain_eur",
        "taxable_gain_eur",
        "exempt_gain_eur",
        "taxable_base_eur",
        "national_rate",
        "local_rate",
        "effective_rate",
        "tax_due_eur",
    )
    def _dec_to_str(self, value: Decimal | None) -> str:
        """
        Stable, non-scientific string representation for totals.

        - Quantize to 8 decimal places for internal consistency.
        - Use the same stable formatter as other schemas so we never
          leak '5.25E+4'-style notation into the API.
        """
        if value is None:
            return "0"
        q = value.quantize(Decimal("0.00000001"))
        s = _dec_to_stable_str(q)  # uses format(d, "f") and strips trailing zeros
        return s or "0"


class RunSummary(BaseModel):
    run_id: int
    jurisdiction: str
    rule_version: str
    tax_year: int
    fx_batch_id: int | None = None
    # FX mode used for this run (effective after environment enforcement).
    strict_fx: bool | None = None
    strict_fx_source: str | None = None  # "cfg" | "prod_enforced" | "disabled"
    fx_fallback_pairs: list[str] | None = None

    # FX transparency (explicit; never inferred from rate values)
    fx_fallback_used: bool | None = None
    fx_fallback_days_count: int | None = None
    fx_fallback_days_sample: list[str] | None = None
    
    # Trust metadata: surfaced in UI to explain integrity and next steps.
    fx_context: dict[str, Any] | None = None
    fee_valuation: dict[str, Any] | None = None

    lots_processed: int = 0
    totals: RunTotals
    warnings: list[str | dict[str, Any]] = Field(default_factory=list)
    timings_ms: dict[str, int] | None = None


# Rebuild models (safe no-ops if already built)
for model in list(globals().values()):
    if isinstance(model, type) and issubclass(model, BaseModel):
        try:
            model.model_rebuild()
        except Exception:
            pass

class PrecheckAssetIssue(BaseModel):
    asset: str
    first_sell_ts: str | None = None
    total_sell_qty: str
    reason: str
    guidance: str | None = None


class PrecheckFileIssue(BaseModel):
    filename: str
    issues_detected: bool
    assets: list[PrecheckAssetIssue] = Field(default_factory=list)


class PrecheckResponse(BaseModel):
    issues_detected: bool
    assets: list[PrecheckAssetIssue] = Field(default_factory=list)
    files: list[PrecheckFileIssue] = Field(default_factory=list)

class WalletOutItem(BaseModel):
    transaction_id: int
    timestamp: str
    asset: str
    amount: str
    exchange: str | None = None


class WalletTransferOverrideRequest(BaseModel):
    classification: Literal["transfer", "sell", "buy"]
    proceeds_eur: str | None = None
    note: str | None = None
    
class WalletTransferRow(BaseModel):
    transaction_id: int
    raw_event_id: int
    filename: str

    timestamp: str
    asset: str
    amount: str

    # persisted from Ledger: fair_value + memo contains cv_ticker=EUR/USD
    fair_value: str | None = None
    cv_ticker: str | None = None

    # current classification
    classification: Literal["transfer", "sell", "buy"] = "transfer"
    proceeds_eur: str | None = None

    # suggestion (auto-fill)
    suggested_proceeds_eur: str | None = None


class WalletTransferFileGroup(BaseModel):
    raw_event_id: int
    filename: str
    ins: list[WalletTransferRow] = Field(default_factory=list)
    outs: list[WalletTransferRow] = Field(default_factory=list)


class WalletTransferBatchItem(BaseModel):
    transaction_id: int
    classification: Literal["transfer", "sell", "buy"]
    proceeds_eur: str | None = None
    note: str | None = None


class WalletTransferBatchRequest(BaseModel):
    raw_event_id: int
    items: list[WalletTransferBatchItem] = Field(default_factory=list)

class WalletOutRow(BaseModel):
    transaction_id: int
    raw_event_id: int | None = None
    filename: str | None = None

    timestamp: str
    asset: str
    amount: str

    # persisted from Ledger countervalue via fair_value + memo cv_ticker
    fair_value: str | None = None
    cv_ticker: str | None = None

    # current override state (if any)
    classification: Literal["transfer", "taxable"] = "transfer"
    proceeds_eur: str | None = None

    # server suggestion (auto-fill)
    suggested_proceeds_eur: str | None = None


class WalletOutGroup(BaseModel):
    raw_event_id: int
    filename: str
    rows: list[WalletOutRow] = Field(default_factory=list)


class WalletOutBatchItem(BaseModel):
    transaction_id: int
    classification: Literal["transfer", "taxable"]
    proceeds_eur: str | None = None
    note: str | None = None


class WalletOutBatchRequest(BaseModel):
    raw_event_id: int
    items: list[WalletOutBatchItem] = Field(default_factory=list)

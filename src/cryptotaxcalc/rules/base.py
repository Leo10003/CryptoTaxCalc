from __future__ import annotations
"""
Base interfaces and helpers for tax rule implementations (HR, IT, etc.)

Each country-specific rule module (e.g. hr.py, it.py) must implement a class
that conforms to the TaxRule protocol defined here.

This base layer provides:
- Common dataclasses (Match, RunContext)
- Shared helpers for EUR rounding and taxable gain normalization
- Optional logging hooks for consistent diagnostics
"""

from typing import Protocol
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
from datetime import datetime

from cryptotaxcalc.models import TransactionRow
from cryptotaxcalc.schemas import CalcConfig
from cryptotaxcalc.logging_setup import get_logger

logger = get_logger("rules.base")


# =========================================================
# Core dataclasses
# =========================================================

@dataclass(slots=True)
class Match:
    """Represents a realized match between a disposal and an acquisition lot."""
    qty: Decimal
    proceeds_eur: Decimal
    cost_eur: Decimal
    # Optional timestamps for HR long-term exemption (and future rules)
    buy_ts: "datetime | None" = None
    sell_ts: "datetime | None" = None

    @property
    def gain_eur(self) -> Decimal:
        """Computed realized gain/loss for this match."""
        return (self.proceeds_eur - self.cost_eur).quantize(Decimal("0.00000001"))


@dataclass(slots=True)
class RunContext:
    """Context shared with every tax rule call."""
    cfg: CalcConfig
    tax_year: int

    def round_eur(self, value: Decimal) -> Decimal:
        """Round according to configuration (default 2dp)."""
        try:
            dp = getattr(self.cfg, "round_dp", 2) or 2
        except Exception:
            dp = 2
        quant = Decimal("1").scaleb(-dp)
        return value.quantize(quant, rounding=ROUND_HALF_UP)


# =========================================================
# TaxRule protocol
# =========================================================

class TaxRule(Protocol):
    """
    Country-specific tax rule interface.

    Implementations must handle:
      - Identifying taxable disposals (is_taxable_disposal)
      - Applying any exemptions or threshold reductions (apply_exemptions)
      - Finalizing taxable gain (finalize_taxable_gain)
    """

    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        """
        Return True if the transaction represents a taxable disposal event
        (e.g., SELL, TRADE, SWAP, etc.) for the given jurisdiction.
        """
        ...

    def apply_exemptions(
        self,
        matches: list[Match],
        tx: TransactionRow,
        ctx: RunContext,
    ) -> list[Match]:
        """
        Apply jurisdiction-specific exemptions to a list of realized matches.
        Returns a new list (may adjust qty/proceeds/cost or drop some matches).
        """
        ...

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        """
        Apply rounding and any final adjustments (e.g. thresholds, limits).
        """
        ...


# =========================================================
# Shared helper utilities
# =========================================================

def normalize_gain(gain: Decimal, ctx: RunContext) -> Decimal:
    """
    Normalize a realized gain according to the rule context.
    - Ensures deterministic rounding.
    - Logs extreme or suspicious values for diagnostics.
    """
    if not isinstance(gain, Decimal):
        try:
            gain = Decimal(str(gain))
        except Exception:
            logger.warning(f"normalize_gain received invalid value: {gain!r}")
            return Decimal("0")

    rounded = ctx.round_eur(gain)
    if abs(rounded) > Decimal("1e9"):
        logger.warning(f"Unusually large gain detected: {rounded}")
    return rounded

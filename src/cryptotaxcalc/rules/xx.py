from __future__ import annotations

from decimal import Decimal

from cryptotaxcalc.models import TransactionRow
from cryptotaxcalc.rules.base import Match, RunContext, TaxRule, normalize_gain


class XxRule(TaxRule):
    """
    Jurisdiction: XX
    Minimal baseline rule:
      - Taxable disposals: SELL/TRADE/SWAP/CONVERT
      - No exemptions
      - Taxable gain = rounded gain (deterministic)
    """

    rule_version = "XX-0.1"

    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        t = (getattr(tx, "type", "") or "").strip().upper()
        return t in {"SELL", "TRADE", "SWAP", "CONVERT"}

    def apply_exemptions(
        self,
        matches: list[Match],
        tx: TransactionRow,
        ctx: RunContext,
    ) -> list[Match]:
        # No exemptions in baseline XX rule.
        return matches

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        # Deterministic rounding and normalization.
        return normalize_gain(gain_eur, ctx)

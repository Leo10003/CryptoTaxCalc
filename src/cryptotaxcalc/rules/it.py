from __future__ import annotations
from decimal import Decimal
from .base import TaxRule, Match, RunContext
from cryptotaxcalc.models import TransactionRow

class ItRule(TaxRule):
    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        t = (tx.type or "").upper()
        return t in {"SELL"}  # expand as you map events (goods/services etc.)

    def apply_exemptions(self, matches: list[Match], tx: TransactionRow, ctx: RunContext) -> list[Match]:
        # Phase-1: threshold gating at finalize step; leave per-lot gains untouched here
        return matches

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        thr = ctx.cfg.it_threshold_eur or Decimal("51645.69")
        # Phase-1: simple gate: if under threshold, tax = 0
        # Later: implement proper “annual average balance” check
        return Decimal("0") if thr and thr > 0 else gain_eur

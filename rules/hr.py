from __future__ import annotations
from decimal import Decimal
from datetime import datetime, timezone
from .base import TaxRule, Match, RunContext
from cryptotaxcalc.models import TransactionRow

TWO_YEARS_DAYS = 730


def _held_days(tx_buy_ts: datetime, tx_sell_ts: datetime) -> int:
    a = tx_buy_ts.replace(tzinfo=timezone.utc)
    b = tx_sell_ts.replace(tzinfo=timezone.utc)
    return max(0, (b - a).days)


class HrRule(TaxRule):
    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        t = (tx.type or "").upper()
        return t in {"SELL"} or (
            t not in {"TRANSFER_IN", "TRANSFER_OUT"} and tx.quote_asset not in (None, "", "NULL")
        )

    def apply_exemptions(
        self, matches: list[Match], tx: TransactionRow, ctx: RunContext
    ) -> list[Match]:
        days = ctx.cfg.holding_exemption_days or TWO_YEARS_DAYS
        # If you track buy timestamps per match, you can zero out gains for >days lots.
        # For Phase-1 placeholder, return matches unchanged (wire actual logic with fifo metadata).
        return matches

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        return gain_eur  # HR: no additional postprocessing beyond exemptions

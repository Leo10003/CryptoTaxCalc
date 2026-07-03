from __future__ import annotations
"""
Italy (IT) tax rule implementation.

Implements the TaxRule interface for Italian jurisdiction:
- Taxable on disposals (SELL)
- Threshold exemption: year-aware (e.g., €2,000 for tax years ≤ 2024; no exemption from 2025)
  and optionally overrideable via config for testing.
- Future extension: apply annual average balance test (“giacenza media”)
"""

from decimal import Decimal
from typing import List

from cryptotaxcalc.rules.base import TaxRule, Match, RunContext, normalize_gain
from cryptotaxcalc.models import TransactionRow
from cryptotaxcalc.logging_setup import get_logger

logger = get_logger("rules.it")


class ItRule(TaxRule):
    """Italy-specific capital gain rule set."""

    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        """
        True if the transaction is a taxable disposal.

        For Italy we treat as taxable:
          - Explicit SELL events
          - Asset-to-asset trades (TRADE / SWAP)

        This matches the usual treatment where swapping one crypto
        for another is a disposal of the asset being given up.
        """
        t = (tx.type or "").upper()
        taxable = t in {"SELL", "TRADE", "SWAP"}
        if taxable:
            logger.debug(f"[IT] Detected taxable disposal type={t} id={getattr(tx, 'id', '?')}")
        return taxable

    def apply_exemptions(
        self, matches: List[Match], tx: TransactionRow, ctx: RunContext
    ) -> List[Match]:
        """
        Italy currently has no per-lot exemption; threshold is applied globally.
        This function simply returns the list unchanged.
        """
        return matches

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        """
        Italy – threshold model (simplified, deterministic).

        Rules:
          - Losses (<= 0) always reduce taxable base.
          - If cfg.it_threshold_eur is set and > 0:
              gains <= threshold -> taxable = 0
              gains > threshold  -> taxable = gain
          - Else default year-aware threshold:
              tax_year <= 2024 -> threshold = €2,000
              tax_year >= 2025 -> no threshold
        """
        gain_rounded = normalize_gain(gain_eur, ctx)

        # Losses always reduce taxable base
        if gain_rounded <= 0:
            logger.debug(f"[IT] Loss/non-positive gain taxable (reduces base), rounded={gain_rounded}")
            return gain_rounded

        # Config override (kept for UI/tests)
        thr: Decimal | None = None
        thr_raw = getattr(getattr(ctx, "cfg", None), "it_threshold_eur", None)
        try:
            if thr_raw is not None and str(thr_raw).strip() != "":
                thr = Decimal(str(thr_raw))
        except Exception:
            thr = None

        # Default year-aware threshold (only if override not set)
        if thr is None:
            try:
                tax_year = int(getattr(ctx, "tax_year", 0) or 0)
            except Exception:
                tax_year = 0

            if tax_year and tax_year <= 2024:
                thr = Decimal("2000")
            else:
                thr = None

        if thr is not None and thr > 0 and gain_rounded <= thr:
            logger.debug(f"[IT] Gain under threshold: taxable=0 (gain={gain_rounded} thr={thr})")
            return Decimal("0")

        return gain_rounded

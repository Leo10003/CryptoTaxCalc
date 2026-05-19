from __future__ import annotations
"""
Croatia (HR) tax rule implementation.

Implements the TaxRule interface for Croatian jurisdiction:
- Taxable on disposals (SELL or trade that yields EUR or another asset)
- Exemption: if held longer than 2 years (default 730 days) — configurable
"""

from decimal import Decimal
from datetime import datetime, timezone
from typing import List, Dict

from cryptotaxcalc.rules.base import TaxRule, Match, RunContext, normalize_gain
from cryptotaxcalc.models import TransactionRow
from cryptotaxcalc.logging_setup import get_logger

logger = get_logger("rules.hr")

# Default exemption duration (in days)
TWO_YEARS_DAYS = 730


# =========================================================
# Utility helpers
# =========================================================

def _held_days(tx_buy_ts: datetime, tx_sell_ts: datetime) -> int:
    """
    Return holding period (days) between acquisition and disposal timestamps.
    Negative or missing values are clamped to 0.
    """
    if not tx_buy_ts or not tx_sell_ts:
        return 0
    a = tx_buy_ts.replace(tzinfo=timezone.utc)
    b = tx_sell_ts.replace(tzinfo=timezone.utc)
    return max(0, (b - a).days)


# =========================================================
# Croatian Rule
# =========================================================

class HrRule(TaxRule):
    """Croatia-specific capital gain rule set."""

    def is_taxable_disposal(self, tx: TransactionRow) -> bool:
        """
        True if the transaction represents a taxable disposal.

        Croatia: taxable on disposals of the asset being given up.
        In our normalized model that typically means:
          - SELL / TRADE / SWAP
        Non-taxable / ignored:
          - BUY (acquisition)
          - TRANSFER variants (movement between wallets/exchanges)
          - INCOME (acquisition)
        """
        t = (tx.type or "").strip().lower()

        # Transfers are not disposals
        if t in {"transfer", "transfer_in", "transfer_out"}:
            return False

        # Acquisitions are not disposals
        if t in {"buy", "income", "purchase", "spot_buy"}:
            return False

        # Disposals / swaps
        if t in {"sell", "trade", "swap", "spot_sell"}:
            return True

        return False

    def apply_exemptions(
        self, matches: List[Match], tx: TransactionRow, ctx: RunContext
    ) -> List[Match]:
        """
        Apply Croatia's long-term holding exemption.
        - If held > N days (default 730): make the gain zero for that matched portion.
        - Requires Match.buy_ts/Match.sell_ts to be set by the lot-matcher when available.
        - If timestamps are missing, we leave the match unchanged (fail-open).
        """
        days_threshold = ctx.cfg.holding_exemption_days or TWO_YEARS_DAYS
        adjusted: List[Match] = []
        exempted_qty = Decimal("0")

        for m in matches:
            # If timestamps are missing, keep as-is
            if not (m.buy_ts and m.sell_ts):
                adjusted.append(m)
                continue

            held_days = _held_days(m.buy_ts, m.sell_ts)
            if held_days > days_threshold:
                # Zero-out gain by setting cost to proceeds for this match
                adjusted.append(Match(
                    qty=m.qty,
                    proceeds_eur=m.proceeds_eur,
                    cost_eur=m.proceeds_eur,
                    buy_ts=m.buy_ts,
                    sell_ts=m.sell_ts,
                ))
                exempted_qty += m.qty
            else:
                adjusted.append(m)

        if exempted_qty:
            logger.info(f"[HR] Exempted gain on {exempted_qty} units (> {days_threshold} days held).")

        return adjusted

    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal:
        """
        Final rounding and postprocessing of realized gain for HR.
        Currently:
          - Applies deterministic rounding
          - Logs anomalies
        """
        rounded = normalize_gain(gain_eur, ctx)
        logger.debug(f"[HR] Final taxable gain rounded: {rounded}")
        return rounded

    def split_taxable_exempt_gain(
        self,
        *,
        gain_eur: Decimal,
        matches_raw: List[Dict],
        ctx: RunContext,
    ) -> tuple[Decimal, Decimal]:
        """
        Compute taxable vs exempt gain for HR.

        This is the canonical HR rule entrypoint used by the EUR-canonical
        calculation runner:

        - Uses per-lot holding-period exemption (default 730 days, configurable).
        - `matches_raw` is the EUR match payload produced by calc_runner, each
          element containing: proceeds_eur, cost_eur, acquired_at, disposed_at.
        """
        _total_gain, taxable_gain, exempt_gain = compute_taxable_gain_for_matches_hr(
            matches_raw=matches_raw,
            ctx=ctx,
            fx_rate=Decimal("1"),
        )

        taxable_gain = self.finalize_taxable_gain(taxable_gain, ctx)

        # Reconcile to the FIFO total (gain_eur) to avoid drift from allocation rounding.
        exempt_gain = gain_eur - taxable_gain
        return taxable_gain, exempt_gain

ISO = "%Y-%m-%dT%H:%M:%S"

def _parse_ts(ts: str) -> datetime:
    # Accepts 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD'
    if "T" in ts:
        return datetime.fromisoformat(ts.replace("Z", "")).replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(ts + "T00:00:00").replace(tzinfo=timezone.utc)

def compute_taxable_gain_for_matches_hr(
    matches_raw: List[Dict],
    ctx: "RunContext",
    *,
    fx_rate: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    HR-only helper used by calc_runner.run_calculation.

    Given a list of per-lot matches (already expressed in EUR),
    split total gain into:

      - total_gain_eur   (sum of all gains/losses)
      - taxable_gain_eur (held <= holding_exemption_days)
      - exempt_gain_eur  (held >  holding_exemption_days)

    Per-match structure expected in matches_raw:
      {
        "proceeds_eur": "123.45",
        "cost_eur":     "100.00",
        "acquired_at":  "2023-01-10T12:00:00"   # ISO string
        "disposed_at":  "2025-03-01T09:30:00"   # ISO string
      }
    """

    # HR config: how many days you must hold to be exempt.
    # Read from ctx.cfg.holding_exemption_days, falling back to the 2-year default.
    threshold_days = getattr(getattr(ctx, "cfg", None), "holding_exemption_days", None)
    if not threshold_days or threshold_days <= 0:
        threshold_days = TWO_YEARS_DAYS

    total_gain = Decimal("0")
    taxable_gain = Decimal("0")
    exempt_gain = Decimal("0")

    for m in matches_raw or []:
        # ---- 1) Parse numbers safely -----------------------------------------
        try:
            proceeds_eur = Decimal(str(m.get("proceeds_eur", "0")))
            cost_eur = Decimal(str(m.get("cost_eur", "0")))
        except Exception:
            # If we cannot parse, skip this match rather than break the run.
            continue

        gain = proceeds_eur - cost_eur
        total_gain += gain

        # ---- 2) Losses: always fully taxable (as negative) -------------------
        # Exemption is a benefit on positive gains; losses reduce taxable base.
        if gain <= 0:
            taxable_gain += gain
            continue

        # ---- 3) Compute holding period in days -------------------------------
        acquired_at = m.get("acquired_at")
        disposed_at = m.get("disposed_at")
        held_days = None

        if isinstance(acquired_at, str) and isinstance(disposed_at, str):
            try:
                buy_ts = datetime.fromisoformat(acquired_at.replace("Z", "+00:00"))
                sell_ts = datetime.fromisoformat(disposed_at.replace("Z", "+00:00"))
                held_days = max((sell_ts - buy_ts).days, 0)
            except Exception:
                held_days = None

        # ---- 4) Classify this gain piece -------------------------------------
        # If we don't know the holding period, be conservative: taxable.
        if threshold_days <= 0 or held_days is None:
            taxable_gain += gain
        elif held_days > threshold_days:
            # Long-term → exempt (HR 2-year rule)
            exempt_gain += gain
        else:
            # Short-term → taxable
            taxable_gain += gain

    # No rounding here; caller (RunTotals/serialization) already handles
    # Decimal → string / dp. We keep full precision.
    return total_gain, taxable_gain, exempt_gain
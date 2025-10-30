# fifo_engine.py
"""
Deterministic FIFO cost-basis calculator.

Goal (MVP):
- Track lots per asset using FIFO (first-in, first-out).
- Produce realized gain/loss events when an asset is SOLD (via a 'trade').
- Ignore 'transfer' for tax purposes (no gain).
- Treat 'income' as a new lot with ZERO cost basis (MVP simplification).
  NOTE: In real tax law, income is taxed at receipt and becomes basis; we don't
  have fair-value-at-receipt yet, so we set basis=0 and emit a warning.

Assumptions (MVP to keep logic clean and auditable):
- A row with type == "trade" means you're SELLING `base_asset` for `quote_asset`.
  Proceeds are denominated in the quote_asset.
- Fees reduce proceeds only when fee_asset == quote_asset (simple & conservative).
- If you sell more than your available lots (short position), we assume zero basis
  for the missing portion and emit a warning.

Design:
- This file is *pure logic* (no DB calls). Give it a list[Transaction]; get back
  (events, summary, warnings). That makes it easy to test and evolve.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import List, Dict, Any, Tuple
from .schemas import Transaction

# Use sufficient precision for money math (increase if you need sub-satoshi granularity).
getcontext().prec = 28


@dataclass
class Lot:
    """
    Represents an acquisition lot for an asset.
    - qty_remaining: how much is still available to be sold
    - cost_per_unit: basis per unit in the quote currency of future proceeds
                     (MVP note: we don't convert currencies here; we keep basis
                     in the same currency as when created; for income it's 0).
    """

    qty_remaining: Decimal
    cost_per_unit: Decimal  # basis/unit in *proceeds currency terms*, MVP=0 for income


@dataclass
class Match:
    """
    How a sell matched against a specific lot.
    """

    from_qty: Decimal  # quantity taken from that lot
    lot_cost_per_unit: Decimal
    lot_cost_total: Decimal


@dataclass
class Realization:
    """
    A realized gain/loss event produced by selling an asset (type='trade').
    """

    timestamp: str
    asset: str
    qty_sold: Decimal
    proceeds: Decimal  # in quote asset units
    cost_basis: Decimal  # same currency as proceeds
    gain: Decimal
    quote_asset: str
    fee_applied: Decimal  # fee deducted from proceeds (when fee_asset == quote_asset)
    matches: List[Match] = field(default_factory=list)


def _is_stable(symbol: str | None) -> bool:
    """Simple helper, used only for warnings/messages."""
    return symbol is not None and symbol.upper() in {"USDT", "USDC", "EUR", "USD", "EURT"}


def compute_fifo(
    transactions: List[Transaction],
) -> Tuple[List[Realization], Dict[str, Any], List[str]]:
    """
    Core FIFO engine (enhanced):
      - BUY: create lot with non-zero basis from quote fields.
      - TRADE (sell): consume lots FIFO; proceeds = quote_amount - fee(if fee in quote).
      - INCOME: create lot; if quote_amount given, use it as fair value for basis; else basis=0 (warn).
      - TRANSFER: ignored for tax events.

    Limitations (MVP):
      - No FX conversion. Proceeds/basis/gain are in the quote asset’s units.
      - Fees only reduce proceeds when fee_asset == quote_asset.
    """
    txs = sorted(transactions, key=lambda t: t.timestamp)
    lots: Dict[str, List[Lot]] = {}
    events: List[Realization] = []
    warnings: List[str] = []

    def add_lot(asset: str, qty: Decimal, cost_per_unit: Decimal) -> None:
        if qty <= 0:
            return
        lots.setdefault(asset, []).append(Lot(qty_remaining=qty, cost_per_unit=cost_per_unit))

    def consume_lots(asset: str, qty_to_sell: Decimal) -> Tuple[Decimal, List[Match], List[str]]:
        cost_total = Decimal("0")
        matches: List[Match] = []
        local_warnings: List[str] = []
        remaining = qty_to_sell
        asset_lots = lots.get(asset, [])

        i = 0
        while remaining > 0 and i < len(asset_lots):
            lot = asset_lots[i]
            take = min(lot.qty_remaining, remaining)
            if take > 0:
                lot_cost = lot.cost_per_unit * take
                matches.append(
                    Match(
                        from_qty=take, lot_cost_per_unit=lot.cost_per_unit, lot_cost_total=lot_cost
                    )
                )
                cost_total += lot_cost
                lot.qty_remaining -= take
                remaining -= take
            if lot.qty_remaining == 0:
                i += 1

        if remaining > 0:
            local_warnings.append(
                f"Selling {qty_to_sell} {asset} but only {qty_to_sell - remaining} available in lots. "
                f"Assuming zero basis for {remaining} {asset}."
            )
            matches.append(
                Match(
                    from_qty=remaining, lot_cost_per_unit=Decimal("0"), lot_cost_total=Decimal("0")
                )
            )

        lots[asset] = [l for l in asset_lots if l.qty_remaining > 0]
        return cost_total, matches, local_warnings

    for t in txs:
        ttype = t.type.strip().lower()
        asset = t.base_asset.upper()

        # Normalize common synonyms so CSVs are flexible
        if ttype == "sell":
            ttype = "trade"

        elif ttype in {"purchase", "spot_buy"}:
            ttype = "buy"

        if ttype == "transfer":
            continue

        elif t.type.lower() == "income":
            # INCOME increases inventory with a cost basis.
            # Priority:
            # 1) If total quote_amount is provided, derive unit basis = quote_amount / base_amount.
            # 2) Else if fair_value (unit) provided, use it directly.
            # 3) Else no fair value -> basis = 0 (with a warning).
            unit_cost = Decimal("0")
            if t.quote_amount is not None and t.base_amount > 0:
                try:
                    unit_cost = t.quote_amount / t.base_amount
                except Exception:
                    unit_cost = Decimal("0")
                    warnings.append(
                        f"Income at {t.timestamp.isoformat()}: invalid quote_amount/base_amount; using basis=0."
                    )
            elif getattr(t, "fair_value", None) is not None:
                unit_cost = t.fair_value
            else:
                warnings.append(
                    f"Income at {t.timestamp.isoformat()}: no fair value provided; using basis=0."
                )

            add_lot(t.base_asset, t.base_amount, unit_cost)
            continue

        elif ttype == "buy":
            # BUY: acquire base_asset using quote_asset. Create lot with non-zero basis.
            if t.quote_asset is None or t.quote_amount is None:
                warnings.append(f"Buy at {t.timestamp.isoformat()} missing quote fields; skipping.")
                continue
            fee_in_quote = Decimal("0")
            if t.fee_asset and t.fee_amount and t.fee_asset.upper() == t.quote_asset.upper():
                fee_in_quote = t.fee_amount
            effective_cost = t.quote_amount + fee_in_quote  # total cost in quote units
            if t.base_amount <= 0:
                warnings.append(
                    f"Buy at {t.timestamp.isoformat()} has non-positive base_amount; skipping."
                )
                continue
            unit_cost = effective_cost / t.base_amount
            add_lot(asset, t.base_amount, unit_cost)
            continue

        elif ttype == "trade":
            # SELL base_asset for quote_asset
            if t.quote_asset is None or t.quote_amount is None:
                warnings.append(
                    f"Trade at {t.timestamp.isoformat()} missing quote fields; skipping."
                )
                continue

            quote = t.quote_asset.upper()
            qty_sold = t.base_amount

            fee = Decimal("0")
            if t.fee_asset and t.fee_amount and t.fee_asset.upper() == quote:
                fee = t.fee_amount

            proceeds = t.quote_amount - fee
            if proceeds < 0:
                warnings.append(
                    f"Trade at {t.timestamp.isoformat()} has fee > proceeds; clamping to 0."
                )
                proceeds = Decimal("0")

            cost_total, matches, local_w = consume_lots(asset, qty_sold)
            warnings.extend(local_w)
            gain = proceeds - cost_total

            events.append(
                Realization(
                    timestamp=t.timestamp.isoformat(),
                    asset=asset,
                    qty_sold=qty_sold,
                    proceeds=proceeds,
                    cost_basis=cost_total,
                    gain=gain,
                    quote_asset=quote,
                    fee_applied=fee,
                    matches=matches,
                )
            )

        else:
            warnings.append(
                f"Unknown transaction type '{t.type}' at {t.timestamp.isoformat()}; skipping."
            )

    summary_by_quote: Dict[str, Dict[str, Decimal]] = {}
    for ev in events:
        q = ev.quote_asset
        agg = summary_by_quote.setdefault(
            q, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")}
        )
        agg["proceeds"] += ev.proceeds
        agg["cost_basis"] += ev.cost_basis
        agg["gain"] += ev.gain

    summary_clean = {
        "by_quote_asset": {
            q: {k: str(v) for k, v in agg.items()} for q, agg in summary_by_quote.items()
        },
        "totals": {
            "proceeds": str(sum((a["proceeds"] for a in summary_by_quote.values()), Decimal("0"))),
            "cost_basis": str(
                sum((a["cost_basis"] for a in summary_by_quote.values()), Decimal("0"))
            ),
            "gain": str(sum((a["gain"] for a in summary_by_quote.values()), Decimal("0"))),
        },
    }
    return events, summary_clean, warnings

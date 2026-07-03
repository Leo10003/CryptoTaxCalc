# fifo_engine.py – Optimized FIFO Engine
"""
Deterministic FIFO cost-basis calculator with audit logging.
Pure function: given transactions → realized events, summary, warnings.
"""

from dataclasses import dataclass, field, asdict
from decimal import Decimal, getcontext, InvalidOperation
from typing import List, Dict, Any, Tuple, Deque
from collections import deque
from pathlib import Path
from datetime import datetime
import json

from cryptotaxcalc.schemas import Transaction
from cryptotaxcalc.logging_setup import get_logger, _atomic_write_json, _now_iso_z

# Precision: 28 digits to avoid rounding issues
getcontext().prec = 28

logger = get_logger("fifo")

# ===========================
# Data Classes
# ===========================

@dataclass
class Lot:
    qty_remaining: Decimal
    cost_per_unit: Decimal
    # Optional: when this lot was originally acquired
    acquired_at: "datetime | None" = None


@dataclass
class Match:
    from_qty: Decimal
    lot_cost_per_unit: Decimal
    lot_cost_total: Decimal
    # When the matched lot was originally acquired (for holding-period rules)
    acquired_at: "datetime | None" = None


@dataclass
class Realization:
    timestamp: str
    asset: str
    qty_sold: Decimal
    proceeds: Decimal
    cost_basis: Decimal
    gain: Decimal
    quote_asset: str
    fee_applied: Decimal
    matches: List[Match] = field(default_factory=list)


# ===========================
# Core Functions
# ===========================

def compute_fifo(
    transactions: List[Transaction],
    *,
    enable_diagnostics: bool = False,
) -> Tuple[List[Realization], Dict[str, Any], List[str]]:
    """
    Deterministic FIFO engine:
      - BUY: creates a cost lot
      - TRADE: consumes lots FIFO and emits realized events
      - INCOME: adds zero-basis or FV-based lots
      - TRANSFER: ignored
    Emits audit logs and returns (events, summary, warnings).
    """
    start_ts = _now_iso_z()
    logger.info(f"FIFO run started at {start_ts}, transactions={len(transactions)}")

    def _tx_sort_key(t: Transaction) -> tuple:
        """
        Stable, deterministic ordering for FIFO.
        If multiple rows share the same timestamp, we fall back to a composite key
        so results do not depend on input list order.
        """
        try:
            ts = t.timestamp.isoformat()
        except Exception:
            ts = str(getattr(t, "timestamp", "") or "")

        def _s(v) -> str:
            # Normalize for deterministic comparisons (avoid None, Decimal ordering quirks, etc.)
            if v is None:
                return ""
            return str(v)

        return (
            ts,
            _s(getattr(t, "type", "")).strip().lower(),
            _s(getattr(t, "base_asset", "")).strip().upper(),
            _s(getattr(t, "base_amount", "")),
            _s(getattr(t, "quote_asset", "")).strip().upper(),
            _s(getattr(t, "quote_amount", "")),
            _s(getattr(t, "fee_asset", "")).strip().upper(),
            _s(getattr(t, "fee_amount", "")),
            _s(getattr(t, "exchange", "")).strip(),
            _s(getattr(t, "memo", "")).strip(),
            _s(getattr(t, "fair_value", "")),
        )

    txs = sorted(transactions, key=_tx_sort_key)
    lots: Dict[str, Deque[Lot]] = {}
    events: List[Realization] = []
    warnings: List[str] = []
    
    # Data-quality tracking:
    # - Fees in quote_asset are applied directly (already supported).
    # - Fees in base_asset are applied by adjusting lot quantity (BUY) or disposal quantity (TRADE).
    # - Fees in third assets (neither base nor quote) are recorded and surfaced as warnings (valuation requires pricing data).
    fee_base_count = 0
    fee_base_assets: Dict[str, int] = {}
    fee_third_count = 0
    fee_third_assets: Dict[str, int] = {}
    
    # Canonical transaction types (normalize common CSV/export labels)
    BUY_TYPES = {
        "buy",
        "purchase",
        "spot_buy",
        "spot_purchase",
        "market_buy",
    }

    # Disposals / swaps / conversions (treated as TRADE: consume lots + emit realization)
    DISPOSAL_TYPES = {
        "trade",
        "sell",
        "spot_sell",
        "spot_trade",
        "swap",
        "spot_swap",
        "convert",
        "conversion",
        "exchange",
        "spend",
        "payment",
    }

    # Income-like events (adds lots, basis via quote_amount/base_amount or fair_value)
    INCOME_TYPES = {
        "income",
        "reward",
        "rewards",
        "staking",
        "staking_reward",
        "interest",
        "airdrop",
        "mining",
        "bonus",
    }

    # Transfers (ignored in pooled FIFO)
    TRANSFER_TYPES = {
        "transfer",
        "transfer_in",
        "transfer_out",
        "deposit",
        "withdraw",
        "withdrawal",
        "send",
        "receive",
    }

    def _dec(v) -> Decimal:
        try:
            return Decimal(str(v or "0"))
        except InvalidOperation:
            return Decimal("0")

    def add_lot(
        asset: str,
        qty: Decimal,
        cost_per_unit: Decimal,
        acquired_at: "datetime | None" = None,
    ) -> None:
        if qty <= 0:
            return
        lots.setdefault(asset, deque()).append(
            Lot(qty_remaining=qty, cost_per_unit=cost_per_unit, acquired_at=acquired_at)
        )
        logger.debug(
            "Added lot %s: qty=%s, cost_per_unit=%s, acquired_at=%s",
            asset,
            qty,
            cost_per_unit,
            acquired_at,
        )

    def consume_lots(asset: str, qty_to_sell: Decimal, event_ts: str) -> Tuple[Decimal, List[Match], List[object]]:
        cost_total = Decimal("0")
        matches: List[Match] = []
        local_warnings: List[object] = []

        remaining = qty_to_sell
        asset_lots = lots.get(asset)
        if asset_lots is None:
            asset_lots = deque()
            lots[asset] = asset_lots

        while remaining > 0 and len(asset_lots) > 0:
            lot = asset_lots[0]
            take = min(lot.qty_remaining, remaining)

            if take > 0:
                lot_cost = lot.cost_per_unit * take
                matches.append(
                    Match(
                        from_qty=take,
                        lot_cost_per_unit=lot.cost_per_unit,
                        lot_cost_total=lot_cost,
                        acquired_at=getattr(lot, "acquired_at", None),
                    )
                )
                cost_total += lot_cost
                lot.qty_remaining -= take
                remaining -= take

            if lot.qty_remaining == 0:
                asset_lots.popleft()

        if remaining > 0:
            local_warnings.append({
                "type": "missing_history",
                "severity": "blocker",
                "asset": asset,
                "missing_qty": str(remaining),
                "sold_qty": str(qty_to_sell),
                "timestamp": event_ts,
                "message": (
                    f"Sold {qty_to_sell} {asset} but no acquisition history was found "
                    f"for {remaining} {asset}."
                ),
                "action_required": (
                    "Import earlier trades, deposits, or transfers before exporting."
                ),
            })
            matches.append(
                Match(
                    from_qty=remaining,
                    lot_cost_per_unit=Decimal("0"),
                    lot_cost_total=Decimal("0"),
                    acquired_at=None,
                )
            )

        return cost_total, matches, local_warnings

    def _serialize_match(m: Match) -> dict:
        """
        Convert a Match dataclass to a JSON-safe dict.
        Ensures acquired_at is an ISO string (or None).
        """
        d = asdict(m)
        if d.get("acquired_at") is not None:
            try:
                d["acquired_at"] = d["acquired_at"].isoformat()
            except Exception:
                # Fallback to string if something unexpected happens
                d["acquired_at"] = str(d["acquired_at"])
        return d

    def _serialize_event(e: Realization) -> dict:
        """
        Convert a Realization dataclass (including matches) to JSON-safe dict.
        """
        d = asdict(e)

        # Matches: normalize acquired_at using the original dataclass objects
        d["matches"] = [_serialize_match(m) for m in (getattr(e, "matches", []) or [])]

        return d

    # Core processing loop
    for t in txs:
        try:
            raw_type = (t.type or "").strip().lower()
            ttype = raw_type.replace(" ", "_").replace("-", "_")
            asset = (t.base_asset or "").upper()

            if not asset:
                warnings.append(f"Transaction {t.timestamp}: missing base_asset; skipped.")
                continue

            if ttype in BUY_TYPES:
                ttype = "buy"
            elif ttype in DISPOSAL_TYPES:
                ttype = "trade"
            elif ttype in INCOME_TYPES:
                ttype = "income"
            elif ttype in TRANSFER_TYPES:
                ttype = "transfer"

            if ttype == "transfer":
                continue

            if ttype == "income":
                base_qty = t.base_amount
                if base_qty is None or base_qty == 0:
                    warnings.append(f"Income {t.timestamp}: missing/zero base_amount; skipped.")
                    continue
                if base_qty < 0:
                    warnings.append(f"Income {t.timestamp}: negative base_amount; using absolute value.")
                    base_qty = abs(base_qty)

                unit_cost = Decimal("0")

                if t.quote_amount is not None and base_qty > 0:
                    quote_amt = t.quote_amount
                    if quote_amt < 0:
                        warnings.append(f"Income {t.timestamp}: negative quote_amount; using absolute value.")
                        quote_amt = abs(quote_amt)
                    try:
                        unit_cost = quote_amt / base_qty
                    except Exception:
                        warnings.append(f"Income {t.timestamp}: invalid division; basis=0.")
                elif getattr(t, "fair_value", None) is not None:
                    fv = _dec(getattr(t, "fair_value", None))
                    if fv < 0:
                        warnings.append(f"Income {t.timestamp}: negative fair_value; clamped to 0.")
                        fv = Decimal("0")
                    unit_cost = fv
                else:
                    warnings.append(f"Income {t.timestamp}: no fair value; basis=0.")

                add_lot(asset, base_qty, unit_cost, acquired_at=t.timestamp)
                continue

            if ttype == "buy":
                if not t.quote_asset or t.quote_amount is None:
                    warnings.append(f"Buy {t.timestamp}: missing quote fields.")
                    continue

                base_qty = t.base_amount
                if base_qty is None or base_qty == 0:
                    warnings.append(f"Buy {t.timestamp}: missing/zero base_amount.")
                    continue
                if base_qty < 0:
                    warnings.append(f"Buy {t.timestamp}: negative base_amount; using absolute value.")
                    base_qty = abs(base_qty)

                quote = t.quote_asset.upper()
                quote_amt = t.quote_amount
                if quote_amt < 0:
                    warnings.append(f"Buy {t.timestamp}: negative quote_amount; using absolute value.")
                    quote_amt = abs(quote_amt)

                fee_in_quote = Decimal("0")
                fee_in_base = Decimal("0")

                if t.fee_asset and t.fee_amount is not None and t.fee_amount != 0:
                    fa = t.fee_asset.upper()
                    fee_amt = t.fee_amount
                    if fee_amt < 0:
                        warnings.append(f"Buy {t.timestamp}: negative fee_amount; using absolute value.")
                        fee_amt = abs(fee_amt)

                    if fa == quote:
                        fee_in_quote = fee_amt
                    elif fa == asset:
                        fee_in_base = fee_amt
                        fee_base_count += 1
                        fee_base_assets[fa] = fee_base_assets.get(fa, 0) + 1
                    else:
                        fee_third_count += 1
                        fee_third_assets[fa] = fee_third_assets.get(fa, 0) + 1

                # Fee in base asset reduces the credited quantity (net received).
                net_base_qty = base_qty - fee_in_base
                if net_base_qty <= 0:
                    warnings.append(f"Buy {t.timestamp}: base fee >= base_amount; skipped.")
                    continue

                effective_cost = quote_amt + fee_in_quote
                try:
                    unit_cost = effective_cost / net_base_qty
                except Exception:
                    warnings.append(f"Buy {t.timestamp}: invalid division; skipped.")
                    continue

                add_lot(asset, net_base_qty, unit_cost, acquired_at=t.timestamp)
                continue

            if ttype == "trade":
                if not t.quote_asset or t.quote_amount is None:
                    warnings.append(f"Trade {t.timestamp}: missing quote fields.")
                    continue

                qty_sold = t.base_amount
                if qty_sold is None or qty_sold == 0:
                    warnings.append(f"Trade {t.timestamp}: missing/zero base_amount.")
                    continue
                if qty_sold < 0:
                    warnings.append(f"Trade {t.timestamp}: negative base_amount; using absolute value.")
                    qty_sold = abs(qty_sold)

                quote = t.quote_asset.upper()
                proceeds_raw = t.quote_amount
                if proceeds_raw < 0:
                    warnings.append(f"Trade {t.timestamp}: negative quote_amount; using absolute value.")
                    proceeds_raw = abs(proceeds_raw)

                fee_in_quote = Decimal("0")
                fee_in_base = Decimal("0")

                if t.fee_asset and t.fee_amount is not None and t.fee_amount != 0:
                    fa = t.fee_asset.upper()
                    fee_amt = t.fee_amount
                    if fee_amt < 0:
                        warnings.append(f"Trade {t.timestamp}: negative fee_amount; using absolute value.")
                        fee_amt = abs(fee_amt)

                    if fa == quote:
                        fee_in_quote = fee_amt
                    elif fa == asset:
                        fee_in_base = fee_amt
                        fee_base_count += 1
                        fee_base_assets[fa] = fee_base_assets.get(fa, 0) + 1
                    else:
                        fee_third_count += 1
                        fee_third_assets[fa] = fee_third_assets.get(fa, 0) + 1

                proceeds = proceeds_raw - fee_in_quote
                if proceeds < 0:
                    proceeds = Decimal("0")
                    warnings.append(f"Trade {t.timestamp}: fee > proceeds; clamped.")

                # Base-asset fee is modeled as additional disposed quantity with zero proceeds.
                qty_disposed = qty_sold + fee_in_base

                cost_total, matches, local_w = consume_lots(asset, qty_disposed, t.timestamp.isoformat())
                warnings.extend(local_w)
                gain = proceeds - cost_total

                # Reporting convenience: approximate total fees in quote terms.
                fee_quote_equiv = fee_in_quote
                if fee_in_base > 0 and qty_sold > 0:
                    try:
                        exec_price = proceeds_raw / qty_sold
                        fee_quote_equiv = fee_in_quote + (fee_in_base * exec_price)
                    except Exception:
                        fee_quote_equiv = fee_in_quote

                events.append(
                    Realization(
                        timestamp=t.timestamp.isoformat(),
                        asset=asset,
                        qty_sold=qty_disposed,
                        proceeds=proceeds,
                        cost_basis=cost_total,
                        gain=gain,
                        quote_asset=quote,
                        fee_applied=fee_quote_equiv,
                        matches=matches,
                    )
                )
                continue

            warnings.append(f"Unknown type '{t.type}' at {t.timestamp}.")

        except Exception as e:
            msg = f"FIFO error at transaction {getattr(t, 'timestamp', '?')}: {e}"
            warnings.append(msg)
            logger.warning(msg)

    # Data-quality surface: quote/base fees are applied; third-asset fees require external pricing.
    # Aggregate into a small number of warnings (avoid per-row spam).
    if fee_base_count > 0:
        top_assets = sorted(fee_base_assets.items(), key=lambda kv: kv[1], reverse=True)[:3]
        top_str = ", ".join([f"{a}×{n}" for a, n in top_assets if a])
        detail = f" (top: {top_str})" if top_str else ""
        warnings.append(
            f"Fee handling: applied base-asset fees for {fee_base_count} transactions{detail}. "
            "BUY lots were reduced by the base-asset fee; TRADE disposals include the base-asset fee as extra disposed quantity."
        )

    if fee_third_count > 0:
        top_assets = sorted(fee_third_assets.items(), key=lambda kv: kv[1], reverse=True)[:3]
        top_str = ", ".join([f"{a}×{n}" for a, n in top_assets if a])
        detail = f" (top: {top_str})" if top_str else ""
        warnings.append(
            f"Fee handling: {fee_third_count} transactions have fees paid in third assets (neither base nor quote){detail}. "
            "These fees are recorded but not valued/applied because price data was unavailable. Load daily prices (base=<ASSET>, quote=EUR) and re-run."
        )

    # Build summary
    summary_by_quote: Dict[str, Dict[str, Decimal]] = {}
    for ev in events:
        q = ev.quote_asset
        agg = summary_by_quote.setdefault(q, {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")})
        agg["proceeds"] += ev.proceeds
        agg["cost_basis"] += ev.cost_basis
        agg["gain"] += ev.gain

    summary_clean = {
        "by_quote_asset": {
            q: {k: str(v.quantize(Decimal('0.00000001'))) for k, v in agg.items()} for q, agg in summary_by_quote.items()
        },
        "totals": {
            "proceeds": str(sum((a["proceeds"] for a in summary_by_quote.values()), Decimal("0"))),
            "cost_basis": str(sum((a["cost_basis"] for a in summary_by_quote.values()), Decimal("0"))),
            "gain": str(sum((a["gain"] for a in summary_by_quote.values()), Decimal("0"))),
        },
    }

    # Diagnostics & audit output
    try:
        out_dir = Path("logs/fifo")
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": start_ts,
            "events_count": len(events),
            "warnings_count": len(warnings),
            "summary": summary_clean,
        }
        _atomic_write_json(out_dir / "last_run.json", payload)
    except Exception as e:
        logger.warning(f"Could not write FIFO diagnostics: {e}")

    logger.info(f"FIFO completed: {len(events)} events, {len(warnings)} warnings.")
    return events, summary_clean, warnings

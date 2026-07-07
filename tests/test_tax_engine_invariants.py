import pytest

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.schemas import Transaction

pytestmark = pytest.mark.smoke


Q8 = Decimal("0.00000001")


def _dt(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _tx(
    *,
    timestamp: datetime,
    type: str,
    base_asset: str,
    base_amount: str,
    quote_asset: str | None = None,
    quote_amount: str | None = None,
    fee_asset: str | None = None,
    fee_amount: str | None = None,
    memo: str | None = None,
) -> Transaction:
    return Transaction(
        timestamp=timestamp,
        type=type,
        base_asset=base_asset,
        base_amount=Decimal(base_amount),
        quote_asset=quote_asset,
        quote_amount=Decimal(quote_amount) if quote_amount is not None else None,
        fee_asset=fee_asset,
        fee_amount=Decimal(fee_amount) if fee_amount is not None else None,
        memo=memo,
    )


def _money(value: Decimal) -> Decimal:
    return value.quantize(Q8)


def _has_blocker_missing_history(warnings: list[object], *, asset: str) -> bool:
    return any(
        isinstance(w, dict)
        and w.get("type") == "missing_history"
        and w.get("severity") == "blocker"
        and w.get("asset") == asset
        for w in warnings
    )


def test_every_realization_gain_equals_proceeds_minus_cost_basis():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="BTC", base_amount="1.25", quote_asset="EUR", quote_amount="50000"),
        _tx(timestamp=t0 + timedelta(days=1), type="buy", base_asset="BTC", base_amount="0.75", quote_asset="EUR", quote_amount="45000"),
        _tx(timestamp=t0 + timedelta(days=2), type="sell", base_asset="BTC", base_amount="0.5", quote_asset="EUR", quote_amount="30000", fee_asset="EUR", fee_amount="50"),
        _tx(timestamp=t0 + timedelta(days=3), type="sell", base_asset="BTC", base_amount="1.1", quote_asset="EUR", quote_amount="77000"),
    ]

    events, _, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 2
    for ev in events:
        assert ev.gain == ev.proceeds - ev.cost_basis


def test_match_quantities_sum_to_disposed_quantity_for_each_event():
    t0 = _dt(2025, 2, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="ETH", base_amount="1", quote_asset="EUR", quote_amount="1200"),
        _tx(timestamp=t0 + timedelta(days=1), type="buy", base_asset="ETH", base_amount="2", quote_asset="EUR", quote_amount="3600"),
        _tx(timestamp=t0 + timedelta(days=2), type="buy", base_asset="ETH", base_amount="3", quote_asset="EUR", quote_amount="6600"),
        _tx(timestamp=t0 + timedelta(days=3), type="sell", base_asset="ETH", base_amount="4.25", quote_asset="EUR", quote_amount="12750"),
    ]

    events, _, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    ev = events[0]
    assert sum((m.from_qty for m in ev.matches), Decimal("0")) == ev.qty_sold
    assert sum((m.lot_cost_total for m in ev.matches), Decimal("0")) == ev.cost_basis


def test_summary_totals_equal_sum_of_realization_events_by_quote_asset():
    t0 = _dt(2025, 3, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="BTC", base_amount="1", quote_asset="EUR", quote_amount="40000"),
        _tx(timestamp=t0 + timedelta(minutes=1), type="buy", base_asset="ETH", base_amount="10", quote_asset="USD", quote_amount="20000"),
        _tx(timestamp=t0 + timedelta(days=1), type="sell", base_asset="BTC", base_amount="0.25", quote_asset="EUR", quote_amount="15000"),
        _tx(timestamp=t0 + timedelta(days=2), type="sell", base_asset="ETH", base_amount="4", quote_asset="USD", quote_amount="10000", fee_asset="USD", fee_amount="10"),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    expected_by_quote: dict[str, dict[str, Decimal]] = {}
    for ev in events:
        bucket = expected_by_quote.setdefault(
            ev.quote_asset,
            {"proceeds": Decimal("0"), "cost_basis": Decimal("0"), "gain": Decimal("0")},
        )
        bucket["proceeds"] += ev.proceeds
        bucket["cost_basis"] += ev.cost_basis
        bucket["gain"] += ev.gain

    assert set(summary["by_quote_asset"]) == set(expected_by_quote)
    for quote, expected in expected_by_quote.items():
        for key, value in expected.items():
            assert Decimal(summary["by_quote_asset"][quote][key]) == _money(value)

    assert Decimal(summary["totals"]["proceeds"]) == sum((ev.proceeds for ev in events), Decimal("0"))
    assert Decimal(summary["totals"]["cost_basis"]) == sum((ev.cost_basis for ev in events), Decimal("0"))
    assert Decimal(summary["totals"]["gain"]) == sum((ev.gain for ev in events), Decimal("0"))


def test_transfers_do_not_reduce_available_fifo_lots():
    t0 = _dt(2025, 4, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="SOL", base_amount="10", quote_asset="EUR", quote_amount="1000"),
        _tx(timestamp=t0 + timedelta(days=1), type="withdrawal", base_asset="SOL", base_amount="10", quote_asset="SOL", quote_amount="10"),
        _tx(timestamp=t0 + timedelta(days=2), type="sell", base_asset="SOL", base_amount="10", quote_asset="EUR", quote_amount="1500"),
    ]

    events, _, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    assert events[0].qty_sold == Decimal("10")
    assert events[0].cost_basis == Decimal("1000")
    assert events[0].gain == Decimal("500")
    assert sum((m.from_qty for m in events[0].matches), Decimal("0")) == Decimal("10")


def test_oversell_always_surfaces_blocker_missing_history_warning():
    t0 = _dt(2025, 5, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="ADA", base_amount="100", quote_asset="EUR", quote_amount="50"),
        _tx(timestamp=t0 + timedelta(days=1), type="sell", base_asset="ADA", base_amount="150", quote_asset="EUR", quote_amount="120"),
    ]

    events, _, warnings = compute_fifo(txs)

    assert len(events) == 1
    assert _has_blocker_missing_history(warnings, asset="ADA")
    assert events[0].qty_sold == Decimal("150")
    assert sum((m.from_qty for m in events[0].matches), Decimal("0")) == Decimal("150")
    assert any(m.lot_cost_per_unit == Decimal("0") and m.lot_cost_total == Decimal("0") for m in events[0].matches)


def test_base_asset_fee_is_included_in_disposed_quantity_and_warned_once():
    t0 = _dt(2025, 6, 1)
    txs = [
        _tx(timestamp=t0, type="buy", base_asset="BTC", base_amount="1", quote_asset="EUR", quote_amount="20000"),
        _tx(timestamp=t0 + timedelta(days=1), type="sell", base_asset="BTC", base_amount="0.4", quote_asset="EUR", quote_amount="12000", fee_asset="BTC", fee_amount="0.01"),
    ]

    events, _, warnings = compute_fifo(txs)

    assert len(events) == 1
    ev = events[0]
    assert ev.qty_sold == Decimal("0.41")
    assert sum((m.from_qty for m in ev.matches), Decimal("0")) == Decimal("0.41")
    assert ev.cost_basis == Decimal("8200.00")
    assert ev.gain == Decimal("3800.00")
    assert ev.fee_applied == Decimal("300.00")
    assert len(warnings) == 1
    assert isinstance(warnings[0], str)
    assert "applied base-asset fees" in warnings[0]
import pytest

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.schemas import Transaction

pytestmark = pytest.mark.smoke


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


def _q8(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"))


def test_single_lot_fifo_exact_gain():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(
            timestamp=t0,
            type="buy",
            base_asset="BTC",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="10000",
        ),
        _tx(
            timestamp=t0 + timedelta(days=10),
            type="sell",
            base_asset="BTC",
            base_amount="0.4",
            quote_asset="EUR",
            quote_amount="6000",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    ev = events[0]
    assert ev.asset == "BTC"
    assert ev.qty_sold == Decimal("0.4")
    assert ev.proceeds == Decimal("6000")
    assert ev.cost_basis == Decimal("4000.0")
    assert ev.gain == Decimal("2000.0")
    assert len(ev.matches) == 1
    assert ev.matches[0].from_qty == Decimal("0.4")
    assert ev.matches[0].lot_cost_per_unit == Decimal("10000")
    assert summary["by_quote_asset"]["EUR"]["gain"] == "2000.00000000"


def test_multi_lot_partial_fifo_exact_gain():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(
            timestamp=t0,
            type="buy",
            base_asset="ETH",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="1000",
        ),
        _tx(
            timestamp=t0 + timedelta(days=1),
            type="buy",
            base_asset="ETH",
            base_amount="2",
            quote_asset="EUR",
            quote_amount="3000",
        ),
        _tx(
            timestamp=t0 + timedelta(days=2),
            type="sell",
            base_asset="ETH",
            base_amount="2.5",
            quote_asset="EUR",
            quote_amount="5000",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    ev = events[0]
    assert ev.qty_sold == Decimal("2.5")
    assert ev.proceeds == Decimal("5000")
    assert ev.cost_basis == Decimal("3250.0")
    assert ev.gain == Decimal("1750.0")
    assert [(m.from_qty, m.lot_cost_per_unit, m.lot_cost_total) for m in ev.matches] == [
        (Decimal("1"), Decimal("1000"), Decimal("1000")),
        (Decimal("1.5"), Decimal("1500"), Decimal("2250.0")),
    ]
    assert summary["by_quote_asset"]["EUR"]["cost_basis"] == "3250.00000000"


def test_quote_fee_reduces_proceeds_and_exact_gain():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(
            timestamp=t0,
            type="buy",
            base_asset="BTC",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="10000",
        ),
        _tx(
            timestamp=t0 + timedelta(days=1),
            type="sell",
            base_asset="BTC",
            base_amount="0.5",
            quote_asset="EUR",
            quote_amount="8000",
            fee_asset="EUR",
            fee_amount="25",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    ev = events[0]
    assert ev.qty_sold == Decimal("0.5")
    assert ev.proceeds == Decimal("7975")
    assert ev.cost_basis == Decimal("5000.0")
    assert ev.gain == Decimal("2975.0")
    assert ev.fee_applied == Decimal("25")
    assert summary["by_quote_asset"]["EUR"]["proceeds"] == "7975.00000000"
    assert summary["by_quote_asset"]["EUR"]["gain"] == "2975.00000000"


def test_transfer_does_not_create_taxable_disposal():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(
            timestamp=t0,
            type="buy",
            base_asset="BTC",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="10000",
        ),
        _tx(
            timestamp=t0 + timedelta(days=1),
            type="transfer",
            base_asset="BTC",
            base_amount="0.75",
            quote_asset="BTC",
            quote_amount="0.75",
            memo="move from exchange to wallet",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert events == []
    assert summary["by_quote_asset"] == {}
    assert summary["totals"]["proceeds"] == "0"
    assert summary["totals"]["cost_basis"] == "0"
    assert summary["totals"]["gain"] == "0"


def test_oversell_produces_blocker_warning_and_does_not_silently_invent_basis():
    t0 = _dt(2025, 1, 1)
    txs = [
        _tx(
            timestamp=t0,
            type="buy",
            base_asset="SOL",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="100",
        ),
        _tx(
            timestamp=t0 + timedelta(days=1),
            type="sell",
            base_asset="SOL",
            base_amount="2",
            quote_asset="EUR",
            quote_amount="300",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert len(events) == 1
    assert len(warnings) == 1
    warning = warnings[0]
    assert isinstance(warning, dict)
    assert warning["type"] == "missing_history"
    assert warning["severity"] == "blocker"
    assert warning["asset"] == "SOL"
    assert warning["missing_qty"] == "1"

    ev = events[0]
    assert ev.qty_sold == Decimal("2")
    assert ev.proceeds == Decimal("300")
    assert ev.cost_basis == Decimal("100")
    assert ev.gain == Decimal("200")
    assert len(ev.matches) == 2
    assert ev.matches[0].from_qty == Decimal("1")
    assert ev.matches[0].lot_cost_total == Decimal("100")
    assert ev.matches[1].from_qty == Decimal("1")
    assert ev.matches[1].lot_cost_per_unit == Decimal("0")
    assert ev.matches[1].lot_cost_total == Decimal("0")
    assert summary["by_quote_asset"]["EUR"]["gain"] == "200.00000000"
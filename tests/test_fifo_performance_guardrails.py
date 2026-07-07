from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.schemas import Transaction

pytestmark = pytest.mark.smoke


def _tx(
    *,
    timestamp: datetime,
    type: str,
    base_asset: str,
    base_amount: str,
    quote_asset: str = "EUR",
    quote_amount: str = "0",
    fee_asset: str = "EUR",
    fee_amount: str = "0",
) -> Transaction:
    return Transaction(
        timestamp=timestamp,
        type=type,
        base_asset=base_asset,
        base_amount=Decimal(base_amount),
        quote_asset=quote_asset,
        quote_amount=Decimal(quote_amount),
        fee_asset=fee_asset,
        fee_amount=Decimal(fee_amount),
    )


def _event_payload(events) -> list[tuple[str, str, str, str, str, int]]:
    return [
        (
            event.timestamp,
            event.asset,
            str(event.qty_sold),
            str(event.proceeds),
            str(event.gain),
            len(event.matches),
        )
        for event in events
    ]


def test_fifo_handles_many_small_lots_with_bounded_runtime_and_exact_totals():
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    txs: list[Transaction] = []

    for i in range(800):
        txs.append(
            _tx(
                timestamp=t0 + timedelta(minutes=i),
                type="buy",
                base_asset="PERFLOT",
                base_amount="0.01",
                quote_amount="1.00",
            )
        )

    for i in range(400):
        txs.append(
            _tx(
                timestamp=t0 + timedelta(days=10, minutes=i),
                type="sell",
                base_asset="PERFLOT",
                base_amount="0.01",
                quote_amount="1.50",
            )
        )

    started = time.perf_counter()
    events, summary, warnings = compute_fifo(txs)
    elapsed = time.perf_counter() - started

    assert elapsed < 3.0, f"FIFO runtime regression: {elapsed:.3f}s for {len(txs)} deterministic rows"
    assert warnings == []
    assert len(events) == 400
    assert all(len(event.matches) == 1 for event in events)
    assert summary["by_quote_asset"]["EUR"] == {
        "proceeds": "600.00000000",
        "cost_basis": "400.00000000",
        "gain": "200.00000000",
    }
    assert summary["totals"] == {
        "proceeds": "600.00",
        "cost_basis": "400.00",
        "gain": "200.00",
    }


def test_fifo_is_deterministic_when_same_timestamp_rows_are_input_in_reverse_order():
    t0 = datetime(2025, 2, 1, tzinfo=timezone.utc)
    chronological = [
        _tx(timestamp=t0, type="buy", base_asset="PERFDET", base_amount="1", quote_amount="100"),
        _tx(timestamp=t0, type="buy", base_asset="PERFDET", base_amount="1", quote_amount="200"),
        _tx(timestamp=t0, type="sell", base_asset="PERFDET", base_amount="1.5", quote_amount="450"),
    ]
    reversed_input = list(reversed(chronological))

    events_a, summary_a, warnings_a = compute_fifo(chronological)
    events_b, summary_b, warnings_b = compute_fifo(reversed_input)

    assert warnings_a == []
    assert warnings_b == []
    assert _event_payload(events_a) == _event_payload(events_b)
    assert summary_a == summary_b
    assert len(events_a) == 1
    assert events_a[0].cost_basis == Decimal("200.0")
    assert events_a[0].gain == Decimal("250.0")


def test_fifo_multi_asset_interleaving_keeps_lots_isolated_and_linear_enough_for_smoke_ci():
    t0 = datetime(2025, 3, 1, tzinfo=timezone.utc)
    assets = ["PERFA", "PERFB", "PERFC", "PERFD"]
    txs: list[Transaction] = []

    for i in range(250):
        for asset_index, asset in enumerate(assets):
            txs.append(
                _tx(
                    timestamp=t0 + timedelta(minutes=(i * len(assets)) + asset_index),
                    type="buy",
                    base_asset=asset,
                    base_amount="1",
                    quote_amount=str(Decimal("10") + asset_index),
                )
            )

    for i in range(125):
        for asset_index, asset in enumerate(assets):
            txs.append(
                _tx(
                    timestamp=t0 + timedelta(days=30, minutes=(i * len(assets)) + asset_index),
                    type="sell",
                    base_asset=asset,
                    base_amount="1",
                    quote_amount=str(Decimal("20") + asset_index),
                )
            )

    started = time.perf_counter()
    events, summary, warnings = compute_fifo(txs)
    elapsed = time.perf_counter() - started

    assert elapsed < 4.0, f"FIFO multi-asset runtime regression: {elapsed:.3f}s for {len(txs)} rows"
    assert warnings == []
    assert len(events) == 500
    assert {event.asset for event in events} == set(assets)
    assert summary["by_quote_asset"]["EUR"] == {
        "proceeds": "10750.00000000",
        "cost_basis": "5750.00000000",
        "gain": "5000.00000000",
    }
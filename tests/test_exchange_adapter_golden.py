from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from cryptotaxcalc.csv_normalizer import parse_csv_with_meta
from cryptotaxcalc.fifo_engine import compute_fifo

pytestmark = pytest.mark.smoke

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "golden"


def _stable_decimal(value: object) -> str:
    decimal_value = Decimal(str(value or "0"))
    if decimal_value == decimal_value.to_integral_value():
        return format(decimal_value.quantize(Decimal("1")), "f")
    return format(decimal_value.normalize(), "f")


def _event_payload(event) -> dict:
    return {
        "timestamp": event.timestamp,
        "asset": event.asset,
        "qty_sold": _stable_decimal(event.qty_sold),
        "proceeds": _stable_decimal(event.proceeds),
        "cost_basis": _stable_decimal(event.cost_basis),
        "gain": _stable_decimal(event.gain),
        "quote_asset": event.quote_asset,
        "fee_applied": _stable_decimal(event.fee_applied),
        "matches": [
            {
                "from_qty": _stable_decimal(match.from_qty),
                "lot_cost_per_unit": _stable_decimal(match.lot_cost_per_unit),
                "lot_cost_total": _stable_decimal(match.lot_cost_total),
                "acquired_at": None if match.acquired_at is None else match.acquired_at.isoformat(),
            }
            for match in event.matches
        ],
    }


def test_binance_spot_trades_fixture_normalizes_and_calculates_fifo_golden_result():
    fixture = FIXTURE_DIR / "binance_spot_trades_fifo.csv"

    rows, errors, meta = parse_csv_with_meta(fixture.read_bytes(), filename=fixture.name)

    assert errors == []
    assert meta["recognized_source_id"] == "binance_spot_trades"
    assert meta["recognized_source_status"] == "supported"
    assert len(rows) == 3
    assert [(row.type, row.base_asset, row.base_amount, row.quote_asset, row.quote_amount, row.fee_asset, row.fee_amount) for row in rows] == [
        ("BUY", "BTC", Decimal("0.5"), "USDT", Decimal("20000"), "USDT", Decimal("10")),
        ("BUY", "BTC", Decimal("0.4"), "USDT", Decimal("20000"), "BNB", Decimal("0.0001")),
        ("SELL", "BTC", Decimal("0.6"), "USDT", Decimal("36000"), "USDT", Decimal("20")),
    ]

    events, summary, warnings = compute_fifo(rows)

    assert len(warnings) == 1
    assert "fees paid in third assets" in warnings[0]
    assert "BNB" in warnings[0]
    assert [_event_payload(event) for event in events] == [
        {
            "timestamp": "2025-03-01T00:00:00",
            "asset": "BTC",
            "qty_sold": "0.6",
            "proceeds": "35980",
            "cost_basis": "25010",
            "gain": "10970",
            "quote_asset": "USDT",
            "fee_applied": "20",
            "matches": [
                {
                    "from_qty": "0.5",
                    "lot_cost_per_unit": "40020",
                    "lot_cost_total": "20010",
                    "acquired_at": "2025-01-01T00:00:00",
                },
                {
                    "from_qty": "0.1",
                    "lot_cost_per_unit": "50000",
                    "lot_cost_total": "5000",
                    "acquired_at": "2025-02-01T00:00:00",
                },
            ],
        }
    ]
    assert summary == {
        "by_quote_asset": {
            "USDT": {
                "proceeds": "35980.00000000",
                "cost_basis": "25010.00000000",
                "gain": "10970.00000000",
            }
        },
        "totals": {
            "proceeds": "35980",
            "cost_basis": "25010",
            "gain": "10970",
        },
    }


def test_coinbase_transactions_fixture_normalizes_and_calculates_fifo_golden_result():
    fixture = FIXTURE_DIR / "coinbase_transactions_fifo.csv"

    rows, errors, meta = parse_csv_with_meta(fixture.read_bytes(), filename=fixture.name)

    assert errors == []
    assert meta["recognized_source_id"] == "coinbase_transactions"
    assert meta["recognized_source_status"] == "supported"
    assert len(rows) == 2
    assert [(row.type, row.base_asset, row.base_amount, row.quote_asset, row.quote_amount, row.fee_asset, row.fee_amount, row.memo) for row in rows] == [
        ("BUY", "ETH", Decimal("2"), "USD", Decimal("4000"), "USD", Decimal("10"), "first lot"),
        ("SELL", "ETH", Decimal("1.25"), "USD", Decimal("3750"), "USD", Decimal("15"), "partial disposal"),
    ]

    events, summary, warnings = compute_fifo(rows)

    assert warnings == []
    assert [_event_payload(event) for event in events] == [
        {
            "timestamp": "2025-01-05T00:00:00",
            "asset": "ETH",
            "qty_sold": "1.25",
            "proceeds": "3735",
            "cost_basis": "2506.25",
            "gain": "1228.75",
            "quote_asset": "USD",
            "fee_applied": "15",
            "matches": [
                {
                    "from_qty": "1.25",
                    "lot_cost_per_unit": "2005",
                    "lot_cost_total": "2506.25",
                    "acquired_at": "2025-01-01T00:00:00",
                }
            ],
        }
    ]
    assert summary == {
        "by_quote_asset": {
            "USD": {
                "proceeds": "3735.00000000",
                "cost_basis": "2506.25000000",
                "gain": "1228.75000000",
            }
        },
        "totals": {
            "proceeds": "3735",
            "cost_basis": "2506.25",
            "gain": "1228.75",
        },
    }
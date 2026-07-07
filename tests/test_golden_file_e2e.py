from __future__ import annotations

import pytest

import json
from decimal import Decimal
from pathlib import Path

from cryptotaxcalc.csv_normalizer import parse_csv_with_meta
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.rules.registry import get_rule, split_taxable_exempt_gain
from cryptotaxcalc.schemas import CalcConfig

pytestmark = pytest.mark.smoke


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "golden"
GOLDEN_CSV = FIXTURE_DIR / "normalized_hr_fifo.csv"
GOLDEN_EXPECTED = FIXTURE_DIR / "normalized_hr_fifo_expected.json"


def _stable_decimal(value: object) -> str:
    decimal_value = Decimal(str(value or "0"))
    if decimal_value == decimal_value.to_integral_value():
        return format(decimal_value.quantize(Decimal("1")), "f")
    return format(decimal_value.normalize(), "f")


def _serialize_events(events) -> list[dict]:
    out: list[dict] = []
    for ev in events:
        out.append(
            {
                "timestamp": ev.timestamp,
                "asset": ev.asset,
                "qty_sold": _stable_decimal(ev.qty_sold),
                "proceeds": _stable_decimal(ev.proceeds),
                "cost_basis": _stable_decimal(ev.cost_basis),
                "gain": _stable_decimal(ev.gain),
                "quote_asset": ev.quote_asset,
                "fee_applied": _stable_decimal(ev.fee_applied),
                "matches": [
                    {
                        "from_qty": _stable_decimal(match.from_qty),
                        "lot_cost_per_unit": _stable_decimal(match.lot_cost_per_unit),
                        "lot_cost_total": _stable_decimal(match.lot_cost_total),
                        "acquired_at": None
                        if match.acquired_at is None
                        else match.acquired_at.isoformat(),
                    }
                    for match in ev.matches
                ],
            }
        )
    return out


def _hr_match_payload(event) -> list[dict]:
    disposed_qty = Decimal(str(event.qty_sold))
    payload: list[dict] = []
    for match in event.matches:
        match_qty = Decimal(str(match.from_qty))
        proceeds_eur = Decimal(str(event.proceeds)) * (match_qty / disposed_qty)
        payload.append(
            {
                "proceeds_eur": str(proceeds_eur),
                "cost_eur": str(match.lot_cost_total),
                "acquired_at": None if match.acquired_at is None else match.acquired_at.isoformat(),
                "disposed_at": event.timestamp,
            }
        )
    return payload


def test_normalized_csv_to_fifo_and_hr_tax_split_matches_golden_file():
    expected = json.loads(GOLDEN_EXPECTED.read_text(encoding="utf-8-sig"))

    rows, errors, meta = parse_csv_with_meta(GOLDEN_CSV.read_bytes(), filename=GOLDEN_CSV.name)

    assert errors == expected["source"]["errors"]
    assert len(rows) == expected["source"]["valid_rows"]
    assert meta["recognized_source_id"] == expected["source"]["recognized_source_id"]

    events, summary, warnings = compute_fifo(rows)

    assert warnings == []
    assert _serialize_events(events) == expected["events"]
    assert summary == expected["summary"]

    assert len(events) == 1
    hr_expected = expected["jurisdiction"]["HR"]
    taxable_gain, exempt_gain = split_taxable_exempt_gain(
        rule=get_rule("HR"),
        gain_eur=Decimal(str(events[0].gain)),
        matches_raw=_hr_match_payload(events[0]),
        ctx=RunContext(
            cfg=CalcConfig(jurisdiction="HR", tax_year=hr_expected["tax_year"], round_dp=2),
            tax_year=hr_expected["tax_year"],
        ),
    )

    assert taxable_gain == Decimal(hr_expected["taxable_gain_eur"])
    assert exempt_gain == Decimal(hr_expected["exempt_gain_eur"])
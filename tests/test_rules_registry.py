from decimal import Decimal

import pytest

from cryptotaxcalc.rules.registry import supported_jurisdictions, get_rule, split_taxable_exempt_gain
from cryptotaxcalc.rules.hr import HrRule
from cryptotaxcalc.rules.it import ItRule
from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.schemas import CalcConfig


class _DummyTx:
    def __init__(self, t: str, tx_id: int = 1):
        self.type = t
        self.id = tx_id


def test_supported_jurisdictions_contains_hr_it():
    codes = supported_jurisdictions()
    assert "HR" in codes
    assert "IT" in codes


def test_get_rule_returns_correct_instances():
    assert isinstance(get_rule("HR"), HrRule)
    assert isinstance(get_rule("hr"), HrRule)
    assert isinstance(get_rule("IT"), ItRule)
    assert isinstance(get_rule("it"), ItRule)

    with pytest.raises(ValueError):
        get_rule("ZZ")


def test_registry_split_uses_hr_custom_splitter():
    cfg = CalcConfig(jurisdiction="HR", holding_exemption_days=730, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)
    rule = get_rule("HR")

    matches = [
        # Long-term gain: exempt
        {
            "proceeds_eur": "1000",
            "cost_eur": "500",
            "acquired_at": "2020-01-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
        # Short-term gain: taxable
        {
            "proceeds_eur": "1000",
            "cost_eur": "700",
            "acquired_at": "2024-12-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
        # Loss: always reduces taxable base
        {
            "proceeds_eur": "200",
            "cost_eur": "300",
            "acquired_at": "2024-06-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
    ]

    taxable, exempt = split_taxable_exempt_gain(
        rule=rule,
        gain_eur=Decimal("700"),
        matches_raw=matches,
        ctx=ctx,
    )

    assert taxable == Decimal("200")
    assert exempt == Decimal("500")


def test_registry_split_default_path_for_it():
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2024)
    rule = get_rule("IT")

    taxable, exempt = split_taxable_exempt_gain(
        rule=rule,
        gain_eur=Decimal("2000.00"),
        matches_raw=[],
        ctx=ctx,
    )

    assert taxable == Decimal("0")
    assert exempt == Decimal("2000.00")


def test_rule_is_taxable_disposal_contract_smoke():
    hr = get_rule("HR")
    it = get_rule("IT")

    assert hr.is_taxable_disposal(_DummyTx("BUY")) is False
    assert hr.is_taxable_disposal(_DummyTx("SELL")) is True

    assert it.is_taxable_disposal(_DummyTx("BUY")) is False
    assert it.is_taxable_disposal(_DummyTx("SELL")) is True
    assert it.is_taxable_disposal(_DummyTx("TRADE")) is True

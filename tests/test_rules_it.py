from decimal import Decimal

from cryptotaxcalc.rules.it import ItRule
from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.schemas import CalcConfig


def test_it_default_threshold_2024_under_or_equal_2000_is_exempt():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2024)

    assert rule.finalize_taxable_gain(Decimal("2000.00"), ctx) == Decimal("0")


def test_it_default_threshold_2024_over_2000_is_taxable():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2024)

    assert rule.finalize_taxable_gain(Decimal("2000.01"), ctx) == Decimal("2000.01")


def test_it_no_threshold_from_2025_all_gains_taxable():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)

    assert rule.finalize_taxable_gain(Decimal("123.45"), ctx) == Decimal("123.45")


def test_it_losses_reduce_taxable_base():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)

    assert rule.finalize_taxable_gain(Decimal("-100.129"), ctx) == Decimal("-100.13")


def test_it_threshold_override_still_supported():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=Decimal("51645.69"), round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)

    assert rule.finalize_taxable_gain(Decimal("123.45"), ctx) == Decimal("0")

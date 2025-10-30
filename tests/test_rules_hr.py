from decimal import Decimal
from rules.hr import HrRule
from rules.base import Match, RunContext
from cryptotaxcalc.schemas import CalcConfig


def test_hr_finalize_no_change():
    rule = HrRule()
    cfg = CalcConfig(jurisdiction="HR")
    ctx = RunContext(cfg=cfg, tax_year=2024)
    assert rule.finalize_taxable_gain(Decimal("100.00"), ctx) == Decimal("100.00")

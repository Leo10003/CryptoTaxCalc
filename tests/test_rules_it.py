from decimal import Decimal
from rules.it import ItRule
from rules.base import RunContext
from cryptotaxcalc.schemas import CalcConfig

def test_it_threshold_gate():
    rule = ItRule()
    cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=Decimal("51645.69"))
    ctx = RunContext(cfg=cfg, tax_year=2024)
    # Phase-1 placeholder: returns 0 when threshold set (to be refined)
    assert rule.finalize_taxable_gain(Decimal("123.45"), ctx) == Decimal("0")

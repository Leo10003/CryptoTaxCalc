import json
from decimal import Decimal
from pathlib import Path

from cryptotaxcalc.rules.registry import get_rule, split_taxable_exempt_gain
from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.schemas import CalcConfig


_FIX_DIR = Path(__file__).parent / "fixtures" / "jurisdictions"


def test_hr_fixture_matches():
    matches = json.loads((_FIX_DIR / "hr_matches.json").read_text(encoding="utf-8"))

    cfg = CalcConfig(jurisdiction="HR", holding_exemption_days=730, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)
    rule = get_rule("HR")

    taxable, exempt = split_taxable_exempt_gain(
        rule=rule,
        gain_eur=Decimal("700"),
        matches_raw=matches,
        ctx=ctx,
    )

    assert taxable == Decimal("200")
    assert exempt == Decimal("500")


def test_it_fixture_cases():
    cases = json.loads((_FIX_DIR / "it_cases.json").read_text(encoding="utf-8"))
    rule = get_rule("IT")

    for c in cases:
        cfg = CalcConfig(jurisdiction="IT", it_threshold_eur=None, round_dp=2)
        ctx = RunContext(cfg=cfg, tax_year=int(c["tax_year"]))

        taxable, _exempt = split_taxable_exempt_gain(
            rule=rule,
            gain_eur=Decimal(str(c["gain_eur"])),
            matches_raw=[],
            ctx=ctx,
        )

        assert taxable == Decimal(str(c["expected_taxable"]))

def test_xx_fixture_cases():
    cases = json.loads((_FIX_DIR / "xx_cases.json").read_text(encoding="utf-8"))
    rule = get_rule("XX")

    for c in cases:
        cfg = CalcConfig(jurisdiction="XX", round_dp=2)
        ctx = RunContext(cfg=cfg, tax_year=int(c["tax_year"]))

        taxable, _exempt = split_taxable_exempt_gain(
            rule=rule,
            gain_eur=Decimal(str(c["gain_eur"])),
            matches_raw=[],
            ctx=ctx,
        )

        assert taxable == Decimal(str(c["expected_taxable"]))

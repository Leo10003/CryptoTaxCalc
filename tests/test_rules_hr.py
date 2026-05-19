from decimal import Decimal

from cryptotaxcalc.rules.hr import compute_taxable_gain_for_matches_hr
from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.schemas import CalcConfig


def test_hr_holding_exemption_split():
    cfg = CalcConfig(jurisdiction="HR", holding_exemption_days=730, round_dp=2)
    ctx = RunContext(cfg=cfg, tax_year=2025)

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

    total, taxable, exempt = compute_taxable_gain_for_matches_hr(matches, ctx, fx_rate=Decimal("1"))

    assert total == Decimal("700")
    assert taxable == Decimal("200")
    assert exempt == Decimal("500")

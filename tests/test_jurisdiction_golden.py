from decimal import Decimal

import pytest

from cryptotaxcalc.rules.base import RunContext
from cryptotaxcalc.rules.registry import get_rule, split_taxable_exempt_gain
from cryptotaxcalc.schemas import CalcConfig


def _ctx(jurisdiction: str, *, tax_year: int, **cfg_overrides) -> RunContext:
    return RunContext(
        cfg=CalcConfig(jurisdiction=jurisdiction, round_dp=2, **cfg_overrides),
        tax_year=tax_year,
    )


def _split(jurisdiction: str, *, gain_eur: str, matches_raw: list[dict], tax_year: int, **cfg_overrides):
    return split_taxable_exempt_gain(
        rule=get_rule(jurisdiction),
        gain_eur=Decimal(gain_eur),
        matches_raw=matches_raw,
        ctx=_ctx(jurisdiction, tax_year=tax_year, **cfg_overrides),
    )


def test_hr_golden_holding_period_boundary_unknown_dates_and_losses():
    matches = [
        {
            "proceeds_eur": "1000",
            "cost_eur": "600",
            "acquired_at": "2023-01-02T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "proceeds_eur": "900",
            "cost_eur": "500",
            "acquired_at": "2023-01-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "proceeds_eur": "300",
            "cost_eur": "100",
            "acquired_at": None,
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "proceeds_eur": "100",
            "cost_eur": "250",
            "acquired_at": "2020-01-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
    ]

    taxable, exempt = _split("HR", gain_eur="850", matches_raw=matches, tax_year=2025, holding_exemption_days=730)

    assert taxable == Decimal("450.00")
    assert exempt == Decimal("400.00")


def test_hr_golden_custom_holding_period_threshold_is_strictly_greater_than_threshold():
    matches = [
        {
            "proceeds_eur": "500",
            "cost_eur": "100",
            "acquired_at": "2024-01-01T00:00:00+00:00",
            "disposed_at": "2024-12-31T00:00:00+00:00",
        },
        {
            "proceeds_eur": "700",
            "cost_eur": "200",
            "acquired_at": "2024-01-01T00:00:00+00:00",
            "disposed_at": "2025-01-01T00:00:00+00:00",
        },
    ]

    taxable, exempt = _split("HR", gain_eur="900", matches_raw=matches, tax_year=2025, holding_exemption_days=365)

    assert taxable == Decimal("400.00")
    assert exempt == Decimal("500.00")


@pytest.mark.parametrize(
    ("tax_year", "gain_eur", "expected_taxable", "expected_exempt"),
    [
        (2023, "1999.999", "0", "1999.999"),
        (2024, "2000.00", "0", "2000.00"),
        (2024, "2000.005", "2000.01", "-0.005"),
        (2025, "0.01", "0.01", "0.00"),
        (2025, "123.455", "123.46", "-0.005"),
        (2025, "-100.129", "-100.13", "0.001"),
    ],
)
def test_it_golden_default_year_aware_threshold_and_rounding(tax_year, gain_eur, expected_taxable, expected_exempt):
    taxable, exempt = _split("IT", gain_eur=gain_eur, matches_raw=[], tax_year=tax_year, it_threshold_eur=None)

    assert taxable == Decimal(expected_taxable)
    assert exempt == Decimal(expected_exempt)


@pytest.mark.parametrize(
    ("gain_eur", "expected_taxable", "expected_exempt"),
    [
        ("51645.69", "0", "51645.69"),
        ("51645.70", "51645.70", "0.00"),
        ("-1.234", "-1.23", "-0.004"),
    ],
)
def test_it_golden_threshold_override_remains_supported(gain_eur, expected_taxable, expected_exempt):
    taxable, exempt = _split(
        "IT",
        gain_eur=gain_eur,
        matches_raw=[],
        tax_year=2025,
        it_threshold_eur=Decimal("51645.69"),
    )

    assert taxable == Decimal(expected_taxable)
    assert exempt == Decimal(expected_exempt)
from __future__ import annotations

"""cryptotaxcalc.rules.registry

Single, explicit registry for jurisdiction rule modules.

Design goals:
- One source of truth for supported jurisdictions.
- Pluggable per-country modules: add a new <code>.py rule module and register it here.
- Deterministic behavior (no filesystem scanning, demo-safe and PyInstaller-safe).
"""

from decimal import Decimal
from typing import Callable, Dict, List, Tuple

from cryptotaxcalc.logging_setup import get_logger
from cryptotaxcalc.rules.base import RunContext, TaxRule

from cryptotaxcalc.rules.hr import HrRule
from cryptotaxcalc.rules.it import ItRule
from cryptotaxcalc.rules.xx import XxRule

logger = get_logger("rules.registry")

_RULE_FACTORIES: Dict[str, Callable[[], TaxRule]] = {
    "HR": HrRule,
    "IT": ItRule,
    "XX": XxRule,
}


def supported_jurisdictions() -> List[str]:
    return sorted(_RULE_FACTORIES.keys())


def get_rule(jurisdiction: str) -> TaxRule:
    code = (jurisdiction or "").strip().upper()
    if code not in _RULE_FACTORIES:
        raise ValueError(
            f"Unsupported jurisdiction: {code!r}. Supported: {', '.join(supported_jurisdictions())}"
        )
    return _RULE_FACTORIES[code]()


def split_taxable_exempt_gain(
    *,
    rule: TaxRule,
    gain_eur: Decimal,
    matches_raw: List[dict],
    ctx: RunContext,
) -> Tuple[Decimal, Decimal]:
    """Return (taxable_gain_eur, exempt_gain_eur) for the given jurisdiction rule.

    Default behavior:
      taxable = rule.finalize_taxable_gain(total_gain)
      exempt  = total_gain - taxable

    Jurisdictions with match-level exemptions (e.g., HR holding period) may expose
    a method `split_taxable_exempt_gain(gain_eur, matches_raw, ctx)` on the rule instance.
    If present, it is used.
    """
    splitter = getattr(rule, "split_taxable_exempt_gain", None)
    if callable(splitter):
        taxable, exempt = splitter(gain_eur=gain_eur, matches_raw=matches_raw, ctx=ctx)
        return taxable, exempt

    taxable = rule.finalize_taxable_gain(gain_eur, ctx)
    exempt = gain_eur - taxable
    return taxable, exempt

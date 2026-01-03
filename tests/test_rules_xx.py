from decimal import Decimal

from cryptotaxcalc.rules.registry import get_rule
from cryptotaxcalc.schemas import CalcConfig
from cryptotaxcalc.rules.base import RunContext


class _Tx:
    def __init__(self, t: str):
        self.type = t


def test_xx_taxable_disposal_contract():
    rule = get_rule("XX")
    assert rule.is_taxable_disposal(_Tx("BUY")) is False
    assert rule.is_taxable_disposal(_Tx("SELL")) is True

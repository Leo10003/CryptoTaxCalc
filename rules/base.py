from __future__ import annotations
from typing import Protocol
from decimal import Decimal
from dataclasses import dataclass
from cryptotaxcalc.models import TransactionRow
from cryptotaxcalc.schemas import CalcConfig

@dataclass
class Match:
    # expand as you wire into fifo_engine
    qty: Decimal
    proceeds_eur: Decimal
    cost_eur: Decimal

@dataclass
class RunContext:
    cfg: CalcConfig
    tax_year: int

class TaxRule(Protocol):
    def is_taxable_disposal(self, tx: TransactionRow) -> bool: ...
    def apply_exemptions(self, matches: list[Match], tx: TransactionRow, ctx: RunContext) -> list[Match]: ...
    def finalize_taxable_gain(self, gain_eur: Decimal, ctx: RunContext) -> Decimal: ...

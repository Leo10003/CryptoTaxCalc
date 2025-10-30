from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from sqlalchemy.orm import Session
from .schemas import CalcConfig, RunSummary, RunTotals
from cryptotaxcalc.rules.base import RunContext, Match, TaxRule
from cryptotaxcalc.rules.hr import HrRule
from cryptotaxcalc.rules.it import ItRule
from .models import TransactionRow, RealizedEvent, CalcRun

# from .fifo_engine import match_fifo  # integrate your real matcher


def _rule_for(cfg: CalcConfig) -> TaxRule:
    return HrRule() if cfg.jurisdiction == "HR" else ItRule()


def run_calculation(session: Session, run: CalcRun, cfg: CalcConfig) -> RunSummary:
    rule = _rule_for(cfg)
    ctx = RunContext(cfg=cfg, tax_year=datetime.utcnow().year)

    totals = RunTotals()
    lots_processed = 0

    # 1) Load candidate transactions (simplified: all for now)
    txs = session.query(TransactionRow).order_by(TransactionRow.timestamp.asc()).all()

    # 2) For each taxable disposal, match buys (Phase-1: placeholder skeleton)
    for tx in txs:
        if not rule.is_taxable_disposal(tx):
            continue

        # TODO: replace with your fifo_engine output (list[Match])
        matches: list[Match] = []  # e.g., Match(qty, proceeds_eur, cost_eur)

        # 3) Apply exemptions / per-jurisdiction transforms
        matches = rule.apply_exemptions(matches, tx, ctx)

        # 4) Aggregate and persist realized rows
        for m in matches:
            lots_processed += 1
            gain = m.proceeds_eur - m.cost_eur
            totals.proceeds_eur += m.proceeds_eur
            totals.cost_eur += m.cost_eur
            totals.gain_eur += gain

            session.add(
                RealizedEvent(
                    run_id=run.id,
                    tx_id=tx.id,
                    timestamp=tx.timestamp.isoformat(),
                    asset=tx.base_asset,
                    qty_sold=str(m.qty),
                    proceeds=str(m.proceeds_eur),
                    cost_basis=str(m.cost_eur),
                    gain=str(gain),
                    quote_asset=tx.quote_asset,
                    fee_applied=None,
                    matches_json=None,
                )
            )

    # 5) Post-process taxable total (IT threshold, etc.)
    totals.taxable_gain_eur = rule.finalize_taxable_gain(totals.gain_eur, ctx)

    session.flush()  # persist realized rows now

    return RunSummary(
        run_id=run.id,
        jurisdiction=cfg.jurisdiction,
        rule_version=cfg.rule_version,
        tax_year=ctx.tax_year,
        fx_batch_id=None,
        lots_processed=lots_processed,
        totals=totals,
    )

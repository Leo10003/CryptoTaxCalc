from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.calc_runner import run_calculation
from cryptotaxcalc.models import Base, CalcRun, TransactionRow
from cryptotaxcalc.schemas import CalcConfig

pytestmark = pytest.mark.smoke


def _tx(
    *,
    ts: str,
    tx_type: str,
    asset: str,
    qty: str,
    quote_amount: str,
    memo: str,
) -> TransactionRow:
    return TransactionRow(
        hash=f"trace-{memo}",
        timestamp=datetime.fromisoformat(ts),
        type=tx_type,
        base_asset=asset,
        base_amount=Decimal(qty),
        quote_asset="EUR",
        quote_amount=Decimal(quote_amount),
        fee_asset="EUR",
        fee_amount=Decimal("0"),
        exchange="TraceTest",
        memo=memo,
    )


def test_calculation_writes_per_run_trace_file(tmp_path, monkeypatch):
    import cryptotaxcalc.calc_runner as calc_runner

    monkeypatch.setattr(calc_runner, "CALC_LOG_DIR", tmp_path / "logs" / "calc")
    monkeypatch.setattr(calc_runner, "CALC_RUNS_LOG_DIR", tmp_path / "logs" / "calc" / "runs")

    engine = create_engine(f"sqlite:///{tmp_path / 'trace_test.sqlite'}")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    db_session = SessionLocal()
    try:
        buy = _tx(
            ts="2022-01-01T12:00:00",
            tx_type="BUY",
            asset="BTC",
            qty="0.10",
            quote_amount="1000",
            memo="buy",
        )
        sell = _tx(
            ts="2024-01-01T12:00:00",
            tx_type="SELL",
            asset="BTC",
            qty="0.04",
            quote_amount="600",
            memo="sell",
        )

        db_session.add_all([buy, sell])
        db_session.commit()

        run = CalcRun(
            started_at=datetime.now(timezone.utc),
            jurisdiction="HR",
            rule_version="test-rule",
            tax_year=2024,
            run_id="trace-test-run",
        )
        db_session.add(run)
        db_session.commit()

        cfg = CalcConfig(
            jurisdiction="HR",
            rule_version="test-rule",
            tax_year=2024,
        )

        summary = run_calculation(db_session, run, cfg)

        trace_path = tmp_path / "logs" / "calc" / "runs" / str(run.id) / "trace.json"
        latest_path = tmp_path / "logs" / "calc" / "last_run.json"

        assert trace_path.exists()
        assert latest_path.exists()

        payload = json.loads(trace_path.read_text(encoding="utf-8"))

        assert payload["run_id"] == run.id
        assert payload["jurisdiction"] == "HR"
        assert payload["rule_version"] == "test-rule"
        assert payload["tax_year"] == 2024
        assert payload["persist_events"] is True
        assert payload["tx_count"] == 2
        assert payload["events_count"] == 1
        assert payload["warnings_count"] == len(summary.warnings)
        assert payload["strict_fx"] is False
        assert payload["totals"]["proceeds_eur"] == "600"
        assert payload["totals"]["cost_eur"] == "400.00"
        assert payload["totals"]["gain_eur"] == "200.00"
        assert "timings_ms" in payload
        assert "summary" in payload

        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        assert latest["run_id"] == run.id
        assert latest["summary"]["run_id"] == run.id
    finally:
        db_session.close()
from __future__ import annotations

import csv
import io
import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.app import app
from cryptotaxcalc.db import SessionLocal, engine, init_db
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.models import Base, CalcRun, FXRate, RealizedEvent, Transaction as DbTransaction
from cryptotaxcalc.schemas import Transaction

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _tx(
    *,
    timestamp: datetime,
    type: str,
    base_asset: str,
    base_amount: str,
    quote_asset: str,
    quote_amount: str,
    fee_asset: str | None = None,
    fee_amount: str | None = None,
) -> Transaction:
    return Transaction(
        timestamp=timestamp,
        type=type,
        base_asset=base_asset,
        base_amount=Decimal(base_amount),
        quote_asset=quote_asset,
        quote_amount=Decimal(quote_amount),
        fee_asset=fee_asset,
        fee_amount=Decimal(fee_amount) if fee_amount is not None else None,
    )


def _memory_session():
    mem_engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=mem_engine)
    Session = sessionmaker(bind=mem_engine, future=True)
    return mem_engine, Session()


def _ensure_schema() -> None:
    init_db(engine)
    Base.metadata.create_all(bind=engine)


def _delete_run(run_id: int) -> None:
    with SessionLocal() as db:
        db.execute(text("DELETE FROM realized_events WHERE run_id = :run_id"), {"run_id": run_id})
        db.execute(text("DELETE FROM calc_runs WHERE id = :run_id"), {"run_id": run_id})
        db.commit()


def _create_precise_export_run() -> int:
    _ensure_schema()
    asset = f"D{uuid.uuid4().hex[:10]}".upper()
    with SessionLocal() as db:
        run = CalcRun(
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            finished_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            jurisdiction="HR",
            rule_version="2025.1",
            tax_year=2025,
            lot_method="FIFO",
            fx_set_id=None,
            params_json={"jurisdiction": "HR", "tax_year": 2025},
            run_id=f"decimal-precision-{uuid.uuid4()}",
            summary_json={},
        )
        db.add(run)
        db.flush()
        run_id = int(run.id)
        db.add(
            RealizedEvent(
                run_id=run_id,
                tx_id=None,
                timestamp="2025-06-01T00:00:00+00:00",
                asset=asset,
                qty_sold=Decimal("0.123456789123456789"),
                proceeds=Decimal("123456789.123456789123456789"),
                cost_basis=Decimal("0.000000019999999999"),
                gain=Decimal("123456789.123456769123456790"),
                quote_asset="EUR",
                fee_applied=Decimal("0.000000000000000001"),
                matches_json=json.dumps(
                    [
                        {
                            "from_qty": "0.123456789123456789",
                            "lot_cost_per_unit": "0.000000162000001326",
                            "lot_cost_total": "0.000000019999999999",
                        }
                    ]
                ),
            )
        )
        db.commit()
    return run_id


def test_sqlite_decimal_columns_round_trip_as_decimal_strings_not_floats():
    mem_engine, db = _memory_session()
    try:
        tx = DbTransaction(
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            type="BUY",
            base_asset="BTC",
            base_amount=Decimal("0.123456789123456789"),
            quote_asset="EUR",
            quote_amount=Decimal("123456789.123456789123456789"),
            fee_asset="EUR",
            fee_amount=Decimal("0.000000000000000001"),
            fair_value=Decimal("999999999.999999999999999999"),
        )
        db.add(tx)
        db.add(FXRate(date=datetime(2025, 1, 1).date(), base="USD", quote="EUR", rate=Decimal("0.912345678912345678")))
        db.commit()

        loaded_tx = db.query(DbTransaction).one()
        loaded_fx = db.query(FXRate).one()
        assert loaded_tx.base_amount == Decimal("0.123456789123456789")
        assert loaded_tx.quote_amount == Decimal("123456789.123456789123456789")
        assert loaded_tx.fee_amount == Decimal("0.000000000000000001")
        assert loaded_tx.fair_value == Decimal("999999999.999999999999999999")
        assert loaded_fx.rate == Decimal("0.912345678912345678")

        with mem_engine.connect() as conn:
            raw_tx = conn.execute(
                text("SELECT base_amount, quote_amount, fee_amount, fair_value FROM transactions")
            ).fetchone()
            raw_fx = conn.execute(text("SELECT rate FROM fx_rates")).fetchone()

        assert raw_tx == (
            "0.123456789123456789",
            "123456789.123456789123456789",
            "0.000000000000000001",
            "999999999.999999999999999999",
        )
        assert raw_fx == ("0.912345678912345678",)
    finally:
        db.close()


def test_fifo_repeating_decimal_cost_basis_is_not_truncated_to_binary_float():
    txs = [
        _tx(
            timestamp=_dt(2025, 1, 1),
            type="buy",
            base_asset="BTC",
            base_amount="3",
            quote_asset="EUR",
            quote_amount="1",
        ),
        _tx(
            timestamp=_dt(2025, 2, 1),
            type="sell",
            base_asset="BTC",
            base_amount="1",
            quote_asset="EUR",
            quote_amount="0.5",
        ),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    event = events[0]
    assert event.cost_basis == Decimal("0.3333333333333333333333333333")
    assert event.gain == Decimal("0.1666666666666666666666666667")
    assert event.matches[0].lot_cost_per_unit == Decimal("0.3333333333333333333333333333")
    assert event.matches[0].lot_cost_total == Decimal("0.3333333333333333333333333333")
    assert summary["by_quote_asset"]["EUR"]["cost_basis"] == "0.33333333"
    assert summary["by_quote_asset"]["EUR"]["gain"] == "0.16666667"


def test_base_asset_fee_precision_is_preserved_in_fifo_disposed_quantity_and_cost_basis():
    txs = [
        _tx(
            timestamp=_dt(2025, 1, 1),
            type="buy",
            base_asset="ETH",
            base_amount="1.000000000000000001",
            quote_asset="EUR",
            quote_amount="1000.000000000000000001",
        ),
        _tx(
            timestamp=_dt(2025, 2, 1),
            type="sell",
            base_asset="ETH",
            base_amount="0.333333333333333333",
            quote_asset="EUR",
            quote_amount="500.000000000000000001",
            fee_asset="ETH",
            fee_amount="0.000000000000000001",
        ),
    ]

    events, _, warnings = compute_fifo(txs)

    assert len(warnings) == 1
    assert "applied base-asset fees" in warnings[0]
    event = events[0]
    assert event.qty_sold == Decimal("0.333333333333333334")
    assert event.cost_basis == Decimal("333.333333333333333667")
    assert event.gain == Decimal("166.666666666666666334")
    assert event.fee_applied == Decimal("1.500000000000000001503000000E-15")


def test_events_csv_export_preserves_decimal_strings_without_scientific_notation_or_float_rounding():
    run_id = _create_precise_export_run()
    try:
        response = client.get(f"/export/events_csv?run_id={run_id}")

        assert response.status_code == 200, response.text
        rows = list(csv.DictReader(io.StringIO(response.text)))
        assert len(rows) == 1
        row = rows[0]
        assert row["qty_sold"] == "0.123456789123456789"
        assert row["proceeds_eur"] == "123456789.123456789123456789"
        assert row["cost_basis_eur"] == "0.000000019999999999"
        assert row["gain_eur"] == "123456789.123456769123456790"
        assert row["fee_applied_eur"] == "0.000000000000000001"
        assert "E" not in row["fee_applied_eur"].upper()
        assert Decimal(row["proceeds_eur"]) == Decimal("123456789.123456789123456789")
        assert Decimal(row["gain_eur"]) == Decimal("123456789.123456769123456790")
    finally:
        _delete_run(run_id)
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.calc_runner import run_calculation
from cryptotaxcalc.csv_normalizer import parse_csv_with_meta
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.models import Base, CalcRun, RealizedEvent, Transaction as DbTransaction, TxType, WalletOutOverride
from cryptotaxcalc.schemas import CalcConfig, Transaction

pytestmark = pytest.mark.smoke


def _dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


def _tx(
    *,
    timestamp: datetime,
    type: str,
    base_asset: str,
    base_amount: str,
    quote_asset: str | None = None,
    quote_amount: str | None = None,
    fee_asset: str | None = None,
    fee_amount: str | None = None,
) -> Transaction:
    return Transaction(
        timestamp=timestamp,
        type=type,
        base_asset=base_asset,
        base_amount=Decimal(base_amount),
        quote_asset=quote_asset,
        quote_amount=Decimal(quote_amount) if quote_amount is not None else None,
        fee_asset=fee_asset,
        fee_amount=Decimal(fee_amount) if fee_amount is not None else None,
    )


def _db_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def _run(db, jurisdiction: str = "HR"):
    run = CalcRun(
        started_at=datetime.now(timezone.utc),
        jurisdiction=jurisdiction,
        tax_year=2025,
        lot_method="FIFO",
        params_json={},
        run_id=f"wallet-transfer-test-{id(db)}",
    )
    db.add(run)
    db.commit()
    summary = run_calculation(
        db,
        run,
        CalcConfig(jurisdiction=jurisdiction, tax_year=2025, strict_fx=False),
    )
    return run, summary


def _business_warnings(warnings: list[str]) -> list[str]:
    ignored_prefixes = (
        "FX check:",
        "Price autosync diag:",
    )
    return [
        warning
        for warning in warnings
        if not str(warning).startswith(ignored_prefixes)
    ]


def test_fifo_transfer_aliases_do_not_create_events_or_reduce_available_lots():
    asset = "WALLETX"
    txs = [
        _tx(timestamp=_dt(2025, 1, 1), type="buy", base_asset=asset, base_amount="1", quote_asset="EUR", quote_amount="1000"),
        _tx(timestamp=_dt(2025, 1, 2), type="transfer_in", base_asset=asset, base_amount="10", quote_asset=asset, quote_amount="10"),
        _tx(timestamp=_dt(2025, 1, 3), type="transfer-out", base_asset=asset, base_amount="0.5", quote_asset=asset, quote_amount="0.5"),
        _tx(timestamp=_dt(2025, 1, 4), type="deposit", base_asset=asset, base_amount="3"),
        _tx(timestamp=_dt(2025, 1, 5), type="withdrawal", base_asset=asset, base_amount="1"),
        _tx(timestamp=_dt(2025, 1, 6), type="send", base_asset=asset, base_amount="0.2"),
        _tx(timestamp=_dt(2025, 1, 7), type="receive", base_asset=asset, base_amount="0.2"),
        _tx(timestamp=_dt(2025, 2, 1), type="sell", base_asset=asset, base_amount="1", quote_asset="EUR", quote_amount="1500"),
    ]

    events, summary, warnings = compute_fifo(txs)

    assert warnings == []
    assert len(events) == 1
    assert events[0].asset == asset
    assert events[0].qty_sold == Decimal("1")
    assert events[0].proceeds == Decimal("1500")
    assert events[0].cost_basis == Decimal("1000")
    assert events[0].gain == Decimal("500")
    assert len(events[0].matches) == 1
    assert events[0].matches[0].from_qty == Decimal("1")
    assert summary["by_quote_asset"]["EUR"]["gain"] == "500.00000000"


def test_generic_csv_transfer_aliases_normalize_to_transfer_and_do_not_create_fifo_events():
    raw = b"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,transfer_in,BTC,0.5,,,,,Ledger,deposit to wallet
2025-01-02T00:00:00Z,transfer-out,BTC,0.25,,,,,Ledger,withdraw from exchange
"""

    rows, errors, meta = parse_csv_with_meta(raw, filename="wallet_transfer_aliases.csv")
    events, summary, warnings = compute_fifo(rows)

    assert errors == []
    assert meta["recognized_source_id"] == "cryptotaxcalc_generic"
    assert [row.type for row in rows] == ["transfer", "transfer"]
    assert events == []
    assert warnings == []
    assert summary["totals"] == {"proceeds": "0", "cost_basis": "0", "gain": "0"}


def test_run_calculation_treats_unclassified_wallet_transfer_out_as_non_taxable_and_keeps_lot_available():
    db = _db_session()
    asset = "WALLETUNCLASS"
    try:
        db.add_all(
            [
                DbTransaction(
                    timestamp=_dt(2025, 1, 1),
                    type=TxType.BUY,
                    base_asset=asset,
                    base_amount=Decimal("1"),
                    quote_asset="EUR",
                    quote_amount=Decimal("1000"),
                    fee_asset="EUR",
                    fee_amount=Decimal("0"),
                ),
                DbTransaction(
                    timestamp=_dt(2025, 1, 2),
                    type=TxType.TRANSFER_OUT,
                    base_asset=asset,
                    base_amount=Decimal("-0.75"),
                    quote_asset=None,
                    quote_amount=None,
                    fee_asset=asset,
                    fee_amount=Decimal("0.001"),
                ),
                DbTransaction(
                    timestamp=_dt(2025, 2, 1),
                    type=TxType.SELL,
                    base_asset=asset,
                    base_amount=Decimal("1"),
                    quote_asset="EUR",
                    quote_amount=Decimal("1500"),
                    fee_asset="EUR",
                    fee_amount=Decimal("0"),
                ),
            ]
        )
        db.commit()

        run, summary = _run(db)
        events = db.query(RealizedEvent).filter(RealizedEvent.run_id == run.id).all()

        assert _business_warnings(summary.warnings) == []
        assert len(events) == 1
        assert events[0].asset == asset
        assert events[0].qty_sold == Decimal("1")
        assert events[0].cost_basis == Decimal("1000")
        assert events[0].gain == Decimal("500")
    finally:
        db.close()


def test_wallet_transfer_sell_override_creates_taxable_disposal_with_explicit_eur_proceeds_and_ignores_network_fee():
    db = _db_session()
    asset = "WALLETSELL"
    try:
        buy = DbTransaction(
            timestamp=_dt(2025, 1, 1),
            type=TxType.BUY,
            base_asset=asset,
            base_amount=Decimal("1"),
            quote_asset="EUR",
            quote_amount=Decimal("1000"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
        )
        out = DbTransaction(
            timestamp=_dt(2025, 2, 1),
            type=TxType.TRANSFER_OUT,
            base_asset=asset,
            base_amount=Decimal("-0.25"),
            quote_asset=None,
            quote_amount=None,
            fee_asset=asset,
            fee_amount=Decimal("0.001"),
        )
        db.add_all([buy, out])
        db.flush()
        db.add(
            WalletOutOverride(
                transaction_id=out.id,
                classification="sell",
                proceeds_eur=Decimal("400"),
                note="taxable wallet disposal",
            )
        )
        db.commit()

        run, summary = _run(db)
        events = db.query(RealizedEvent).filter(RealizedEvent.run_id == run.id).all()

        assert _business_warnings(summary.warnings) == []
        assert len(events) == 1
        event = events[0]
        assert event.asset == asset
        assert event.qty_sold == Decimal("0.25")
        assert event.proceeds == Decimal("400.00000000")
        assert event.cost_basis == Decimal("250.00")
        assert event.gain == Decimal("150.00000000")
        assert event.fee_applied == Decimal("0")
    finally:
        db.close()


def test_wallet_transfer_buy_override_creates_acquisition_lot_with_explicit_eur_cost_and_ignores_network_fee():
    db = _db_session()
    asset = "WALLETBUY"
    try:
        transfer_in = DbTransaction(
            timestamp=_dt(2025, 1, 1),
            type=TxType.TRANSFER_IN,
            base_asset=asset,
            base_amount=Decimal("2"),
            quote_asset=None,
            quote_amount=None,
            fee_asset=asset,
            fee_amount=Decimal("0.002"),
        )
        later_sell = DbTransaction(
            timestamp=_dt(2025, 2, 1),
            type=TxType.SELL,
            base_asset=asset,
            base_amount=Decimal("0.5"),
            quote_asset="EUR",
            quote_amount=Decimal("600"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
        )
        db.add_all([transfer_in, later_sell])
        db.flush()
        db.add(
            WalletOutOverride(
                transaction_id=transfer_in.id,
                classification="buy",
                proceeds_eur=Decimal("1000"),
                note="classified wallet receipt as buy",
            )
        )
        db.commit()

        run, summary = _run(db)
        events = db.query(RealizedEvent).filter(RealizedEvent.run_id == run.id).all()

        assert _business_warnings(summary.warnings) == []
        assert len(events) == 1
        event = events[0]
        assert event.asset == asset
        assert event.qty_sold == Decimal("0.5")
        assert event.proceeds == Decimal("600")
        assert event.cost_basis == Decimal("250.0")
        assert event.gain == Decimal("350.0")
        assert event.fee_applied == Decimal("0")
    finally:
        db.close()


def test_wallet_transfer_taxable_override_without_proceeds_is_left_as_transfer_with_warning():
    db = _db_session()
    asset = "WALLETNOPROCEEDS"
    try:
        buy = DbTransaction(
            timestamp=_dt(2025, 1, 1),
            type=TxType.BUY,
            base_asset=asset,
            base_amount=Decimal("1"),
            quote_asset="EUR",
            quote_amount=Decimal("1000"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
        )
        out = DbTransaction(
            timestamp=_dt(2025, 2, 1),
            type=TxType.TRANSFER_OUT,
            base_asset=asset,
            base_amount=Decimal("-0.25"),
            quote_asset=None,
            quote_amount=None,
            fee_asset=asset,
            fee_amount=Decimal("0.001"),
        )
        db.add_all([buy, out])
        db.flush()
        db.add(
            WalletOutOverride(
                transaction_id=out.id,
                classification="sell",
                proceeds_eur=None,
                note="incomplete override",
            )
        )
        db.commit()

        run, summary = _run(db)
        events = db.query(RealizedEvent).filter(RealizedEvent.run_id == run.id).all()

        assert events == []
        business_warnings = _business_warnings(summary.warnings)
        assert len(business_warnings) == 1
        assert f"Wallet transfer override missing proceeds_eur for tx_id={out.id}; treated as TRANSFER." in business_warnings
    finally:
        db.close()
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import cryptotaxcalc.calc_runner as calc_runner
from cryptotaxcalc.calc_runner import _normalize_transactions_to_eur
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.fx_utils import clear_fx_cache, ensure_fx_rates_schema
from cryptotaxcalc.schemas import Transaction

pytestmark = pytest.mark.smoke


def _make_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Session = sessionmaker(bind=engine, future=True)
    db = Session()
    ensure_fx_rates_schema(db)
    return db


def _insert_rate(db, day: date, base: str, quote: str, rate: Decimal) -> None:
    db.execute(
        text("INSERT INTO fx_rates (date, base, quote, rate, batch_id) VALUES (:d,:b,:q,:r,:bid)"),
        {"d": day.isoformat(), "b": base.upper(), "q": quote.upper(), "r": str(rate), "bid": 1},
    )
    db.commit()


def _tx(
    *,
    timestamp: datetime,
    type: str,
    base_asset: str,
    base_amount: str,
    quote_asset: str | None = "EUR",
    quote_amount: str | None = "0",
    fee_asset: str | None = "EUR",
    fee_amount: str | None = "0",
    memo: str | None = None,
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
        memo=memo,
    )


def _meta():
    return (
        [],
        {"fallback_days": set(), "fallback_pairs": set()},
        {
            "third_fee_detected": 0,
            "third_fee_valued": 0,
            "missing_price_days": set(),
            "missing_price_pairs": set(),
            "internal_price_used": 0,
            "internal_price_assets": set(),
            "internal_price_fallback_days": set(),
        },
    )


def test_internal_fee_price_derives_same_day_bnb_eur_price_from_user_bnb_usdt_trade(monkeypatch):
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 7, 1), "USD", "EUR", Decimal("0.9"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 7, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_FROM_TRADES", True)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_LOOKBACK_DAYS", 7)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="BNB",
                base_amount="2",
                quote_asset="USDT",
                quote_amount="600",
                fee_asset="USDT",
                fee_amount="0",
                memo="BNB/USDT price source trade",
            ),
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.1",
                memo="ETH buy with internally priced BNB fee",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert warnings == []
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 1
    assert fee_meta["internal_price_used"] == 1
    assert fee_meta["internal_price_assets"] == {"BNB/EUR"}
    assert fee_meta["internal_price_fallback_days"] == set()
    assert len(normalized) == 3

    bnb_buy, eth_buy, synthetic_fee_sale = normalized
    assert bnb_buy.quote_asset == "EUR"
    assert bnb_buy.quote_amount == Decimal("540.00000000")
    assert eth_buy.fee_asset == "EUR"
    assert eth_buy.fee_amount == Decimal("27.00000000")
    assert synthetic_fee_sale.type == "trade"
    assert synthetic_fee_sale.base_asset == "BNB"
    assert synthetic_fee_sale.base_amount == Decimal("0.1")
    assert synthetic_fee_sale.quote_asset == "EUR"
    assert synthetic_fee_sale.quote_amount == Decimal("27.00000000")
    assert "synthetic fee disposal (BNB fee)" in synthetic_fee_sale.memo

    events, summary, fifo_warnings = compute_fifo(normalized)
    assert fifo_warnings == []
    assert len(events) == 1
    assert events[0].asset == "BNB"
    assert events[0].proceeds == Decimal("27.00000000")
    assert events[0].cost_basis == Decimal("27.000000000")
    assert events[0].gain == Decimal("0E-9")
    assert summary["totals"]["gain"] == "0E-9"


def test_internal_fee_price_lookback_uses_recent_user_trade_and_records_audit_fallback_day(monkeypatch):
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 7, 1), "USD", "EUR", Decimal("0.8"))
    _insert_rate(db, date(2025, 7, 4), "USD", "EUR", Decimal("0.8"))
    warnings, fx_meta, fee_meta = _meta()
    price_day = datetime(2025, 7, 1, tzinfo=timezone.utc)
    fee_day = datetime(2025, 7, 4, tzinfo=timezone.utc)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_FROM_TRADES", True)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_LOOKBACK_DAYS", 3)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=price_day,
                type="buy",
                base_asset="BNB",
                base_amount="1",
                quote_asset="USDT",
                quote_amount="300",
                fee_asset="EUR",
                fee_amount="0",
                memo="lookback price source",
            ),
            _tx(
                timestamp=fee_day,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.2",
                memo="fee uses three-day internal lookback",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert warnings == []
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 1
    assert fee_meta["internal_price_used"] == 1
    assert fee_meta["internal_price_assets"] == {"BNB/EUR"}
    assert fee_meta["internal_price_fallback_days"] == {"2025-07-04"}
    assert normalized[1].fee_asset == "EUR"
    assert normalized[1].fee_amount == Decimal("48.00000000")
    assert normalized[2].base_asset == "BNB"
    assert normalized[2].quote_amount == Decimal("48.00000000")


def test_internal_fee_price_outside_lookback_is_not_used_and_missing_price_is_audited(monkeypatch):
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 7, 1), "USD", "EUR", Decimal("0.8"))
    _insert_rate(db, date(2025, 7, 4), "USD", "EUR", Decimal("0.8"))
    warnings, fx_meta, fee_meta = _meta()
    price_day = datetime(2025, 7, 1, tzinfo=timezone.utc)
    fee_day = datetime(2025, 7, 4, tzinfo=timezone.utc)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_FROM_TRADES", True)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_LOOKBACK_DAYS", 2)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=price_day,
                type="buy",
                base_asset="BNB",
                base_amount="1",
                quote_asset="USDT",
                quote_amount="300",
                fee_asset="EUR",
                fee_amount="0",
                memo="too-old price source",
            ),
            _tx(
                timestamp=fee_day,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.2",
                memo="fee outside internal lookback",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(normalized) == 2
    assert normalized[1].fee_asset == "BNB"
    assert normalized[1].fee_amount == Decimal("0.2")
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 0
    assert fee_meta["internal_price_used"] == 0
    assert fee_meta["internal_price_assets"] == set()
    assert fee_meta["internal_price_fallback_days"] == set()
    assert fee_meta["missing_price_days"] == {"2025-07-04"}
    assert fee_meta["missing_price_pairs"] == {"BNB/EUR"}
    assert any(w.startswith("Fee FX lookup debug: asset=BNB day=2025-07-04") for w in warnings)
    assert any(w.startswith("Fee valuation incomplete: missing BNB/EUR price for 2025-07-04") for w in warnings)


def test_internal_fee_price_feature_flag_disabled_keeps_user_trade_prices_from_being_used(monkeypatch):
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 7, 5), "USD", "EUR", Decimal("0.9"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 7, 5, tzinfo=timezone.utc)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_FROM_TRADES", False)
    monkeypatch.setattr(calc_runner, "FEE_INTERNAL_PRICE_LOOKBACK_DAYS", 7)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="BNB",
                base_amount="2",
                quote_asset="USDT",
                quote_amount="600",
                fee_asset="EUR",
                fee_amount="0",
                memo="available but disabled price source",
            ),
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.1",
                memo="fee should not use disabled internal price source",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(normalized) == 2
    assert normalized[1].fee_asset == "BNB"
    assert normalized[1].fee_amount == Decimal("0.1")
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 0
    assert fee_meta["internal_price_used"] == 0
    assert fee_meta["internal_price_assets"] == set()
    assert fee_meta["missing_price_days"] == {"2025-07-05"}
    assert fee_meta["missing_price_pairs"] == {"BNB/EUR"}
    assert any(w.startswith("Fee valuation incomplete: missing BNB/EUR price for 2025-07-05") for w in warnings)
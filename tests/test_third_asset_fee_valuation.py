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
    fair_value: str | None = None,
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
        fair_value=Decimal(fair_value) if fair_value is not None else None,
    )


def _meta():
    return (
        [],
        {"fallback_days": set(), "fallback_pairs": set()},
        {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()},
    )


def test_third_asset_fee_with_eur_price_is_valued_and_creates_synthetic_fee_disposal():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 6, 1), "BNB", "EUR", Decimal("300"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 6, 1, tzinfo=timezone.utc)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="BNB",
                base_amount="1",
                quote_amount="200",
                fee_asset="EUR",
                fee_amount="0",
                memo="BNB acquisition lot",
            ),
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.1",
                memo="ETH buy with BNB fee",
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
    assert fee_meta["missing_price_days"] == set()
    assert fee_meta["missing_price_pairs"] == set()
    assert len(normalized) == 3

    eth_buy = normalized[1]
    synthetic_fee_sale = normalized[2]
    assert eth_buy.base_asset == "ETH"
    assert eth_buy.fee_asset == "EUR"
    assert eth_buy.fee_amount == Decimal("30.00000000")
    assert synthetic_fee_sale.type == "trade"
    assert synthetic_fee_sale.base_asset == "BNB"
    assert synthetic_fee_sale.base_amount == Decimal("0.1")
    assert synthetic_fee_sale.quote_asset == "EUR"
    assert synthetic_fee_sale.quote_amount == Decimal("30.00000000")
    assert "synthetic fee disposal (BNB fee)" in synthetic_fee_sale.memo

    events, summary, fifo_warnings = compute_fifo(normalized)

    assert fifo_warnings == []
    assert len(events) == 1
    assert events[0].asset == "BNB"
    assert events[0].qty_sold == Decimal("0.1")
    assert events[0].proceeds == Decimal("30.00000000")
    assert events[0].cost_basis == Decimal("20.0")
    assert events[0].gain == Decimal("10.00000000")
    assert summary["by_quote_asset"]["EUR"] == {
        "proceeds": "30.00000000",
        "cost_basis": "20.00000000",
        "gain": "10.00000000",
    }


def test_missing_third_asset_fee_price_in_non_strict_mode_is_warned_and_not_silently_applied():
    clear_fx_cache()
    db = _make_session()
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 6, 2, tzinfo=timezone.utc)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="0.1",
                memo="ETH buy with unpriced BNB fee",
            )
        ],
        db=db,
        strict_fx=False,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(normalized) == 1
    assert normalized[0].fee_asset == "BNB"
    assert normalized[0].fee_amount == Decimal("0.1")
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 0
    assert fee_meta["missing_price_days"] == {"2025-06-02"}
    assert fee_meta["missing_price_pairs"] == {"BNB/EUR"}
    assert any(w.startswith("Fee FX lookup debug: asset=BNB day=2025-06-02") for w in warnings)
    assert any(w.startswith("Fee valuation incomplete: missing BNB/EUR price for 2025-06-02") for w in warnings)
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_missing_third_asset_fee_price_in_strict_fee_mode_raises_instead_of_ignoring_fee(monkeypatch):
    clear_fx_cache()
    db = _make_session()
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 6, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(calc_runner, "STRICT_FEE_VALUATION", True)

    with pytest.raises(ValueError) as raised:
        _normalize_transactions_to_eur(
            [
                _tx(
                    timestamp=t0,
                    type="buy",
                    base_asset="ETH",
                    base_amount="1",
                    quote_amount="2000",
                    fee_asset="BNB",
                    fee_amount="0.1",
                    memo="strict missing BNB fee price",
                )
            ],
            db=db,
            strict_fx=True,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_meta,
        )

    assert str(raised.value) == (
        "Strict fee valuation: missing BNB->EUR price for 2025-06-03. "
        "Load daily prices into fx_rates (base=<ASSET>, quote=EUR) and re-run."
    )
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 0
    assert fee_meta["missing_price_days"] == {"2025-06-03"}
    assert fee_meta["missing_price_pairs"] == {"BNB/EUR"}


def test_negative_third_asset_fee_amount_is_abs_normalized_before_valuation():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 6, 4), "BNB", "EUR", Decimal("250"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 6, 4, tzinfo=timezone.utc)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_amount="2000",
                fee_asset="BNB",
                fee_amount="-0.2",
                memo="negative BNB fee from source CSV",
            )
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert any("Fee amount negative at" in warning and "using absolute value" in warning for warning in warnings)
    assert fee_meta["third_fee_detected"] == 1
    assert fee_meta["third_fee_valued"] == 1
    assert len(normalized) == 2
    assert normalized[0].fee_asset == "EUR"
    assert normalized[0].fee_amount == Decimal("50.00000000")
    assert normalized[1].base_asset == "BNB"
    assert normalized[1].base_amount == Decimal("0.2")
    assert normalized[1].quote_amount == Decimal("50.00000000")


def test_base_asset_and_quote_asset_fees_are_not_misclassified_as_third_asset_fees():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 6, 5), "BNB", "EUR", Decimal("999"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 6, 5, tzinfo=timezone.utc)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="BNB",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="200",
                fee_asset="BNB",
                fee_amount="0.01",
                memo="base asset fee",
            ),
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="1",
                quote_asset="EUR",
                quote_amount="2000",
                fee_asset="EUR",
                fee_amount="10",
                memo="quote asset fee",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(normalized) == 2
    assert warnings == []
    assert fee_meta["third_fee_detected"] == 0
    assert fee_meta["third_fee_valued"] == 0
    assert fee_meta["missing_price_days"] == set()
    assert fee_meta["missing_price_pairs"] == set()
    assert normalized[0].fee_asset == "BNB"
    assert normalized[0].fee_amount == Decimal("0.01")
    assert normalized[1].fee_asset == "EUR"
    assert normalized[1].fee_amount == Decimal("10")
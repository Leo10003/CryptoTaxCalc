from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.calc_runner import _normalize_transactions_to_eur
from cryptotaxcalc.fx_utils import clear_fx_cache, ensure_fx_rates_schema, ensure_rate_or_default_lookup
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


def test_strict_fx_rejects_missing_usd_eur_rate_before_fifo_can_produce_output():
    clear_fx_cache()
    db = _make_session()
    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError) as raised:
        _normalize_transactions_to_eur(
            [
                _tx(
                    timestamp=t0,
                    type="buy",
                    base_asset="BTC",
                    base_amount="1",
                    quote_asset="USD",
                    quote_amount="10000",
                    fee_asset="USD",
                    fee_amount="10",
                )
            ],
            db=db,
            strict_fx=True,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_meta,
        )

    assert "Strict FX: missing USD->EUR rate for 2025-01-01" in str(raised.value)
    assert warnings == []
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_non_strict_fx_fallback_converts_usd_at_default_one_and_records_warning_metadata():
    clear_fx_cache()
    db = _make_session()
    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="sell",
                base_asset="BTC",
                base_amount="0.5",
                quote_asset="USD",
                quote_amount="15000",
                fee_asset="USD",
                fee_amount="25",
            )
        ],
        db=db,
        strict_fx=False,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(out) == 1
    assert out[0].quote_asset == "EUR"
    assert out[0].quote_amount == Decimal("15000.00000000")
    assert out[0].fee_asset == "EUR"
    assert out[0].fee_amount == Decimal("25.00000000")
    assert fx_meta["fallback_days"] == {"2025-01-01"}
    assert fx_meta["fallback_pairs"] == {"USD/EUR"}
    assert len(warnings) == 1
    assert "FX integrity warning: missing USD->EUR rate for 2025-01-01" in warnings[0]
    assert "conversion assumed 1.0" in warnings[0]


def test_usd_quote_and_fee_are_converted_with_exact_same_day_fx_rate():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 1, 1), "USD", "EUR", Decimal("0.92"))
    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="2",
                quote_asset="USD",
                quote_amount="4000",
                fee_asset="USD",
                fee_amount="10",
            )
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(out) == 1
    assert out[0].quote_asset == "EUR"
    assert out[0].quote_amount == Decimal("3680.00000000")
    assert out[0].fee_asset == "EUR"
    assert out[0].fee_amount == Decimal("9.20000000")
    assert warnings == []
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_fx_rate_lookup_uses_previous_available_rate_within_lookback_without_fallback_flag():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 1, 3), "USD", "EUR", Decimal("0.91"))

    lookup = ensure_rate_or_default_lookup(
        db,
        date(2025, 1, 5),
        base="USD",
        quote="EUR",
        default_rate=Decimal("1.0"),
        max_lookback_days=7,
    )

    assert lookup.rate == Decimal("0.91")
    assert lookup.matched_date == "2025-01-03"
    assert lookup.looked_back_days == 2
    assert lookup.used_fallback is False


def test_unsupported_non_eur_non_usd_quote_requires_fair_value_before_fifo():
    clear_fx_cache()
    db = _make_session()
    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError) as raised:
        _normalize_transactions_to_eur(
            [
                _tx(
                    timestamp=t0,
                    type="buy",
                    base_asset="ETH",
                    base_amount="1",
                    quote_asset="BTC",
                    quote_amount="0.05",
                )
            ],
            db=db,
            strict_fx=True,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_meta,
        )

    assert "Unsupported quote asset 'BTC'" in str(raised.value)
    assert "Provide fair_value (EUR)" in str(raised.value)
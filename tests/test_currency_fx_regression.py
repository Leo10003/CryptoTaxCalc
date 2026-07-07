from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

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
        fair_value=Decimal(fair_value) if fair_value is not None else None,
    )


def _meta():
    return (
        [],
        {"fallback_days": set(), "fallback_pairs": set()},
        {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()},
    )


def test_eur_quote_and_eur_fee_never_use_fx_fallback_even_when_strict_fx_is_enabled():
    clear_fx_cache()
    db = _make_session()
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 5, 1, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="sell",
                base_asset="EUREXACT",
                base_amount="1.25",
                quote_asset="EUR",
                quote_amount="2500.123456789",
                fee_asset="EUR",
                fee_amount="12.345678901",
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
    assert out[0].quote_amount == Decimal("2500.123456789")
    assert out[0].fee_asset == "EUR"
    assert out[0].fee_amount == Decimal("12.345678901")
    assert warnings == []
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_usd_and_usdc_quote_legs_use_same_usd_eur_rate_without_stablecoin_noise():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 5, 2), "USD", "EUR", Decimal("0.9001"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 5, 2, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="USDLEG",
                base_amount="1",
                quote_asset="USD",
                quote_amount="100",
                fee_asset="USD",
                fee_amount="2",
            ),
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="USDCLEG",
                base_amount="1",
                quote_asset="USDC",
                quote_amount="100",
                fee_asset="USDC",
                fee_amount="2",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert [t.quote_asset for t in out] == ["EUR", "EUR"]
    assert [t.quote_amount for t in out] == [Decimal("90.01000000"), Decimal("90.01000000")]
    assert [t.fee_asset for t in out] == ["EUR", "EUR"]
    assert [t.fee_amount for t in out] == [Decimal("1.80020000"), Decimal("1.80020000")]
    assert warnings == []
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_less_common_usd_stablecoin_conversion_records_single_audit_warning_and_no_fallback_when_rate_exists():
    clear_fx_cache()
    db = _make_session()
    _insert_rate(db, date(2025, 5, 3), "USD", "EUR", Decimal("0.88"))
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 5, 3, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="BUSDLEG",
                base_amount="1",
                quote_asset="BUSD",
                quote_amount="100",
                fee_asset="BUSD",
                fee_amount="3",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert out[0].quote_asset == "EUR"
    assert out[0].quote_amount == Decimal("88.00000000")
    assert out[0].fee_asset == "EUR"
    assert out[0].fee_amount == Decimal("2.64000000")
    assert warnings == ["Stablecoin assumption: BUSD treated as USD for FX conversion (audit note)."]
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_unsupported_crypto_quote_uses_explicit_fair_value_without_fx_fallback_or_quote_leakage():
    clear_fx_cache()
    db = _make_session()
    warnings, fx_meta, fee_meta = _meta()
    t0 = datetime(2025, 5, 4, tzinfo=timezone.utc)

    out = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=t0,
                type="buy",
                base_asset="ETH",
                base_amount="2",
                quote_asset="BTC",
                quote_amount="0.08",
                fee_asset="EUR",
                fee_amount="0",
                fair_value="4800.55",
            ),
        ],
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    assert len(out) == 1
    assert out[0].quote_asset == "EUR"
    assert out[0].quote_amount == Decimal("4800.55")
    assert warnings == ["Used fair_value as EUR quote amount for unsupported quote asset 'BTC' on 2025-05-04 (audit note)."]
    assert fx_meta["fallback_days"] == set()
    assert fx_meta["fallback_pairs"] == set()


def test_non_strict_missing_usd_fx_fallback_is_visible_in_metadata_and_fifo_totals_are_eur_canonical():
    clear_fx_cache()
    db = _make_session()
    warnings, fx_meta, fee_meta = _meta()
    asset = "FXFALLBACK"
    buy_day = datetime(2025, 5, 5, tzinfo=timezone.utc)
    sell_day = datetime(2025, 5, 6, tzinfo=timezone.utc)

    normalized = _normalize_transactions_to_eur(
        [
            _tx(
                timestamp=buy_day,
                type="buy",
                base_asset=asset,
                base_amount="1",
                quote_asset="USD",
                quote_amount="1000",
                fee_asset="USD",
                fee_amount="10",
            ),
            _tx(
                timestamp=sell_day,
                type="sell",
                base_asset=asset,
                base_amount="0.5",
                quote_asset="USDT",
                quote_amount="750",
                fee_asset="USDT",
                fee_amount="5",
            ),
        ],
        db=db,
        strict_fx=False,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    events, summary, fifo_warnings = compute_fifo(normalized)

    assert [t.quote_asset for t in normalized] == ["EUR", "EUR"]
    assert [t.fee_asset for t in normalized] == ["EUR", "EUR"]
    assert fx_meta["fallback_days"] == {"2025-05-05", "2025-05-06"}
    assert fx_meta["fallback_pairs"] == {"USD/EUR"}
    assert len(warnings) == 2
    assert all("FX integrity warning: missing USD->EUR rate" in warning for warning in warnings)
    assert fifo_warnings == []
    assert len(events) == 1
    assert events[0].quote_asset == "EUR"
    assert events[0].proceeds == Decimal("745.00000000")
    assert events[0].cost_basis == Decimal("505.0000")
    assert events[0].gain == Decimal("240.00000000")
    assert summary["totals"] == {
        "proceeds": "745.00000000",
        "cost_basis": "505.000000000",
        "gain": "240.000000000",
    }
import pytest
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.schemas import Transaction
from cryptotaxcalc.calc_runner import _normalize_transactions_to_eur
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.fx_utils import ensure_fx_rates_schema, clear_fx_cache


def _make_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Session = sessionmaker(bind=engine, future=True)
    db = Session()
    ensure_fx_rates_schema(db)
    return db


def _insert_rate(db, day: date, base: str, quote: str, rate: Decimal):
    db.execute(
        text("INSERT INTO fx_rates (date, base, quote, rate, batch_id) VALUES (:d,:b,:q,:r,:bid)"),
        {"d": day.isoformat(), "b": base, "q": quote, "r": str(rate), "bid": 1},
    )
    db.commit()


def test_third_asset_fee_is_valued_and_synthetic_disposal_created():
    clear_fx_cache()
    db = _make_session()

    # Price for fee asset (BNB/EUR) on the same day (no-lookback required)
    d = date(2025, 1, 1)
    _insert_rate(db, d, "BNB", "EUR", Decimal("250"))

    t0 = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

    txs = [
        # BNB lot to give basis for synthetic disposal
        Transaction(timestamp=t0, type="buy", base_asset="BNB", base_amount=Decimal("1"),
                    quote_asset="EUR", quote_amount=Decimal("200")),
        # BTC lot for sale
        Transaction(timestamp=t0 + timedelta(minutes=1), type="buy", base_asset="BTC", base_amount=Decimal("0.01"),
                    quote_asset="EUR", quote_amount=Decimal("600")),
        # BTC sale with fee paid in third asset (BNB)
        Transaction(timestamp=t0 + timedelta(minutes=2), type="trade", base_asset="BTC", base_amount=Decimal("0.005"),
                    quote_asset="EUR", quote_amount=Decimal("400"),
                    fee_asset="BNB", fee_amount=Decimal("0.01"),
                    memo="BNB fee"),
    ]

    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}

    out = _normalize_transactions_to_eur(
        txs,
        db=db,
        strict_fx=True,
        warnings=warnings,
        fx_meta=fx_meta,
        fee_val_meta=fee_meta,
    )

    # Expect 1 synthetic fee disposal appended
    assert len(out) == 4

    btc_trade = next(t for t in out if t.base_asset.upper() == "BTC" and (t.memo or "").find("BNB fee") >= 0)
    assert btc_trade.fee_asset == "EUR"
    assert btc_trade.fee_amount == Decimal("2.50000000")

    synth = next(t for t in out if (t.memo or "").find("synthetic fee disposal") >= 0)
    assert synth.base_asset == "BNB"
    assert synth.base_amount == Decimal("0.01")
    assert synth.quote_asset == "EUR"
    assert synth.quote_amount == Decimal("2.50000000")

    events, summary, fifo_warnings = compute_fifo(out)

    # BTC disposal + BNB synthetic disposal
    assert {e.asset for e in events} == {"BTC", "BNB"}

    btc_ev = next(e for e in events if e.asset == "BTC")
    bnb_ev = next(e for e in events if e.asset == "BNB")

    assert btc_ev.gain.quantize(Decimal("0.00000001")) == Decimal("97.50000000")
    assert bnb_ev.gain.quantize(Decimal("0.00000001")) == Decimal("0.50000000")

    # No “fee valuation incomplete” warnings expected when price exists
    assert not any("Fee valuation incomplete" in w for w in warnings)


def test_strict_fee_valuation_raises_when_price_missing(monkeypatch):
    import cryptotaxcalc.calc_runner as cr

    clear_fx_cache()
    db = _make_session()

    # Force strict fee valuation for this test only
    monkeypatch.setattr(cr, "STRICT_FEE_VALUATION", True)

    t0 = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

    txs = [
        Transaction(timestamp=t0, type="buy", base_asset="BTC", base_amount=Decimal("0.01"),
                    quote_asset="EUR", quote_amount=Decimal("600")),
        Transaction(timestamp=t0 + timedelta(minutes=1), type="trade", base_asset="BTC", base_amount=Decimal("0.005"),
                    quote_asset="EUR", quote_amount=Decimal("400"),
                    fee_asset="BNB", fee_amount=Decimal("0.01")),
    ]

    warnings: list[str] = []
    fx_meta = {"fallback_days": set(), "fallback_pairs": set()}
    fee_meta = {"third_fee_detected": 0, "third_fee_valued": 0, "missing_price_days": set(), "missing_price_pairs": set()}

    with pytest.raises(ValueError):
        _normalize_transactions_to_eur(
            txs,
            db=db,
            strict_fx=True,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_meta,
        )

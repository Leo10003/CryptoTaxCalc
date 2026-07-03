import os
import random
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import text

# Make sure the tests use an isolated DB file
os.environ.setdefault("SQLALCHEMY_DATABASE_URL", "sqlite:///./test_calc_determinism.sqlite")

from cryptotaxcalc.db import engine, SessionLocal, init_db  # type: ignore
from cryptotaxcalc.models import Base, TransactionRow, RealizedEvent, CalcRun, RunDigest, RunInput  # type: ignore
import pytest
from fastapi import HTTPException

from cryptotaxcalc.app import calculate_v2, CalculateV2Request, on_startup, report_summary  # type: ignore


def _reset_db():
    """
    Hard reset of tables we care about for determinism tests.

    IMPORTANT:
    Mirror the real app startup so that calc_runs has tax_year, summary_json,
    digest columns, etc.:

      - on_startup() → runs init_db(engine), migrations and column adders
      - then we just clear rows from the relevant tables.
    """
    # 1) Run full startup/migration sequence on the test DB
    on_startup()

    # 2) Clear rows from the tables we care about
    with SessionLocal() as session:
        # Child tables first (FK → calc_runs)
        session.query(RunInput).delete()
        session.query(RealizedEvent).delete()
        session.query(RunDigest).delete()

        # Parents after children
        session.query(CalcRun).delete()
        session.query(TransactionRow).delete()
        session.commit()


def _seed_simple_hr_dataset():
    """
    Seed a tiny, deterministic dataset:
    - 1 BUY BTC/EUR
    - 1 SELL BTC/EUR (same size)
    This should always produce the same single realized event for BTC.
    """
    with SessionLocal() as session:
        # BUY 1 BTC @ 20 000 EUR
        session.add(TransactionRow(
            timestamp=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            type="buy",
            base_asset="BTC",
            base_amount=Decimal("1"),
            quote_asset="EUR",
            quote_amount=Decimal("20000"),
            fee_asset=None,
            fee_amount=None,
            exchange="TEST",
            memo="BUY BTC",
        ))
        # SELL 1 BTC @ 25 000 EUR
        session.add(TransactionRow(
            timestamp=datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc),
            type="sell",
            base_asset="BTC",
            base_amount=Decimal("1"),
            quote_asset="EUR",
            quote_amount=Decimal("25000"),
            fee_asset=None,
            fee_amount=None,
            exchange="TEST",
            memo="SELL BTC",
        ))
        session.commit()
        

def _seed_same_timestamp_two_lots(order: str = "cheap_first"):
    """
    Seed 2 BUY lots with the same timestamp but different EUR cost bases, then a SELL.

    If FIFO ordering is not deterministic for equal timestamps, the cost_basis for the sell
    can flip depending on insertion order (catastrophic for auditability).

    Expected deterministic rule (post-fix): the cheaper lot should be consumed first.
    """
    ts_buy = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    ts_sell = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)

    cheap = TransactionRow(
        timestamp=ts_buy,
        type="buy",
        base_asset="BTC",
        base_amount=Decimal("1"),
        quote_asset="EUR",
        quote_amount=Decimal("20000"),
        fee_asset=None,
        fee_amount=None,
        exchange="TEST",
        memo="BUY BTC CHEAP",
    )
    expensive = TransactionRow(
        timestamp=ts_buy,
        type="buy",
        base_asset="BTC",
        base_amount=Decimal("1"),
        quote_asset="EUR",
        quote_amount=Decimal("30000"),
        fee_asset=None,
        fee_amount=None,
        exchange="TEST",
        memo="BUY BTC EXPENSIVE",
    )
    sell = TransactionRow(
        timestamp=ts_sell,
        type="sell",
        base_asset="BTC",
        base_amount=Decimal("1"),
        quote_asset="EUR",
        quote_amount=Decimal("40000"),
        fee_asset=None,
        fee_amount=None,
        exchange="TEST",
        memo="SELL BTC",
    )

    rows = [cheap, expensive]
    if order == "expensive_first":
        rows = [expensive, cheap]
    elif order == "random":
        random.Random(1337).shuffle(rows)

    with SessionLocal() as session:
        session.add_all(rows)
        session.add(sell)
        session.commit()


def _clear_fx_tables():
    with SessionLocal() as session:
        session.execute(text("DELETE FROM fx_rates"))
        session.execute(text("DELETE FROM fx_batches"))
        session.commit()


def _seed_fx_rate_usd_eur(day_iso: str, eur_per_usd: Decimal) -> None:
    """
    Seed USD->EUR FX rate (stored as EUR per 1 USD) for a specific day.
    """
    with SessionLocal() as session:
        session.execute(
            text("DELETE FROM fx_rates WHERE date = :d AND base='USD' AND quote='EUR'"),
            {"d": day_iso},
        )
        session.execute(
            text(
                "INSERT INTO fx_rates (date, base, quote, rate, batch_id) "
                "VALUES (:d, 'USD', 'EUR', :r, NULL)"
            ),
            {"d": day_iso, "r": str(eur_per_usd)},
        )
        session.commit()


def _seed_small_gain_dataset(quote: str = "EUR"):
    """
    Small deterministic gain dataset:
    BUY  1 BTC @ 20 000 (quote)
    SELL 1 BTC @ 21 000 (quote)
    Gain = 1 000 (quote units).
    """
    q = (quote or "EUR").upper()
    with SessionLocal() as session:
        session.add(TransactionRow(
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            type="buy",
            base_asset="BTC",
            base_amount=Decimal("1"),
            quote_asset=q,
            quote_amount=Decimal("20000"),
            fee_asset=None,
            fee_amount=None,
            exchange="TEST",
            memo=f"BUY BTC ({q})",
        ))
        session.add(TransactionRow(
            timestamp=datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc),
            type="sell",
            base_asset="BTC",
            base_amount=Decimal("1"),
            quote_asset=q,
            quote_amount=Decimal("21000"),
            fee_asset=None,
            fee_amount=None,
            exchange="TEST",
            memo=f"SELL BTC ({q})",
        ))
        session.commit()


def test_calculate_v2_is_repeatable_for_same_inputs():
    """
    Calling /calculate/v2 twice on the same dataset must yield
    the same summary (deterministic engine behaviour).
    """
    _reset_db()
    _seed_simple_hr_dataset()

    req = CalculateV2Request(jurisdiction="HR", tax_year=2025)

    # First run
    with SessionLocal() as db:
        r1 = calculate_v2(
            req=req,
            request=None,
            db=db,
            debug=False,
            jurisdiction="HR",
        )
        summary1 = r1.summary

    # Second run (fresh session, same DB contents)
    with SessionLocal() as db:
        r2 = calculate_v2(
            req=req,
            request=None,
            db=db,
            debug=False,
            jurisdiction="HR",
        )
        summary2 = r2.summary

    # Ignore per-run identity fields (run_id differs by design).
    s1 = dict(summary1)
    s2 = dict(summary2)
    # Volatile fields: these change run-to-run but do not affect tax correctness.
    for k in ("run_id", "timings_ms"):
        s1.pop(k, None)
        s2.pop(k, None)

    assert s1 == s2, "calculate_v2 must be deterministic for the same inputs (excluding run_id)"


def test_calculate_v2_digests_repeatable_for_same_inputs():
    """
    For the same inputs, input_hash and output_hash should be stable across runs.
    (manifest_hash is run-specific by design because it includes run timestamps/ids).
    """
    _reset_db()
    _seed_simple_hr_dataset()

    req = CalculateV2Request(jurisdiction="HR", tax_year=2025)

    with SessionLocal() as db:
        r1 = calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")
        d1 = dict(r1.digests or {})

    with SessionLocal() as db:
        r2 = calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")
        d2 = dict(r2.digests or {})

    assert d1.get("input_hash") == d2.get("input_hash")
    assert d1.get("output_hash") == d2.get("output_hash")


def test_calculate_v2_creates_run_and_realized_events():
    """
    After a calculation, we must have:
    - at least one CalcRun row
    - at least one RealizedEvent row linked to that run
    - a corresponding RunDigest row.
    """
    _reset_db()
    _seed_simple_hr_dataset()

    req = CalculateV2Request(jurisdiction="HR", tax_year=2025)

    with SessionLocal() as db:
        r = calculate_v2(
            req=req,
            request=None,
            db=db,
            debug=False,
            jurisdiction="HR",
        )
        run_id = r.run_id

    with SessionLocal() as session:
        run = session.query(CalcRun).filter(CalcRun.id == run_id).first()
        assert run is not None, "CalcRun row must be created by calculate_v2"

        events = session.query(RealizedEvent).filter(RealizedEvent.run_id == run.id).all()
        assert events, "RealizedEvent rows must be persisted for the run"

        digest = session.query(RunDigest).filter(RunDigest.run_id == run.id).first()
        assert digest is not None, "RunDigest row must exist for the run"


def test_calculate_v2_works_for_it_jurisdiction():
    """
    Smoke test: IT jurisdiction path runs successfully and returns a summary.
    We don't lock exact numbers here, only that the structure is present.
    """
    _reset_db()
    _seed_simple_hr_dataset()  # same trades; rules may differ, but engine must not crash

    req = CalculateV2Request(jurisdiction="IT", tax_year=2025)

    with SessionLocal() as db:
        r = calculate_v2(
            req=req,
            request=None,
            db=db,
            debug=False,
            jurisdiction="IT",
        )
        summary = r.summary

    assert isinstance(summary, dict)
    assert "totals" in summary or "eur_summary" in summary, "Summary must contain core totals keys"

def test_it_threshold_year_aware_small_gains():
    """
    Italy:
      - Tax year 2024: €2,000 threshold applies → gain 1,000 => taxable 0
      - Tax year 2025: threshold removed → gain 1,000 => taxable 1,000
    """
    _reset_db()
    _seed_small_gain_dataset(quote="EUR")

    # 2024: threshold applies
    req_2024 = CalculateV2Request(jurisdiction="IT", tax_year=2024)
    with SessionLocal() as db:
        r_2024 = calculate_v2(req=req_2024, request=None, db=db, debug=False, jurisdiction="IT")
        s_2024 = dict(r_2024.summary or {})
        totals_2024 = dict(s_2024.get("totals") or {})
        taxable_2024 = Decimal(str(totals_2024.get("taxable_gain_eur") or "0"))
    assert taxable_2024 == Decimal("0")

    # 2025: no threshold
    req_2025 = CalculateV2Request(jurisdiction="IT", tax_year=2025)
    with SessionLocal() as db:
        r_2025 = calculate_v2(req=req_2025, request=None, db=db, debug=False, jurisdiction="IT")
        s_2025 = dict(r_2025.summary or {})
        totals_2025 = dict(s_2025.get("totals") or {})
        taxable_2025 = Decimal(str(totals_2025.get("taxable_gain_eur") or "0"))
    assert taxable_2025 == Decimal("1000")


def test_calculate_v2_fx_fallback_is_recorded_when_fx_missing_and_strict_fx_false():
    _reset_db()
    _clear_fx_tables()
    _seed_small_gain_dataset(quote="USD")

    req = CalculateV2Request(jurisdiction="HR", tax_year=2024, strict_fx=False)

    with SessionLocal() as db:
        r = calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")
        s = dict(r.summary or {})

    assert s.get("fx_fallback_used") is True
    assert int(s.get("fx_fallback_days_count") or 0) >= 1


def test_calculate_v2_strict_fx_rejects_missing_fx_with_400():
    _reset_db()
    _clear_fx_tables()
    _seed_small_gain_dataset(quote="USD")

    req = CalculateV2Request(jurisdiction="HR", tax_year=2024, strict_fx=True)

    with SessionLocal() as db:
        with pytest.raises(HTTPException) as exc:
            calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")

    assert exc.value.status_code == 400
    assert "Strict FX" in str(exc.value.detail) or "missing" in str(exc.value.detail).lower()


def test_report_summary_usd_to_eur_conversion_direction():
    """
    Guardrail: /report/summary must convert USD->EUR by multiplying by EUR-per-USD rate
    (never divide).
    """
    _reset_db()
    _clear_fx_tables()
    _seed_small_gain_dataset(quote="USD")

    # EUR per 1 USD = 0.5
    _seed_fx_rate_usd_eur("2024-06-01", Decimal("0.5"))

    out = report_summary(year=2024)
    totals = (out.get("eur_summary") or {}).get("totals_eur") or {}

    proceeds = Decimal(str(totals.get("proceeds") or "0"))
    cost = Decimal(str(totals.get("cost_basis") or "0"))
    gain = Decimal(str(totals.get("gain") or "0"))

    q = Decimal("0.00000001")
    assert proceeds.quantize(q) == Decimal("10500").quantize(q)  # 21000 * 0.5
    assert cost.quantize(q) == Decimal("10000").quantize(q)      # 20000 * 0.5
    assert gain.quantize(q) == Decimal("500").quantize(q)        # 1000 * 0.5

def test_fifo_same_timestamp_order_is_deterministic_and_not_insertion_dependent():
    """
    This test would fail if FIFO ordering for equal timestamps depends on insertion/DB order.

    We seed the same economic scenario twice, differing only in the insertion order
    of two BUY lots that share the exact same timestamp.
    The realized event cost_basis for the SELL must be identical across both runs.

    Expected: cheaper lot (20,000 EUR) is consumed first → cost_basis=20,000 and gain=20,000.
    """
    # Run A: insert cheap then expensive
    _reset_db()
    _seed_same_timestamp_two_lots(order="cheap_first")
    req = CalculateV2Request(jurisdiction="HR", tax_year=2025)
    with SessionLocal() as db:
        r_a = calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")
        run_id_a = r_a.run_id

    with SessionLocal() as session:
        ev_a = session.query(RealizedEvent).filter(RealizedEvent.run_id == run_id_a).first()
        assert ev_a is not None
        cb_a = Decimal(str(ev_a.cost_basis))
        gain_a = Decimal(str(ev_a.gain))

    # Run B: reset DB; insert expensive then cheap
    _reset_db()
    _seed_same_timestamp_two_lots(order="expensive_first")
    with SessionLocal() as db:
        r_b = calculate_v2(req=req, request=None, db=db, debug=False, jurisdiction="HR")
        run_id_b = r_b.run_id

    with SessionLocal() as session:
        ev_b = session.query(RealizedEvent).filter(RealizedEvent.run_id == run_id_b).first()
        assert ev_b is not None
        cb_b = Decimal(str(ev_b.cost_basis))
        gain_b = Decimal(str(ev_b.gain))

    assert cb_a == Decimal("20000")
    assert gain_a == Decimal("20000")

    assert cb_b == Decimal("20000")
    assert gain_b == Decimal("20000")

    assert cb_a == cb_b
    assert gain_a == gain_b
    

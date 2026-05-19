import os
from datetime import datetime, timezone
from decimal import Decimal

# Make sure the tests use an isolated DB file
os.environ.setdefault("SQLALCHEMY_DATABASE_URL", "sqlite:///./test_calc_determinism.sqlite")

from cryptotaxcalc.db import engine, SessionLocal, init_db  # type: ignore
from cryptotaxcalc.models import Base, TransactionRow, RealizedEvent, CalcRun, RunDigest, RunInput  # type: ignore
from cryptotaxcalc.app import calculate_v2, CalculateV2Request, on_startup  # type: ignore


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

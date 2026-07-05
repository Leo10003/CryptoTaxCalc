# tests/smoke_test.py
# Run with:
#   pytest -q -m smoke --maxfail=1 --disable-warnings -rA

from __future__ import annotations
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import sqlite3
import io
import json
import re
import uuid
import zipfile
import csv
from typing import List, Tuple
import pytest
from cryptotaxcalc.db import SessionLocal, init_db, engine
from cryptotaxcalc.models import Base, Transaction, TxType
from cryptotaxcalc.schemas import TransactionRead
from decimal import Decimal
from datetime import datetime, timezone
import pathlib, time
import subprocess
from sqlalchemy import text

from fastapi.testclient import TestClient

# Import your FastAPI app
from cryptotaxcalc.app import app

client = TestClient(app)

@pytest.mark.smoke
def test_health_smoke():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    if isinstance(data, dict):
        assert data.get("status") == "ok"
    else:
        assert data in ("OK", '"OK"')

@pytest.mark.smoke
def test_version_smoke():
    r = client.get("/version")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert "version" in data

@pytest.mark.smoke
def test_core_ui_pages_render_without_server_error():
    """
    Smoke-check user-facing HTML routes.

    This catches template/rendering failures where the API imports successfully
    but browser pages crash with 500 errors.
    """
    paths = [
        "/",
    ]

    for path in paths:
        r = client.get(path)

        if r.status_code in (404, 405):
            pytest.skip(f"UI route {path!r} is not available in this build")

        assert r.status_code < 500, (
            f"UI route {path!r} returned server error {r.status_code}:\n{r.text[:1000]}"
        )

        ct = r.headers.get("content-type", "").lower()
        assert "text/html" in ct or "application/json" in ct or "text/plain" in ct, (
            f"Unexpected content type for {path!r}: {ct}"
        )

@pytest.mark.smoke
def test_support_bundle_endpoint_if_token():
    """
    Runs only if an admin token is configured.
    If not set, we skip (so the suite still passes and doesn't return exit=5).
    """
    import json
    token = os.getenv("BUNDLE_TOKEN")
    if not token:
        pytest.skip("Admin token not configured; set BUNDLE_TOKEN to test /admin/bundle")

    r = client.post("/admin/bundle", headers={"X-Admin-Token": token})

    # 404 is an intentional "not discoverable" response when admin endpoints/scripts are disabled in prod mode.
    if r.status_code == 404:
        pytest.skip("Admin bundle endpoint is disabled by prod hardening (ENABLE_ADMIN_ENDPOINTS/ENABLE_ADMIN_SCRIPTS).")

    assert r.status_code == 200
    # Optionally validate the response shape if your endpoint returns JSON:
    try:
        _ = r.json()
    except Exception:
        # If it returns a stream/bytes, this is fine too; just ensure it’s 200
        pass
@pytest.mark.smoke
def test_db_path_is_openable_and_parent_dir_exists():
    """
    Ensures the configured SQLite path is writable/openable.
    If the parent dir doesn't exist, we create it (mirrors server startup needs).
    Skips on non-SQLite backends.
    """
    from cryptotaxcalc.db import SQLALCHEMY_DATABASE_URL

    url = str(SQLALCHEMY_DATABASE_URL)
    if not url.startswith("sqlite"):
        pytest.skip("Non-SQLite backend; path-openability test not applicable.")

    # Handle sqlite:///<abs path>
    if url.startswith("sqlite:///"):
        db_path = pathlib.Path(url.replace("sqlite:///", ""))
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # Just open/close via sqlite3 to catch OS-level errors early
        try:
            with sqlite3.connect(str(db_path)) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
        except sqlite3.Error as e:
            pytest.fail(f"SQLite cannot open DB file at {db_path}: {e}")

@pytest.mark.smoke
def test_auto_repair_migrations_is_idempotent_and_safe():
    """
    Calls auto_repair_migrations() (the thing that bit you at server startup) to ensure
    it does not raise and is idempotent. Skips on non-SQLite.
    """
    from cryptotaxcalc.db import SQLALCHEMY_DATABASE_URL, auto_repair_migrations

    url = str(SQLALCHEMY_DATABASE_URL)
    if not url.startswith("sqlite"):
        pytest.skip("Non-SQLite backend; migration auto-repair not exercised here.")

    # Should not raise even if run multiple times (e.g., reloader or repeated startups)
    auto_repair_migrations()
    auto_repair_migrations()

@pytest.mark.smoke
def test_init_db_repairs_legacy_calc_runs_schema(tmp_path):
    """
    Regression guard for old SQLite databases.

    Older local databases may have calc_runs without newer /calculate/v2 metadata
    columns. init_db(engine) must repair that shape before calculations run.
    """
    from sqlalchemy import create_engine

    legacy_db = tmp_path / "legacy_calc_runs.sqlite"
    legacy_engine = create_engine(
        f"sqlite:///{legacy_db}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    with legacy_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE calc_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                finished_at TEXT,
                jurisdiction TEXT,
                rule_version TEXT,
                lot_method TEXT,
                fx_set_id INTEGER,
                params_json TEXT,
                run_id TEXT
            );
        """))

    init_db(legacy_engine)

    with legacy_engine.connect() as conn:
        rows = conn.execute(text("PRAGMA table_info(calc_runs);")).fetchall()

    columns = {str(row[1]) for row in rows}

    required_columns = {
        "tax_year",
        "input_hash",
        "output_hash",
        "manifest_hash",
        "summary_json",
    }

    missing = required_columns - columns
    assert not missing, f"init_db did not repair calc_runs columns: {sorted(missing)}"


@pytest.mark.smoke
def test_init_db_rebuilds_legacy_run_digests_schema(tmp_path):
    """
    Regression guard for old SQLite databases.

    Older deployments may have created run_digests with run_id as the primary key
    and no synthetic id column. init_db(engine) must rebuild that table into the
    modern ORM-compatible shape while preserving existing digest rows.
    """
    from sqlalchemy import create_engine

    legacy_db = tmp_path / "legacy_run_digests.sqlite"
    legacy_engine = create_engine(
        f"sqlite:///{legacy_db}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    with legacy_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE run_digests (
                run_id INTEGER PRIMARY KEY,
                input_hash TEXT,
                output_hash TEXT,
                manifest_hash TEXT,
                manifest_json TEXT,
                created_at TEXT
            );
        """))
        conn.execute(
            text("""
                INSERT INTO run_digests (
                    run_id,
                    input_hash,
                    output_hash,
                    manifest_hash,
                    manifest_json,
                    created_at
                )
                VALUES (
                    42,
                    'input-hash-smoke',
                    'output-hash-smoke',
                    'manifest-hash-smoke',
                    '{"ok": true}',
                    '2026-01-01T00:00:00'
                );
            """)
        )

    init_db(legacy_engine)

    with legacy_engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(run_digests);")).fetchall()
        rows = conn.execute(text("""
            SELECT id, run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at
            FROM run_digests
            WHERE run_id = 42
        """)).fetchall()

    columns = {str(col[1]) for col in cols}

    required_columns = {
        "id",
        "run_id",
        "input_hash",
        "output_hash",
        "manifest_hash",
        "manifest_json",
        "created_at",
    }

    missing = required_columns - columns
    assert not missing, f"init_db did not rebuild run_digests columns: {sorted(missing)}"

    assert len(rows) == 1, f"Expected one preserved run_digests row, got {rows!r}"

    row = rows[0]
    assert row[0] is not None, "rebuilt run_digests.id should be populated"
    assert row[1] == 42
    assert row[2] == "input-hash-smoke"
    assert row[3] == "output-hash-smoke"
    assert row[4] == "manifest-hash-smoke"
    assert row[5] == '{"ok": true}'
    assert row[6] == "2026-01-01T00:00:00"


@pytest.mark.smoke
def test_init_db_repairs_legacy_fx_rates_schema(tmp_path):
    """
    Regression guard for old SQLite databases.

    Older local DBs may have an incomplete fx_rates table. init_db(engine) must
    add the required columns before FX lookups/calculations run.
    """
    from sqlalchemy import create_engine

    legacy_db = tmp_path / "legacy_fx_rates.sqlite"
    legacy_engine = create_engine(
        f"sqlite:///{legacy_db}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    with legacy_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE fx_rates (
                date TEXT
            );
        """))
        conn.execute(text("""
            INSERT INTO fx_rates (date)
            VALUES ('2024-01-01');
        """))

    init_db(legacy_engine)

    with legacy_engine.connect() as conn:
        cols = conn.execute(text("PRAGMA table_info(fx_rates);")).fetchall()

    columns = {str(col[1]) for col in cols}

    required_columns = {
        "date",
        "base",
        "quote",
        "rate",
        "batch_id",
    }

    missing = required_columns - columns
    assert not missing, f"init_db did not repair fx_rates columns: {sorted(missing)}"


@pytest.mark.smoke
def test_engine_connectivity_and_select_1():
    """
    Minimal 'can we talk to the DB?' check using the SQLAlchemy engine,
    mirrors what the app will do at runtime.
    """
    from cryptotaxcalc.db import engine

    with engine.connect() as conn:
        one = conn.execute(text("SELECT 1")).scalar_one()
        assert one == 1

def _load_env_file_fallback(env_path: pathlib.Path):
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
ENV_FILE = ROOT_DIR / ".env"

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=str(ENV_FILE), override=False)
except Exception:
    _load_env_file_fallback(ENV_FILE)

# --------------------------------------------------------------------------------------
# Import the FastAPI app (supports running from repo root without pip install)
# --------------------------------------------------------------------------------------
try:
    from cryptotaxcalc.app import app  # type: ignore
except Exception as e:
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SRC = os.path.join(ROOT, "src")
    if SRC not in sys.path:
        sys.path.insert(0, SRC)
    try:
        from cryptotaxcalc.app import app  # type: ignore
    except Exception as e2:
        raise RuntimeError(f"Failed to import app: {e2}") from e

from fastapi.testclient import TestClient  # noqa: E402

# Ensure smoke tests have a usable schema even when TestClient startup/lifespan
# hooks are not entered before the first request.
init_db(engine)
Base.metadata.create_all(bind=engine)

client = TestClient(app)
pytestmark = pytest.mark.smoke


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def _is_uuid(value: str) -> bool:
    if not isinstance(value, str):
        return False
    if not UUID_RE.match(value):
        return False
    try:
        uuid.UUID(value)
        return True
    except Exception:
        return False


def _call_calculate_v2_and_get_payload(
    jurisdiction: str = "HR",
    load_demo: bool = True,
) -> Tuple[int, dict]:
    # Ensure demo data exists for normal smoke runs.
    # Deterministic tests can disable this so their expected values are not polluted.
    if load_demo:
        try:
            token = os.getenv("ADMIN_TOKEN") or os.getenv("BUNDLE_TOKEN") or ""
            headers = {"X-Admin-Token": token} if token else None
            r_demo = client.post("/demo/load", headers=headers)
            if r_demo.status_code not in (200, 204, 404, 401, 403):
                raise AssertionError(f"/demo/load returned {r_demo.status_code}: {r_demo.text}")
        except Exception:
            pass

    res = client.post("/calculate/v2", json={"jurisdiction": jurisdiction})
    assert res.status_code == 200, f"/calculate/v2 failed: {res.text}"
    data = res.json()
    assert "run_id" in data, "Response must include run_id"
    run_id = data["run_id"]
    assert isinstance(run_id, int), f"run_id must be int for v2, got {type(run_id).__name__}: {run_id!r}"
    return run_id, data


def _insert_deterministic_btc_buy_sell_rows(memo_tag: str) -> None:
    """
    Insert a tiny deterministic BUY -> SELL dataset for populated smoke tests.

    Scenario:
      BUY  0.10 BTC for 1000 EUR
      SELL 0.04 BTC for  600 EUR

    Expected economic result under FIFO:
      cost basis for sold 0.04 BTC = 400 EUR
      proceeds = 600 EUR
      gain = 200 EUR

    The test below does not hard-code the exact gain yet because response/event
    shapes can vary by endpoint version. It first proves that a populated run
    produces at least one realized/exportable event.
    """
    db = SessionLocal()
    try:
        buy = Transaction(
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            type=TxType.BUY,
            base_asset="BTC",
            base_amount=Decimal("0.10"),
            quote_asset="EUR",
            quote_amount=Decimal("1000"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
            exchange="SmokeDeterministic",
            memo=f"{memo_tag}: buy",
        )
        sell = Transaction(
            timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            type=TxType.SELL,
            base_asset="BTC",
            base_amount=Decimal("0.04"),
            quote_asset="EUR",
            quote_amount=Decimal("600"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
            exchange="SmokeDeterministic",
            memo=f"{memo_tag}: sell",
        )
        db.add_all([buy, sell])
        db.commit()
    finally:
        db.close()


def _insert_deterministic_multilot_fifo_rows(asset: str, memo_tag: str) -> None:
    """
    Insert a deterministic multi-lot FIFO dataset.

    Scenario:
      BUY  0.10 asset for 1000 EUR
      BUY  0.20 asset for 3000 EUR
      SELL 0.25 asset for 5000 EUR

    Expected FIFO:
      cost basis = 1000 + 2250 = 3250 EUR
      proceeds   = 5000 EUR
      gain       = 1750 EUR
    """
    db = SessionLocal()
    try:
        rows = [
            Transaction(
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                type=TxType.BUY,
                base_asset=asset,
                base_amount=Decimal("0.10"),
                quote_asset="EUR",
                quote_amount=Decimal("1000"),
                fee_asset="EUR",
                fee_amount=Decimal("0"),
                exchange="SmokeDeterministic",
                memo=f"{memo_tag}: buy lot 1",
            ),
            Transaction(
                timestamp=datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc),
                type=TxType.BUY,
                base_asset=asset,
                base_amount=Decimal("0.20"),
                quote_asset="EUR",
                quote_amount=Decimal("3000"),
                fee_asset="EUR",
                fee_amount=Decimal("0"),
                exchange="SmokeDeterministic",
                memo=f"{memo_tag}: buy lot 2",
            ),
            Transaction(
                timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
                type=TxType.SELL,
                base_asset=asset,
                base_amount=Decimal("0.25"),
                quote_asset="EUR",
                quote_amount=Decimal("5000"),
                fee_asset="EUR",
                fee_amount=Decimal("0"),
                exchange="SmokeDeterministic",
                memo=f"{memo_tag}: sell across lots",
            ),
        ]
        db.add_all(rows)
        db.commit()
    finally:
        db.close()


def _insert_deterministic_oversell_row(asset: str, memo_tag: str) -> None:
    """
    Insert a deterministic invalid/edge-case dataset.

    Scenario:
      SELL asset without any prior BUY lot.

    A robust tax engine should not crash. It should either reject the run
    with a client/business error or return a warning/error in the payload.
    """
    db = SessionLocal()
    try:
        sell = Transaction(
            timestamp=datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            type=TxType.SELL,
            base_asset=asset,
            base_amount=Decimal("1.00"),
            quote_asset="EUR",
            quote_amount=Decimal("100"),
            fee_asset="EUR",
            fee_amount=Decimal("0"),
            exchange="SmokeDeterministic",
            memo=f"{memo_tag}: oversell without buy",
        )
        db.add(sell)
        db.commit()
    finally:
        db.close()


def _insert_deterministic_missing_fx_rows(asset: str, memo_tag: str) -> None:
    """
    Insert a deterministic dataset that requires non-EUR FX conversion.

    The far-past date and USD quote currency are intended to exercise strict FX
    behavior without relying on live network access.
    """
    db = SessionLocal()
    try:
        rows = [
            Transaction(
                timestamp=datetime(2011, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
                type=TxType.BUY,
                base_asset=asset,
                base_amount=Decimal("1.00"),
                quote_asset="USD",
                quote_amount=Decimal("100"),
                fee_asset="USD",
                fee_amount=Decimal("0"),
                exchange="SmokeDeterministic",
                memo=f"{memo_tag}: fx buy",
            ),
            Transaction(
                timestamp=datetime(2011, 1, 4, 12, 0, 0, tzinfo=timezone.utc),
                type=TxType.SELL,
                base_asset=asset,
                base_amount=Decimal("1.00"),
                quote_asset="USD",
                quote_amount=Decimal("150"),
                fee_asset="USD",
                fee_amount=Decimal("0"),
                exchange="SmokeDeterministic",
                memo=f"{memo_tag}: fx sell",
            ),
        ]
        db.add_all(rows)
        db.commit()
    finally:
        db.close()


def _decimal_from_csv_row(row: dict, *names: str) -> Decimal | None:
    for name in names:
        if name not in row:
            continue

        raw = row.get(name)
        if raw is None:
            continue

        text_value = str(raw).strip()
        if not text_value:
            continue

        # Accept common CSV formatting variants.
        text_value = text_value.replace("€", "").replace(",", "").strip()

        try:
            return Decimal(text_value)
        except Exception:
            continue

    return None


def _assert_csv_contains_expected_btc_fifo_result(csv_text: str) -> None:
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    assert rows, "events.csv should contain at least one parsed data row"

    btc_rows = [
        row for row in rows
        if str(row.get("asset") or row.get("base_asset") or "").strip().upper() == "BTC"
    ]
    assert btc_rows, f"events.csv should contain at least one BTC row, got rows={rows!r}"

    expected_proceeds = Decimal("600")
    expected_cost = Decimal("400")
    expected_gain = Decimal("200")

    matching_rows = []

    for row in btc_rows:
        proceeds = _decimal_from_csv_row(
            row,
            "proceeds_eur",
            "proceeds",
            "sell_value_eur",
            "value_eur",
        )
        cost = _decimal_from_csv_row(
            row,
            "cost_eur",
            "cost_basis_eur",
            "basis_eur",
            "cost_basis",
        )
        gain = _decimal_from_csv_row(
            row,
            "gain_eur",
            "gain",
            "taxable_gain_eur",
            "realized_gain_eur",
        )

        if gain == expected_gain:
            matching_rows.append(row)

        if proceeds is not None:
            assert proceeds == expected_proceeds, f"Expected proceeds 600 EUR, got {proceeds} in row {row!r}"

        if cost is not None:
            assert cost == expected_cost, f"Expected cost basis 400 EUR, got {cost} in row {row!r}"

        if gain is not None:
            assert gain == expected_gain, f"Expected gain 200 EUR, got {gain} in row {row!r}"

    assert matching_rows, (
        "events.csv should contain a BTC row with gain 200 EUR for the deterministic "
        f"BUY 0.10 BTC / SELL 0.04 BTC scenario. BTC rows were: {btc_rows!r}"
    )


def _assert_csv_contains_expected_asset_gain(
    csv_text: str,
    asset: str,
    expected_gain: Decimal,
    expected_proceeds: Decimal | None = None,
    expected_cost: Decimal | None = None,
) -> None:
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    assert rows, "events.csv should contain at least one parsed data row"

    asset_upper = asset.upper()
    asset_rows = [
        row for row in rows
        if str(row.get("asset") or row.get("base_asset") or "").strip().upper() == asset_upper
    ]

    assert asset_rows, f"events.csv should contain at least one {asset_upper} row, got rows={rows!r}"

    matching_gain_rows = []

    for row in asset_rows:
        proceeds = _decimal_from_csv_row(
            row,
            "proceeds_eur",
            "proceeds",
            "sell_value_eur",
            "value_eur",
        )
        cost = _decimal_from_csv_row(
            row,
            "cost_eur",
            "cost_basis_eur",
            "basis_eur",
            "cost_basis",
        )
        gain = _decimal_from_csv_row(
            row,
            "gain_eur",
            "gain",
            "taxable_gain_eur",
            "realized_gain_eur",
        )

        if gain == expected_gain:
            matching_gain_rows.append(row)

        if expected_proceeds is not None and proceeds is not None:
            assert proceeds == expected_proceeds, (
                f"Expected proceeds {expected_proceeds} EUR for {asset_upper}, "
                f"got {proceeds} in row {row!r}"
            )

        if expected_cost is not None and cost is not None:
            assert cost == expected_cost, (
                f"Expected cost basis {expected_cost} EUR for {asset_upper}, "
                f"got {cost} in row {row!r}"
            )

    assert matching_gain_rows, (
        f"events.csv should contain a {asset_upper} row with gain {expected_gain} EUR. "
        f"{asset_upper} rows were: {asset_rows!r}"
    )


def _payload_mentions_problem(payload: dict, asset: str) -> bool:
    """
    Best-effort recursive scan for warning/error text in a response payload.

    This intentionally accepts multiple shapes because API versions may expose
    errors under warnings, errors, issues, diagnostics, precheck, summary, etc.
    """
    asset_upper = asset.upper()
    problem_words = (
        "insufficient",
        "missing",
        "negative",
        "oversell",
        "over-sell",
        "no lot",
        "no lots",
        "cost basis",
        "basis",
        "unmatched",
        "short",
        "cannot",
        "error",
        "warning",
    )

    def walk(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            out = []
            for k, v in value.items():
                out.extend(walk(k))
                out.extend(walk(v))
            return out
        if isinstance(value, list):
            out = []
            for item in value:
                out.extend(walk(item))
            return out
        return [str(value)]

    text = "\n".join(walk(payload)).lower()
    return asset_upper.lower() in text and any(word in text for word in problem_words)


def _payload_mentions_fx_problem(payload: dict, asset: str) -> bool:
    asset_upper = asset.upper()
    problem_words = (
        "fx",
        "foreign exchange",
        "exchange rate",
        "rate",
        "missing",
        "strict",
        "fallback",
        "unavailable",
        "not found",
        "cannot",
        "error",
        "warning",
    )

    def walk(value) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            out = []
            for k, v in value.items():
                out.extend(walk(k))
                out.extend(walk(v))
            return out
        if isinstance(value, list):
            out = []
            for item in value:
                out.extend(walk(item))
            return out
        return [str(value)]

    text = "\n".join(walk(payload)).lower()
    return asset_upper.lower() in text and any(word in text for word in problem_words)


def _try_download_zip(run_id: str):
    """Try both legacy and compact endpoints, return (content, url_used, status_code, text)."""
    paths = [f"/history/{run_id}/download", f"/history/run/{run_id}/download"]
    last = (None, None, None, None)  # content, url, status, text
    for p in paths:
        r = client.get(p)
        if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith("application/zip"):
            return r.content, p, r.status_code, r.text
        last = (None, p, r.status_code, r.text)
    return last


def _try_csv_upload_endpoint(csv_text: str):
    """
    Try known CSV upload/import endpoints.

    Returns:
      (response, endpoint_used)

    The test skips if this build does not expose a CSV endpoint.
    """
    endpoints = [
        "/upload/csv",
        "/import/csv",
        "/api/upload/csv",
        "/api/import/csv",
        "/api/v1/upload/csv",
        "/api/v1/import/csv",
    ]

    for endpoint in endpoints:
        files = {
            "file": (
                "smoke_transactions.csv",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        }

        r = client.post(endpoint, files=files)

        if r.status_code not in (404, 405):
            return r, endpoint

    pytest.skip("No CSV upload/import endpoint is available in this build")


def _try_csv_import_endpoint(csv_text: str):
    """
    Try known CSV import/save endpoints.

    Returns:
      (response, endpoint_used)

    The test skips if this build does not expose a CSV import endpoint.
    """
    endpoints = [
        "/import/csv",
        "/api/import/csv",
        "/api/v1/import/csv",
    ]

    for endpoint in endpoints:
        files = {
            "file": (
                "smoke_transactions.csv",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        }

        r = client.post(endpoint, files=files)

        if r.status_code not in (404, 405):
            return r, endpoint

    pytest.skip("No CSV import/save endpoint is available in this build")


def _try_csv_import_multiple_endpoint(
    csv_text: str,
    reset: bool = False,
    filename: str = "smoke_transactions.csv",
):
    """
    Try known CSV multi-file import/save endpoints.

    Returns:
      (response, endpoint_used)

    The test skips if this build does not expose a CSV multi-import endpoint.
    """
    endpoints = [
        "/import/multiple",
        "/api/import/multiple",
        "/api/v1/import/multiple",
    ]

    for endpoint in endpoints:
        files = [
            (
                "files",
                (
                    filename,
                    csv_text.encode("utf-8"),
                    "text/csv",
                ),
            )
        ]

        r = client.post(
            endpoint,
            params={"reset": "true" if reset else "false"},
            files=files,
        )

        if r.status_code not in (404, 405):
            return r, endpoint

    pytest.skip("No CSV multi-import endpoint is available in this build")


def _try_csv_import_multiple_two_files_endpoint(
    csv_text_1: str,
    csv_text_2: str,
    reset: bool = False,
    filename_1: str = "smoke_transactions.csv",
    filename_2: str = "smoke_transactions.csv",
):
    """
    Try known CSV multi-file import/save endpoints with two uploaded files.

    Returns:
      (response, endpoint_used)

    The test skips if this build does not expose a CSV multi-import endpoint.
    """
    endpoints = [
        "/import/multiple",
        "/api/import/multiple",
        "/api/v1/import/multiple",
    ]

    for endpoint in endpoints:
        files = [
            (
                "files",
                (
                    filename_1,
                    csv_text_1.encode("utf-8"),
                    "text/csv",
                ),
            ),
            (
                "files",
                (
                    filename_2,
                    csv_text_2.encode("utf-8"),
                    "text/csv",
                ),
            ),
        ]

        r = client.post(
            endpoint,
            params={"reset": "true" if reset else "false"},
            files=files,
        )

        if r.status_code not in (404, 405):
            return r, endpoint

    pytest.skip("No CSV multi-import endpoint is available in this build")


def _count_transactions_by_memo_fragment(fragment: str) -> int:
    db = SessionLocal()
    try:
        return int(
            db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM transactions
                    WHERE memo LIKE :memo_fragment
                    """
                ),
                {"memo_fragment": f"%{fragment}%"},
            ).scalar()
            or 0
        )
    finally:
        db.close()


def _csv_response_reports_success(data: dict) -> bool:
    """
    Accept multiple response shapes from preview/import endpoints.

    Known possible fields include:
      total_valid
      inserted
      imported
      rows
      count
      preview_first_5
    """
    numeric_success_fields = (
        "total_valid",
        "inserted",
        "imported",
        "rows_imported",
        "rows_inserted",
        "count",
        "valid_rows",
    )

    for field in numeric_success_fields:
        value = data.get(field)
        if isinstance(value, int) and value >= 1:
            return True

    preview = data.get("preview_first_5")
    if isinstance(preview, list) and len(preview) >= 1:
        return True

    items = data.get("items")
    if isinstance(items, list) and len(items) >= 1:
        return True

    return False


def _csv_response_reports_errors(data: dict) -> bool:
    """
    Accept multiple response shapes from CSV preview/import validation.

    We want malformed CSVs to be visibly rejected or reported with errors.
    Supports both top-level preview responses and nested import responses:
      {"errors": [...]}
      {"total_errors": 1}
      {"results": [{"errors": [...], "skipped_errors": 1}]}
    """
    numeric_error_fields = (
        "total_errors",
        "errors_count",
        "error_count",
        "skipped_errors",
        "invalid_rows",
        "rows_invalid",
    )

    def object_reports_errors(obj) -> bool:
        if not isinstance(obj, dict):
            return False

        for field in numeric_error_fields:
            value = obj.get(field)
            if isinstance(value, int) and value >= 1:
                return True

        for field in ("errors", "issues", "detail"):
            value = obj.get(field)

            if isinstance(value, list) and len(value) >= 1:
                return True

            if isinstance(value, str) and value.strip():
                return True

        return False

    if object_reports_errors(data):
        return True

    results = data.get("results")
    if isinstance(results, list):
        return any(object_reports_errors(item) for item in results)

    return False


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------
def test_csv_import_persists_valid_buy_sell_rows():
    memo_tag = f"smoke-import-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on valid CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (404, 405):
        pytest.skip(f"CSV import endpoint {endpoint} is not available in this build")

    assert r.status_code in (200, 201, 202, 204), (
        f"CSV import endpoint {endpoint} rejected valid CSV: {r.status_code} {r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_count - before_count == 2, (
        f"CSV import endpoint {endpoint} should persist exactly 2 rows with memo tag {memo_tag!r}. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )


def test_csv_import_rejects_malformed_file_without_partial_persistence():
    memo_tag = f"smoke-import-bad-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid-looking buy
not-a-date,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} invalid sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on malformed CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (404, 405):
        pytest.skip(f"CSV import endpoint {endpoint} is not available in this build")

    after_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_count == before_count, (
        f"CSV import endpoint {endpoint} should not partially persist rows from malformed files. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed CSV import status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV import endpoint {endpoint} should return JSON validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV import endpoint {endpoint} accepted malformed CSV but did not report errors. "
        f"Response was: {data!r}"
    )


def test_csv_import_duplicate_file_does_not_create_duplicate_transactions():
    memo_tag = f"smoke-import-dupe-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r1, endpoint = _try_csv_import_endpoint(csv_text)

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on first duplicate test import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )
    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV import endpoint {endpoint} rejected first valid import: {r1.status_code} {r1.text[:1000]}"
    )

    after_first_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_first_count - before_count == 2, (
        f"First CSV import should persist exactly 2 rows. "
        f"before={before_count}, after_first={after_first_count}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_endpoint(csv_text)
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV import endpoint {endpoint2} must not crash on duplicate import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )
    assert r2.status_code in (200, 201, 202, 204), (
        f"CSV import endpoint {endpoint2} rejected duplicate import unexpectedly: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    after_second_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_second_count == after_first_count, (
        f"Duplicate CSV import should not create additional transaction rows. "
        f"after_first={after_first_count}, after_second={after_second_count}, response={r2.text[:1000]}"
    )

    if r2.status_code != 204:
        ct = r2.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r2.json()
            assert isinstance(data, dict), f"CSV import endpoint {endpoint2} should return a JSON object"

            results = data.get("results")
            if isinstance(results, list) and results:
                first_result = results[0]
                inserted = first_result.get("inserted")
                skipped_duplicates = first_result.get("skipped_duplicates")

                if isinstance(inserted, int):
                    assert inserted == 0, (
                        f"Duplicate import should report inserted=0. "
                        f"Response was: {data!r}"
                    )

                if isinstance(skipped_duplicates, int):
                    assert skipped_duplicates >= 2, (
                        f"Duplicate import should report at least 2 skipped duplicates. "
                        f"Response was: {data!r}"
                    )


def test_csv_import_multiple_reset_replaces_existing_transactions():
    old_memo_tag = f"smoke-reset-old-{uuid.uuid4().hex}"
    new_memo_tag = f"smoke-reset-new-{uuid.uuid4().hex}"

    old_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{old_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{old_memo_tag} sell
"""

    new_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{new_memo_tag} buy
2024-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{new_memo_tag} sell
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        old_csv_text,
        reset=False,
        filename="smoke_reset_old.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on initial import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected initial valid import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    old_count_after_first = _count_transactions_by_memo_fragment(old_memo_tag)
    assert old_count_after_first == 2, (
        f"Initial multi-import should persist exactly 2 old rows. "
        f"count={old_count_after_first}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_endpoint(
        new_csv_text,
        reset=True,
        filename="smoke_reset_new.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    assert r2.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint2} rejected valid reset import: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    old_count_after_reset = _count_transactions_by_memo_fragment(old_memo_tag)
    new_count_after_reset = _count_transactions_by_memo_fragment(new_memo_tag)

    assert old_count_after_reset == 0, (
        f"reset=true should remove old imported transaction rows. "
        f"old_count_after_reset={old_count_after_reset}, response={r2.text[:1000]}"
    )

    assert new_count_after_reset == 2, (
        f"reset=true should persist exactly 2 replacement rows. "
        f"new_count_after_reset={new_count_after_reset}, response={r2.text[:1000]}"
    )

    if r2.status_code != 204:
        ct = r2.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r2.json()
            assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return JSON"

            results = data.get("results")
            if isinstance(results, list) and results:
                inserted = results[0].get("inserted")
                if isinstance(inserted, int):
                    assert inserted == 2, (
                        f"reset import should report inserted=2 for replacement file. "
                        f"Response was: {data!r}"
                    )


def test_csv_import_multiple_duplicate_filenames_do_not_duplicate_transactions():
    memo_tag = f"smoke-multifile-dupe-name-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=csv_text,
        csv_text_2=csv_text,
        reset=False,
        filename_1="same_name.csv",
        filename_2="same_name.csv",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on duplicate filenames. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        # Explicit duplicate-filename / duplicate-content rejection is acceptable.
        return

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV multi-import status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_count - before_count == 2, (
        f"Two identical files with duplicate filenames should not create duplicate transaction rows. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    if r.status_code != 204:
        ct = r.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r.json()
            assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return JSON"

            results = data.get("results")
            if isinstance(results, list) and len(results) >= 2:
                total_inserted = sum(
                    item.get("inserted", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("inserted", 0), int)
                )
                total_skipped_duplicates = sum(
                    item.get("skipped_duplicates", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("skipped_duplicates", 0), int)
                )

                assert total_inserted == 2, (
                    f"Duplicate same-content files should insert only 2 total rows. "
                    f"Response was: {data!r}"
                )

                assert total_skipped_duplicates >= 2, (
                    f"Second duplicate file should report at least 2 skipped duplicates. "
                    f"Response was: {data!r}"
                )


def test_csv_import_multiple_rejects_malformed_batch_without_partial_persistence():
    valid_memo_tag = f"smoke-batch-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-batch-bad-{uuid.uuid4().hex}"

    valid_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{valid_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{valid_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    before_valid_count = _count_transactions_by_memo_fragment(valid_memo_tag)
    before_bad_count = _count_transactions_by_memo_fragment(bad_memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=valid_csv_text,
        csv_text_2=malformed_csv_text,
        reset=False,
        filename_1="smoke_batch_valid.csv",
        filename_2="smoke_batch_bad.csv",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on mixed valid/malformed batch. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (404, 405):
        pytest.skip(f"CSV multi-import endpoint {endpoint} is not available in this build")

    after_valid_count = _count_transactions_by_memo_fragment(valid_memo_tag)
    after_bad_count = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert after_valid_count == before_valid_count, (
        f"CSV multi-import endpoint {endpoint} should not persist the valid file "
        f"when another file in the same batch is malformed. "
        f"before={before_valid_count}, after={after_valid_count}, response={r.text[:1000]}"
    )

    assert after_bad_count == before_bad_count, (
        f"CSV multi-import endpoint {endpoint} should not persist malformed file rows. "
        f"before={before_bad_count}, after={after_bad_count}, response={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected mixed-batch CSV import status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint} accepted a malformed batch but did not report errors. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_reset_with_malformed_batch_preserves_existing_transactions():
    existing_memo_tag = f"smoke-reset-preserve-existing-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-preserve-bad-{uuid.uuid4().hex}"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename="smoke_reset_preserve_existing.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_bad_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    bad_count_before_bad_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_bad_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_bad_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_endpoint(
        malformed_csv_text,
        reset=True,
        filename="smoke_reset_preserve_bad.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on malformed reset import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_bad_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    bad_count_after_bad_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_bad_reset == existing_count_before_bad_reset, (
        f"reset=true with malformed replacement batch must preserve existing transactions. "
        f"before={existing_count_before_bad_reset}, after={existing_count_after_bad_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_bad_reset == bad_count_before_bad_reset, (
        f"reset=true with malformed replacement batch must not persist bad replacement rows. "
        f"before={bad_count_before_bad_reset}, after={bad_count_after_bad_reset}, "
        f"response={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        return

    assert r2.status_code in (200, 201, 202), (
        f"Unexpected malformed reset CSV import status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON validation feedback. "
        f"status={r2.status_code}, content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint2} accepted malformed reset batch but did not report errors. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_reset_with_duplicate_files_replaces_once():
    old_memo_tag = f"smoke-reset-dupe-old-{uuid.uuid4().hex}"
    new_memo_tag = f"smoke-reset-dupe-new-{uuid.uuid4().hex}"

    old_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{old_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{old_memo_tag} sell
"""

    new_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{new_memo_tag} buy
2024-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{new_memo_tag} sell
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        old_csv_text,
        reset=False,
        filename="smoke_reset_dupe_old.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    old_count_after_setup = _count_transactions_by_memo_fragment(old_memo_tag)
    assert old_count_after_setup == 2, (
        f"Setup import should persist exactly 2 old rows. "
        f"count={old_count_after_setup}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=new_csv_text,
        csv_text_2=new_csv_text,
        reset=True,
        filename_1="smoke_reset_dupe_new.csv",
        filename_2="smoke_reset_dupe_new.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset duplicate-file import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        # Explicit duplicate-file/content rejection is acceptable if old data is preserved or replaced
        # according to the endpoint's policy. The stricter behavior below applies to successful imports.
        return

    assert r2.status_code in (200, 201, 202, 204), (
        f"Unexpected reset duplicate-file CSV import status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    old_count_after_reset = _count_transactions_by_memo_fragment(old_memo_tag)
    new_count_after_reset = _count_transactions_by_memo_fragment(new_memo_tag)

    assert old_count_after_reset == 0, (
        f"reset=true should remove old transaction rows. "
        f"old_count_after_reset={old_count_after_reset}, response={r2.text[:1000]}"
    )

    assert new_count_after_reset == 2, (
        f"reset=true with duplicate replacement files should insert replacement rows once. "
        f"new_count_after_reset={new_count_after_reset}, response={r2.text[:1000]}"
    )

    if r2.status_code != 204:
        ct = r2.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r2.json()
            assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return JSON"

            results = data.get("results")
            if isinstance(results, list) and len(results) >= 2:
                total_inserted = sum(
                    item.get("inserted", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("inserted", 0), int)
                )
                total_skipped_duplicates = sum(
                    item.get("skipped_duplicates", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("skipped_duplicates", 0), int)
                )

                assert total_inserted == 2, (
                    f"reset=true duplicate replacement files should insert only 2 total rows. "
                    f"Response was: {data!r}"
                )

                assert total_skipped_duplicates >= 2, (
                    f"Second replacement file should report at least 2 skipped duplicates. "
                    f"Response was: {data!r}"
                )


def test_csv_import_multiple_accepts_two_different_valid_files():
    btc_memo_tag = f"smoke-multifile-btc-{uuid.uuid4().hex}"
    eth_memo_tag = f"smoke-multifile-eth-{uuid.uuid4().hex}"

    btc_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{btc_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{btc_memo_tag} sell
"""

    eth_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{eth_memo_tag} buy
2024-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{eth_memo_tag} sell
"""

    btc_before_count = _count_transactions_by_memo_fragment(btc_memo_tag)
    eth_before_count = _count_transactions_by_memo_fragment(eth_memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=btc_csv_text,
        csv_text_2=eth_csv_text,
        reset=False,
        filename_1="smoke_multifile_btc.csv",
        filename_2="smoke_multifile_eth.csv",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on two valid files. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected two valid files: "
        f"{r.status_code} {r.text[:1000]}"
    )

    btc_after_count = _count_transactions_by_memo_fragment(btc_memo_tag)
    eth_after_count = _count_transactions_by_memo_fragment(eth_memo_tag)

    assert btc_after_count - btc_before_count == 2, (
        f"First valid file should persist exactly 2 BTC rows. "
        f"before={btc_before_count}, after={btc_after_count}, response={r.text[:1000]}"
    )

    assert eth_after_count - eth_before_count == 2, (
        f"Second valid file should persist exactly 2 ETH rows. "
        f"before={eth_before_count}, after={eth_after_count}, response={r.text[:1000]}"
    )

    if r.status_code != 204:
        ct = r.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r.json()
            assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return JSON"

            results = data.get("results")
            if isinstance(results, list):
                assert len(results) >= 2, (
                    f"CSV multi-import endpoint {endpoint} should report one result per uploaded file. "
                    f"Response was: {data!r}"
                )

                total_inserted = sum(
                    item.get("inserted", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("inserted", 0), int)
                )

                total_errors = sum(
                    item.get("skipped_errors", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("skipped_errors", 0), int)
                )

                assert total_inserted == 4, (
                    f"Two different valid files should report 4 inserted rows total. "
                    f"Response was: {data!r}"
                )

                assert total_errors == 0, (
                    f"Two different valid files should not report parse errors. "
                    f"Response was: {data!r}"
                )


def test_csv_import_multiple_reset_accepts_two_different_valid_files():
    old_memo_tag = f"smoke-reset-multifile-old-{uuid.uuid4().hex}"
    btc_memo_tag = f"smoke-reset-multifile-btc-{uuid.uuid4().hex}"
    eth_memo_tag = f"smoke-reset-multifile-eth-{uuid.uuid4().hex}"

    old_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{old_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{old_memo_tag} sell
"""

    btc_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{btc_memo_tag} buy
2024-07-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{btc_memo_tag} sell
"""

    eth_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-03-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{eth_memo_tag} buy
2024-08-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{eth_memo_tag} sell
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        old_csv_text,
        reset=False,
        filename="smoke_reset_multifile_old.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    old_count_after_setup = _count_transactions_by_memo_fragment(old_memo_tag)
    assert old_count_after_setup == 2, (
        f"Setup import should persist exactly 2 old rows. "
        f"count={old_count_after_setup}, response={r1.text[:1000]}"
    )

    btc_before_count = _count_transactions_by_memo_fragment(btc_memo_tag)
    eth_before_count = _count_transactions_by_memo_fragment(eth_memo_tag)

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=btc_csv_text,
        csv_text_2=eth_csv_text,
        reset=True,
        filename_1="smoke_reset_multifile_btc.csv",
        filename_2="smoke_reset_multifile_eth.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset two-file import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    assert r2.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint2} rejected valid reset two-file import: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    old_count_after_reset = _count_transactions_by_memo_fragment(old_memo_tag)
    btc_after_count = _count_transactions_by_memo_fragment(btc_memo_tag)
    eth_after_count = _count_transactions_by_memo_fragment(eth_memo_tag)

    assert old_count_after_reset == 0, (
        f"reset=true should remove old transaction rows before replacement import. "
        f"old_count_after_reset={old_count_after_reset}, response={r2.text[:1000]}"
    )

    assert btc_after_count - btc_before_count == 2, (
        f"First replacement file should persist exactly 2 BTC rows. "
        f"before={btc_before_count}, after={btc_after_count}, response={r2.text[:1000]}"
    )

    assert eth_after_count - eth_before_count == 2, (
        f"Second replacement file should persist exactly 2 ETH rows. "
        f"before={eth_before_count}, after={eth_after_count}, response={r2.text[:1000]}"
    )

    if r2.status_code != 204:
        ct = r2.headers.get("content-type", "").lower()
        if "application/json" in ct:
            data = r2.json()
            assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return JSON"

            results = data.get("results")
            if isinstance(results, list):
                assert len(results) >= 2, (
                    f"CSV multi-import reset should report one result per uploaded replacement file. "
                    f"Response was: {data!r}"
                )

                total_inserted = sum(
                    item.get("inserted", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("inserted", 0), int)
                )

                total_errors = sum(
                    item.get("skipped_errors", 0)
                    for item in results
                    if isinstance(item, dict) and isinstance(item.get("skipped_errors", 0), int)
                )

                assert total_inserted == 4, (
                    f"Two different valid reset files should report 4 inserted rows total. "
                    f"Response was: {data!r}"
                )

                assert total_errors == 0, (
                    f"Two different valid reset files should not report parse errors. "
                    f"Response was: {data!r}"
                )


def test_csv_import_multiple_reset_rejects_mixed_valid_and_malformed_batch_without_data_loss():
    existing_memo_tag = f"smoke-reset-mixed-existing-{uuid.uuid4().hex}"
    replacement_memo_tag = f"smoke-reset-mixed-replacement-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-mixed-bad-{uuid.uuid4().hex}"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    replacement_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{replacement_memo_tag} buy
2024-07-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{replacement_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename="smoke_reset_mixed_existing.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_before_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_before_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=replacement_csv_text,
        csv_text_2=malformed_csv_text,
        reset=True,
        filename_1="smoke_reset_mixed_replacement.csv",
        filename_2="smoke_reset_mixed_bad.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on mixed malformed reset batch. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_after_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_after_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"reset=true with mixed malformed batch must preserve existing transactions. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert replacement_count_after_reset == replacement_count_before_reset, (
        f"reset=true with mixed malformed batch must not partially import valid replacement rows. "
        f"before={replacement_count_before_reset}, after={replacement_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_reset == bad_count_before_reset, (
        f"reset=true with mixed malformed batch must not import malformed replacement rows. "
        f"before={bad_count_before_reset}, after={bad_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        return

    assert r2.status_code in (200, 201, 202), (
        f"Unexpected mixed malformed reset CSV import status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON validation feedback. "
        f"status={r2.status_code}, content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint2} accepted mixed malformed reset batch "
        f"but did not report errors. Response was: {data!r}"
    )


def test_csv_import_multiple_reports_results_in_upload_order():
    first_memo_tag = f"smoke-order-first-{uuid.uuid4().hex}"
    second_memo_tag = f"smoke-order-second-{uuid.uuid4().hex}"

    first_filename = "smoke_order_first.csv"
    second_filename = "smoke_order_second.csv"

    first_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{first_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{first_memo_tag} sell
"""

    second_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{second_memo_tag} buy
2024-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{second_memo_tag} sell
"""

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=first_csv_text,
        csv_text_2=second_csv_text,
        reset=False,
        filename_1=first_filename,
        filename_2=second_filename,
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on ordered multi-file import. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV multi-import endpoint {endpoint} should return JSON for ordered result validation. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list), (
        f"CSV multi-import endpoint {endpoint} should return a results list. "
        f"Response was: {data!r}"
    )

    assert len(results) >= 2, (
        f"CSV multi-import endpoint {endpoint} should report one result per uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == first_filename, (
        f"First result should correspond to first uploaded file. "
        f"Expected {first_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == second_filename, (
        f"Second result should correspond to second uploaded file. "
        f"Expected {second_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert _count_transactions_by_memo_fragment(first_memo_tag) == 2, (
        f"First ordered file should persist exactly 2 rows. Response was: {r.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(second_memo_tag) == 2, (
        f"Second ordered file should persist exactly 2 rows. Response was: {r.text[:1000]}"
    )


def test_csv_import_multiple_reports_malformed_results_in_upload_order():
    first_memo_tag = f"smoke-bad-order-first-{uuid.uuid4().hex}"
    second_memo_tag = f"smoke-bad-order-second-{uuid.uuid4().hex}"

    first_filename = "smoke_bad_order_first.csv"
    second_filename = "smoke_bad_order_second.csv"

    first_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{first_memo_tag} invalid timestamp
"""

    second_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,SELL,,0.50,EUR,1400,EUR,0,SmokeCSV,{second_memo_tag} missing asset
"""

    before_first_count = _count_transactions_by_memo_fragment(first_memo_tag)
    before_second_count = _count_transactions_by_memo_fragment(second_memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=first_csv_text,
        csv_text_2=second_csv_text,
        reset=False,
        filename_1=first_filename,
        filename_2=second_filename,
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on ordered malformed multi-file import. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_first_count = _count_transactions_by_memo_fragment(first_memo_tag)
    after_second_count = _count_transactions_by_memo_fragment(second_memo_tag)

    assert after_first_count == before_first_count, (
        f"Malformed first file should not persist rows. "
        f"before={before_first_count}, after={after_first_count}, response={r.text[:1000]}"
    )

    assert after_second_count == before_second_count, (
        f"Malformed second file should not persist rows. "
        f"before={before_second_count}, after={after_second_count}, response={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed ordered CSV multi-import status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON validation feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list), (
        f"CSV multi-import endpoint {endpoint} should return a results list. "
        f"Response was: {data!r}"
    )

    assert len(results) >= 2, (
        f"CSV multi-import endpoint {endpoint} should report one result per malformed uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == first_filename, (
        f"First error result should correspond to first uploaded malformed file. "
        f"Expected {first_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == second_filename, (
        f"Second error result should correspond to second uploaded malformed file. "
        f"Expected {second_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert _csv_response_reports_errors({"results": [results[0]]}), (
        f"First malformed file result should report errors. Response was: {data!r}"
    )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Second malformed file result should report errors. Response was: {data!r}"
    )


def test_csv_import_multiple_reports_mixed_batch_results_in_upload_order():
    valid_memo_tag = f"smoke-mixed-order-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-mixed-order-bad-{uuid.uuid4().hex}"

    valid_filename = "smoke_mixed_order_valid.csv"
    bad_filename = "smoke_mixed_order_bad.csv"

    valid_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{valid_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{valid_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    before_valid_count = _count_transactions_by_memo_fragment(valid_memo_tag)
    before_bad_count = _count_transactions_by_memo_fragment(bad_memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=valid_csv_text,
        csv_text_2=malformed_csv_text,
        reset=False,
        filename_1=valid_filename,
        filename_2=bad_filename,
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on mixed ordered multi-file import. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_valid_count = _count_transactions_by_memo_fragment(valid_memo_tag)
    after_bad_count = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert after_valid_count == before_valid_count, (
        f"Mixed malformed batch should not persist rows from the valid file. "
        f"before={before_valid_count}, after={after_valid_count}, response={r.text[:1000]}"
    )

    assert after_bad_count == before_bad_count, (
        f"Mixed malformed batch should not persist rows from the malformed file. "
        f"before={before_bad_count}, after={after_bad_count}, response={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected mixed ordered CSV multi-import status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON validation feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list), (
        f"CSV multi-import endpoint {endpoint} should return a results list. "
        f"Response was: {data!r}"
    )

    assert len(results) >= 2, (
        f"Mixed malformed batch should report one result per uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == valid_filename, (
        f"First result should correspond to first uploaded valid file. "
        f"Expected {valid_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == bad_filename, (
        f"Second result should correspond to second uploaded malformed file. "
        f"Expected {bad_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    first_inserted = results[0].get("inserted")
    if isinstance(first_inserted, int):
        assert first_inserted == 0, (
            f"Preflight failure should report inserted=0 for valid file in malformed batch. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Malformed file result should report errors. Response was: {data!r}"
    )


def test_csv_import_multiple_reset_reports_mixed_batch_results_in_upload_order():
    existing_memo_tag = f"smoke-reset-order-existing-{uuid.uuid4().hex}"
    replacement_memo_tag = f"smoke-reset-order-replacement-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-order-bad-{uuid.uuid4().hex}"

    existing_filename = "smoke_reset_order_existing.csv"
    replacement_filename = "smoke_reset_order_replacement.csv"
    bad_filename = "smoke_reset_order_bad.csv"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    replacement_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{replacement_memo_tag} buy
2024-07-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{replacement_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename=existing_filename,
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_before_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_before_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=replacement_csv_text,
        csv_text_2=malformed_csv_text,
        reset=True,
        filename_1=replacement_filename,
        filename_2=bad_filename,
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on mixed reset ordered import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_after_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_after_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"Malformed reset batch should preserve existing rows. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert replacement_count_after_reset == replacement_count_before_reset, (
        f"Malformed reset batch should not persist valid replacement rows. "
        f"before={replacement_count_before_reset}, after={replacement_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_reset == bad_count_before_reset, (
        f"Malformed reset batch should not persist malformed replacement rows. "
        f"before={bad_count_before_reset}, after={bad_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        return

    assert r2.status_code in (200, 201, 202), (
        f"Unexpected mixed reset ordered CSV multi-import status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON validation feedback. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list), (
        f"CSV multi-import endpoint {endpoint2} should return a results list. "
        f"Response was: {data!r}"
    )

    assert len(results) >= 2, (
        f"Mixed reset malformed batch should report one result per uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == replacement_filename, (
        f"First result should correspond to first uploaded valid replacement file. "
        f"Expected {replacement_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == bad_filename, (
        f"Second result should correspond to second uploaded malformed replacement file. "
        f"Expected {bad_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    first_inserted = results[0].get("inserted")
    if isinstance(first_inserted, int):
        assert first_inserted == 0, (
            f"Preflight failure should report inserted=0 for valid replacement file. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Malformed replacement file result should report errors. Response was: {data!r}"
    )


def test_csv_import_multiple_mixed_preflight_results_include_source_metadata():
    valid_memo_tag = f"smoke-source-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-source-bad-{uuid.uuid4().hex}"

    valid_filename = "smoke_source_valid.csv"
    bad_filename = "smoke_source_bad.csv"

    valid_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{valid_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{valid_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=valid_csv_text,
        csv_text_2=malformed_csv_text,
        reset=False,
        filename_1=valid_filename,
        filename_2=bad_filename,
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on mixed preflight source metadata test. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected mixed preflight source metadata status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON validation feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"CSV multi-import endpoint {endpoint} should return result entries for both files. "
        f"Response was: {data!r}"
    )

    for index, expected_filename in enumerate((valid_filename, bad_filename)):
        result = results[index]

        assert result.get("filename") == expected_filename, (
            f"Result {index} should correspond to {expected_filename!r}. "
            f"Response was: {data!r}"
        )

        assert result.get("recognized_source_id"), (
            f"Result {index} should include recognized_source_id. "
            f"Response was: {data!r}"
        )

        assert result.get("recognized_source_name"), (
            f"Result {index} should include recognized_source_name. "
            f"Response was: {data!r}"
        )

        assert result.get("recognized_source_status"), (
            f"Result {index} should include recognized_source_status. "
            f"Response was: {data!r}"
        )

        confidence = result.get("recognized_source_confidence")
        assert isinstance(confidence, (int, float)), (
            f"Result {index} should include numeric recognized_source_confidence. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Malformed file result should still report errors. Response was: {data!r}"
    )

    assert _count_transactions_by_memo_fragment(valid_memo_tag) == 0, (
        f"Mixed malformed preflight batch should not persist valid file rows. "
        f"Response was: {r.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(bad_memo_tag) == 0, (
        f"Mixed malformed preflight batch should not persist malformed file rows. "
        f"Response was: {r.text[:1000]}"
    )


def test_csv_import_multiple_reset_preflight_results_include_source_metadata():
    existing_memo_tag = f"smoke-reset-source-existing-{uuid.uuid4().hex}"
    replacement_memo_tag = f"smoke-reset-source-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-source-bad-{uuid.uuid4().hex}"

    existing_filename = "smoke_reset_source_existing.csv"
    replacement_filename = "smoke_reset_source_valid.csv"
    bad_filename = "smoke_reset_source_bad.csv"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    replacement_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{replacement_memo_tag} buy
2024-07-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{replacement_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename=existing_filename,
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_before_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_before_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=replacement_csv_text,
        csv_text_2=malformed_csv_text,
        reset=True,
        filename_1=replacement_filename,
        filename_2=bad_filename,
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset preflight metadata test. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_after_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_after_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"reset=true malformed preflight should preserve existing rows. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert replacement_count_after_reset == replacement_count_before_reset, (
        f"reset=true malformed preflight should not persist valid replacement rows. "
        f"before={replacement_count_before_reset}, after={replacement_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_reset == bad_count_before_reset, (
        f"reset=true malformed preflight should not persist bad replacement rows. "
        f"before={bad_count_before_reset}, after={bad_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        return

    assert r2.status_code in (200, 201, 202), (
        f"Unexpected reset preflight source metadata status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON validation feedback. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"CSV multi-import endpoint {endpoint2} should return result entries for both reset files. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == replacement_filename, (
        f"First result should correspond to the valid replacement file. "
        f"Expected {replacement_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == bad_filename, (
        f"Second result should correspond to the malformed replacement file. "
        f"Expected {bad_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    for index in (0, 1):
        result = results[index]

        assert result.get("recognized_source_id"), (
            f"Result {index} should include recognized_source_id. Response was: {data!r}"
        )

        assert result.get("recognized_source_name"), (
            f"Result {index} should include recognized_source_name. Response was: {data!r}"
        )

        assert result.get("recognized_source_status"), (
            f"Result {index} should include recognized_source_status. Response was: {data!r}"
        )

        confidence = result.get("recognized_source_confidence")
        assert isinstance(confidence, (int, float)), (
            f"Result {index} should include numeric recognized_source_confidence. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Malformed reset file result should still report errors. Response was: {data!r}"
    )


def test_csv_import_multiple_malformed_preflight_meta_has_no_imported_year_range():
    valid_memo_tag = f"smoke-meta-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-meta-bad-{uuid.uuid4().hex}"

    valid_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{valid_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{valid_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=valid_csv_text,
        csv_text_2=malformed_csv_text,
        reset=False,
        filename_1="smoke_meta_valid.csv",
        filename_2="smoke_meta_bad.csv",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on malformed preflight meta test. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(valid_memo_tag) == 0, (
        f"Malformed preflight batch should not persist valid file rows. "
        f"Response was: {r.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(bad_memo_tag) == 0, (
        f"Malformed preflight batch should not persist bad file rows. "
        f"Response was: {r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed preflight meta status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON validation feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"Malformed preflight batch should report validation errors. Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Malformed preflight response should include a meta object. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Malformed preflight response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Malformed preflight response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_reset_malformed_preflight_meta_has_no_imported_year_range():
    existing_memo_tag = f"smoke-reset-meta-existing-{uuid.uuid4().hex}"
    replacement_memo_tag = f"smoke-reset-meta-valid-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-meta-bad-{uuid.uuid4().hex}"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    replacement_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-02-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{replacement_memo_tag} buy
2024-07-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{replacement_memo_tag} sell
"""

    malformed_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{bad_memo_tag} invalid timestamp
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename="smoke_reset_meta_existing.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_before_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_before_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=replacement_csv_text,
        csv_text_2=malformed_csv_text,
        reset=True,
        filename_1="smoke_reset_meta_valid.csv",
        filename_2="smoke_reset_meta_bad.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset malformed preflight meta test. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    replacement_count_after_reset = _count_transactions_by_memo_fragment(replacement_memo_tag)
    bad_count_after_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"reset=true malformed preflight should preserve existing rows. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert replacement_count_after_reset == replacement_count_before_reset, (
        f"reset=true malformed preflight should not persist valid replacement rows. "
        f"before={replacement_count_before_reset}, after={replacement_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_reset == bad_count_before_reset, (
        f"reset=true malformed preflight should not persist bad replacement rows. "
        f"before={bad_count_before_reset}, after={bad_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 422):
        return

    assert r2.status_code in (200, 201, 202), (
        f"Unexpected reset malformed preflight meta status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON validation feedback. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"Malformed reset preflight batch should report validation errors. Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Malformed reset preflight response should include a meta object. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Malformed reset preflight response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Malformed reset preflight response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_success_meta_reports_global_imported_year_range():
    first_memo_tag = f"smoke-success-meta-first-{uuid.uuid4().hex}"
    second_memo_tag = f"smoke-success-meta-second-{uuid.uuid4().hex}"

    first_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2023-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{first_memo_tag} buy
2023-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{first_memo_tag} sell
"""

    second_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{second_memo_tag} buy
2025-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{second_memo_tag} sell
"""

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=first_csv_text,
        csv_text_2=second_csv_text,
        reset=False,
        filename_1="smoke_success_meta_2023.csv",
        filename_2="smoke_success_meta_2025.csv",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on successful metadata test. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV multi-import endpoint {endpoint} should return JSON for successful metadata validation. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _count_transactions_by_memo_fragment(first_memo_tag) == 2, (
        f"First successful metadata file should persist exactly 2 rows. "
        f"Response was: {r.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(second_memo_tag) == 2, (
        f"Second successful metadata file should persist exactly 2 rows. "
        f"Response was: {r.text[:1000]}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Successful multi-import response should include a meta object. "
        f"Response was: {data!r}"
    )

    assert meta.get("min_year") == 2023, (
        f"Successful multi-import should report global min_year=2023. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") == 2025, (
        f"Successful multi-import should report global max_year=2025. "
        f"Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"Successful multi-import should report one result per uploaded file. "
        f"Response was: {data!r}"
    )

    result_years = [
        (
            item.get("filename"),
            item.get("min_year"),
            item.get("max_year"),
        )
        for item in results
        if isinstance(item, dict)
    ]

    assert ("smoke_success_meta_2023.csv", 2023, 2023) in result_years, (
        f"First file should report per-file year range 2023..2023. "
        f"Result years were: {result_years!r}. Response was: {data!r}"
    )

    assert ("smoke_success_meta_2025.csv", 2025, 2025) in result_years, (
        f"Second file should report per-file year range 2025..2025. "
        f"Result years were: {result_years!r}. Response was: {data!r}"
    )


def test_csv_import_multiple_reset_success_meta_reports_global_replacement_year_range():
    old_memo_tag = f"smoke-reset-success-meta-old-{uuid.uuid4().hex}"
    first_memo_tag = f"smoke-reset-success-meta-first-{uuid.uuid4().hex}"
    second_memo_tag = f"smoke-reset-success-meta-second-{uuid.uuid4().hex}"

    old_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{old_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{old_memo_tag} sell
"""

    first_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2023-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{first_memo_tag} buy
2023-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{first_memo_tag} sell
"""

    second_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-02-01T12:00:00Z,BUY,ETH,1.50,EUR,3000,EUR,0,SmokeCSV,{second_memo_tag} buy
2025-07-01T12:00:00Z,SELL,ETH,0.50,EUR,1400,EUR,0,SmokeCSV,{second_memo_tag} sell
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        old_csv_text,
        reset=False,
        filename="smoke_reset_success_meta_old.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    old_count_after_setup = _count_transactions_by_memo_fragment(old_memo_tag)
    assert old_count_after_setup == 2, (
        f"Setup import should persist exactly 2 old rows. "
        f"count={old_count_after_setup}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=first_csv_text,
        csv_text_2=second_csv_text,
        reset=True,
        filename_1="smoke_reset_success_meta_2023.csv",
        filename_2="smoke_reset_success_meta_2025.csv",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset metadata import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    assert r2.status_code in (200, 201, 202), (
        f"CSV multi-import endpoint {endpoint2} should return JSON for reset metadata validation. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    old_count_after_reset = _count_transactions_by_memo_fragment(old_memo_tag)
    first_count_after_reset = _count_transactions_by_memo_fragment(first_memo_tag)
    second_count_after_reset = _count_transactions_by_memo_fragment(second_memo_tag)

    assert old_count_after_reset == 0, (
        f"reset=true should remove old rows before successful replacement import. "
        f"old_count_after_reset={old_count_after_reset}, response={r2.text[:1000]}"
    )

    assert first_count_after_reset == 2, (
        f"First reset metadata file should persist exactly 2 rows. "
        f"count={first_count_after_reset}, response={r2.text[:1000]}"
    )

    assert second_count_after_reset == 2, (
        f"Second reset metadata file should persist exactly 2 rows. "
        f"count={second_count_after_reset}, response={r2.text[:1000]}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Successful reset multi-import response should include a meta object. "
        f"Response was: {data!r}"
    )

    assert meta.get("min_year") == 2023, (
        f"Successful reset multi-import should report replacement min_year=2023. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") == 2025, (
        f"Successful reset multi-import should report replacement max_year=2025. "
        f"Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"Successful reset multi-import should report one result per replacement file. "
        f"Response was: {data!r}"
    )

    result_years = [
        (
            item.get("filename"),
            item.get("min_year"),
            item.get("max_year"),
        )
        for item in results
        if isinstance(item, dict)
    ]

    assert ("smoke_reset_success_meta_2023.csv", 2023, 2023) in result_years, (
        f"First replacement file should report per-file year range 2023..2023. "
        f"Result years were: {result_years!r}. Response was: {data!r}"
    )

    assert ("smoke_reset_success_meta_2025.csv", 2025, 2025) in result_years, (
        f"Second replacement file should report per-file year range 2025..2025. "
        f"Result years were: {result_years!r}. Response was: {data!r}"
    )


def test_csv_import_wrapper_success_meta_reports_imported_year_range():
    memo_tag = f"smoke-wrapper-success-meta-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This wrapper metadata test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on wrapper metadata import. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint} should return JSON for wrapper metadata validation. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV import endpoint {endpoint} should return JSON. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV import endpoint {endpoint} should return a JSON object"

    assert _count_transactions_by_memo_fragment(memo_tag) == 2, (
        f"Wrapper import should persist exactly 2 rows. Response was: {r.text[:1000]}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Wrapper import response should include a meta object. Response was: {data!r}"
    )

    assert meta.get("min_year") == 2022, (
        f"Wrapper import should report global min_year=2022. Response was: {data!r}"
    )

    assert meta.get("max_year") == 2024, (
        f"Wrapper import should report global max_year=2024. Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Wrapper import should return a non-empty results list. Response was: {data!r}"
    )

    first_result = results[0]
    assert first_result.get("filename") == "smoke_transactions.csv", (
        f"Wrapper import result should report uploaded filename. Response was: {data!r}"
    )

    assert first_result.get("inserted") == 2, (
        f"Wrapper import should report inserted=2. Response was: {data!r}"
    )

    assert first_result.get("min_year") == 2022, (
        f"Wrapper import result should report min_year=2022. Response was: {data!r}"
    )

    assert first_result.get("max_year") == 2024, (
        f"Wrapper import result should report max_year=2024. Response was: {data!r}"
    )


def test_csv_import_wrapper_malformed_file_reports_safe_meta_and_no_persistence():
    memo_tag = f"smoke-wrapper-bad-meta-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid-looking buy
not-a-date,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} invalid sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This wrapper malformed metadata test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on malformed wrapper import. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_count == before_count, (
        f"Malformed wrapper import should not persist any rows. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed wrapper import status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV import endpoint {endpoint} should return JSON validation feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"Malformed wrapper import should report validation errors. Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Malformed wrapper import response should include a meta object. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Malformed wrapper import should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Malformed wrapper import should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Malformed wrapper import should return a non-empty results list. Response was: {data!r}"
    )

    first_result = results[0]

    assert first_result.get("filename") == "smoke_transactions.csv", (
        f"Malformed wrapper result should report uploaded filename. Response was: {data!r}"
    )

    inserted = first_result.get("inserted")
    if isinstance(inserted, int):
        assert inserted == 0, (
            f"Malformed wrapper import should report inserted=0. Response was: {data!r}"
        )

    skipped_errors = first_result.get("skipped_errors")
    if isinstance(skipped_errors, int):
        assert skipped_errors >= 1, (
            f"Malformed wrapper import should report skipped_errors>=1. Response was: {data!r}"
        )


def test_csv_import_wrapper_duplicate_file_does_not_create_duplicate_transactions():
    memo_tag = f"smoke-wrapper-dupe-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r1, endpoint = _try_csv_import_endpoint(csv_text)

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This wrapper duplicate test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r1.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on first wrapper duplicate import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint} rejected first wrapper duplicate import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    after_first_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_first_count - before_count == 2, (
        f"First wrapper import should persist exactly 2 rows. "
        f"before={before_count}, after_first={after_first_count}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_endpoint(csv_text)
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV import endpoint {endpoint2} must not crash on duplicate wrapper import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    assert r2.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint2} rejected duplicate wrapper import unexpectedly: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    after_second_count = _count_transactions_by_memo_fragment(memo_tag)

    assert after_second_count == after_first_count, (
        f"Duplicate wrapper import should not create additional transaction rows. "
        f"after_first={after_first_count}, after_second={after_second_count}, response={r2.text[:1000]}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"Duplicate wrapper import should return JSON. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"Duplicate wrapper import should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Duplicate wrapper import should return a non-empty results list. Response was: {data!r}"
    )

    first_result = results[0]

    inserted = first_result.get("inserted")
    if isinstance(inserted, int):
        assert inserted == 0, (
            f"Duplicate wrapper import should report inserted=0. Response was: {data!r}"
        )

    skipped_duplicates = first_result.get("skipped_duplicates")
    if isinstance(skipped_duplicates, int):
        assert skipped_duplicates >= 2, (
            f"Duplicate wrapper import should report skipped_duplicates>=2. Response was: {data!r}"
        )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Duplicate wrapper import response should include meta. Response was: {data!r}"
    )

    assert meta.get("min_year") == 2022, (
        f"Duplicate wrapper import should still report detected min_year=2022. Response was: {data!r}"
    )

    assert meta.get("max_year") == 2024, (
        f"Duplicate wrapper import should still report detected max_year=2024. Response was: {data!r}"
    )


def test_csv_import_wrapper_returns_deprecation_warning_header():
    memo_tag = f"smoke-wrapper-warning-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This deprecation warning test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on deprecation warning test. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint} rejected valid wrapper warning import: "
        f"{r.status_code} {r.text[:1000]}"
    )

    warning = r.headers.get("warning") or r.headers.get("Warning")
    assert warning, (
        f"Deprecated CSV import endpoint {endpoint} should return a Warning header. "
        f"Headers were: {dict(r.headers)!r}"
    )

    warning_lower = warning.lower()

    assert "deprecated" in warning_lower, (
        f"Warning header should mention deprecation. Warning was: {warning!r}"
    )

    assert "import/multiple" in warning_lower, (
        f"Warning header should tell clients to use /import/multiple. Warning was: {warning!r}"
    )

    assert _count_transactions_by_memo_fragment(memo_tag) == 2, (
        f"Wrapper warning import should still persist exactly 2 rows. Response was: {r.text[:1000]}"
    )


def test_csv_import_wrapper_malformed_response_returns_deprecation_warning_header():
    memo_tag = f"smoke-wrapper-bad-warning-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid-looking buy
not-a-date,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} invalid sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This malformed deprecation warning test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on malformed warning test. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count == before_count, (
        f"Malformed wrapper warning import should not persist rows. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    warning = r.headers.get("warning") or r.headers.get("Warning")
    assert warning, (
        f"Deprecated CSV import endpoint {endpoint} should return a Warning header even on malformed input. "
        f"Headers were: {dict(r.headers)!r}"
    )

    warning_lower = warning.lower()

    assert "deprecated" in warning_lower, (
        f"Warning header should mention deprecation. Warning was: {warning!r}"
    )

    assert "import/multiple" in warning_lower, (
        f"Warning header should tell clients to use /import/multiple. Warning was: {warning!r}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed wrapper warning status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"Malformed wrapper warning response should be JSON. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"Malformed wrapper warning response should be a JSON object"

    assert _csv_response_reports_errors(data), (
        f"Malformed wrapper warning response should report validation errors. Response was: {data!r}"
    )


def test_csv_import_wrapper_duplicate_response_returns_deprecation_warning_header():
    memo_tag = f"smoke-wrapper-dupe-warning-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    r1, endpoint = _try_csv_import_endpoint(csv_text)

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This duplicate deprecation warning test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r1.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on first duplicate warning import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint} rejected first duplicate warning import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(memo_tag) == 2, (
        f"First wrapper duplicate warning import should persist exactly 2 rows. "
        f"Response was: {r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_endpoint(csv_text)
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV import endpoint {endpoint2} must not crash on duplicate warning import. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    assert r2.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint2} rejected duplicate warning import unexpectedly: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    assert _count_transactions_by_memo_fragment(memo_tag) == 2, (
        f"Duplicate wrapper warning import should not create additional rows. "
        f"Response was: {r2.text[:1000]}"
    )

    warning = r2.headers.get("warning") or r2.headers.get("Warning")
    assert warning, (
        f"Deprecated CSV import endpoint {endpoint2} should return a Warning header on duplicate import. "
        f"Headers were: {dict(r2.headers)!r}"
    )

    warning_lower = warning.lower()

    assert "deprecated" in warning_lower, (
        f"Warning header should mention deprecation. Warning was: {warning!r}"
    )

    assert "import/multiple" in warning_lower, (
        f"Warning header should tell clients to use /import/multiple. Warning was: {warning!r}"
    )

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"Duplicate wrapper warning response should be JSON. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"Duplicate wrapper warning response should be a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Duplicate wrapper warning response should include results. Response was: {data!r}"
    )

    first_result = results[0]

    inserted = first_result.get("inserted")
    if isinstance(inserted, int):
        assert inserted == 0, (
            f"Duplicate wrapper warning import should report inserted=0. Response was: {data!r}"
        )

    skipped_duplicates = first_result.get("skipped_duplicates")
    if isinstance(skipped_duplicates, int):
        assert skipped_duplicates >= 2, (
            f"Duplicate wrapper warning import should report skipped_duplicates>=2. "
            f"Response was: {data!r}"
        )


def test_csv_import_multiple_rejects_non_csv_filename_cleanly():
    memo_tag = f"smoke-multiple-non-csv-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid content wrong extension
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_multiple_endpoint(
        csv_text,
        reset=False,
        filename="smoke_transactions.txt",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on non-CSV filename. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count == before_count, (
        f"CSV multi-import endpoint {endpoint} should not persist rows from non-CSV filename. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    assert r.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected non-CSV filename status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    if r.status_code in (400, 409, 415, 422):
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON for non-CSV filename feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint} accepted non-CSV filename but did not report errors. "
        f"Response was: {data!r}"
    )

    text = json.dumps(data, default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"CSV multi-import endpoint {endpoint} should explain non-CSV filename rejection. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_reset_rejects_non_csv_filename_without_data_loss():
    existing_memo_tag = f"smoke-reset-non-csv-existing-{uuid.uuid4().hex}"
    bad_memo_tag = f"smoke-reset-non-csv-bad-{uuid.uuid4().hex}"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    bad_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{bad_memo_tag} valid content wrong extension
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename="smoke_reset_non_csv_existing.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    bad_count_before_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_endpoint(
        bad_csv_text,
        reset=True,
        filename="smoke_transactions.txt",
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset non-CSV filename. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    bad_count_after_reset = _count_transactions_by_memo_fragment(bad_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"reset=true with non-CSV filename must preserve existing rows. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert bad_count_after_reset == bad_count_before_reset, (
        f"reset=true with non-CSV filename must not import replacement rows. "
        f"before={bad_count_before_reset}, after={bad_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert r2.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected reset non-CSV filename status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 415, 422):
        return

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON for reset non-CSV feedback. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint2} accepted reset non-CSV filename but did not report errors. "
        f"Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"reset non-CSV response should include meta. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"reset non-CSV response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"reset non-CSV response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )

    text = json.dumps(data, default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"CSV multi-import endpoint {endpoint2} should explain reset non-CSV filename rejection. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_rejects_mixed_csv_and_non_csv_batch_atomically():
    csv_memo_tag = f"smoke-mixed-non-csv-valid-{uuid.uuid4().hex}"
    txt_memo_tag = f"smoke-mixed-non-csv-bad-{uuid.uuid4().hex}"

    csv_filename = "smoke_mixed_non_csv_valid.csv"
    txt_filename = "smoke_mixed_non_csv_bad.txt"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{csv_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{csv_memo_tag} sell
"""

    txt_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2023-01-01T12:00:00Z,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{txt_memo_tag} valid content wrong extension
"""

    before_csv_count = _count_transactions_by_memo_fragment(csv_memo_tag)
    before_txt_count = _count_transactions_by_memo_fragment(txt_memo_tag)

    r, endpoint = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=csv_text,
        csv_text_2=txt_text,
        reset=False,
        filename_1=csv_filename,
        filename_2=txt_filename,
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on mixed CSV/non-CSV batch. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_csv_count = _count_transactions_by_memo_fragment(csv_memo_tag)
    after_txt_count = _count_transactions_by_memo_fragment(txt_memo_tag)

    assert after_csv_count == before_csv_count, (
        f"Mixed CSV/non-CSV batch should not persist rows from the valid CSV file. "
        f"before={before_csv_count}, after={after_csv_count}, response={r.text[:1000]}"
    )

    assert after_txt_count == before_txt_count, (
        f"Mixed CSV/non-CSV batch should not persist rows from the non-CSV file. "
        f"before={before_txt_count}, after={after_txt_count}, response={r.text[:1000]}"
    )

    assert r.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected mixed CSV/non-CSV batch status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    if r.status_code in (400, 409, 415, 422):
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON for mixed CSV/non-CSV feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint} accepted mixed CSV/non-CSV batch without errors. "
        f"Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"Mixed CSV/non-CSV batch should return one result per uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == csv_filename, (
        f"First result should correspond to first uploaded CSV file. "
        f"Expected {csv_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == txt_filename, (
        f"Second result should correspond to second uploaded non-CSV file. "
        f"Expected {txt_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    first_inserted = results[0].get("inserted")
    if isinstance(first_inserted, int):
        assert first_inserted == 0, (
            f"Preflight failure should report inserted=0 for valid CSV file in mixed non-CSV batch. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Non-CSV file result should report validation errors. Response was: {data!r}"
    )

    text = json.dumps(results[1], default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"Non-CSV file result should explain CSV/file validation failure. "
        f"Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Mixed CSV/non-CSV response should include meta. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Mixed CSV/non-CSV rejected response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Mixed CSV/non-CSV rejected response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_reset_rejects_mixed_csv_and_non_csv_batch_without_data_loss():
    existing_memo_tag = f"smoke-reset-mixed-non-csv-existing-{uuid.uuid4().hex}"
    csv_memo_tag = f"smoke-reset-mixed-non-csv-valid-{uuid.uuid4().hex}"
    txt_memo_tag = f"smoke-reset-mixed-non-csv-bad-{uuid.uuid4().hex}"

    csv_filename = "smoke_reset_mixed_non_csv_valid.csv"
    txt_filename = "smoke_reset_mixed_non_csv_bad.txt"

    existing_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,DOGE,1000,EUR,100,EUR,0,SmokeCSV,{existing_memo_tag} buy
2024-06-01T12:00:00Z,SELL,DOGE,400,EUR,80,EUR,0,SmokeCSV,{existing_memo_tag} sell
"""

    replacement_csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{csv_memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{csv_memo_tag} sell
"""

    txt_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2023-01-01T12:00:00Z,BUY,ETH,1.00,EUR,2000,EUR,0,SmokeCSV,{txt_memo_tag} valid content wrong extension
"""

    r1, endpoint = _try_csv_import_multiple_endpoint(
        existing_csv_text,
        reset=False,
        filename="smoke_reset_mixed_non_csv_existing.csv",
    )

    if r1.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r1.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on setup import. "
        f"status={r1.status_code}, body={r1.text[:1000]}"
    )

    assert r1.status_code in (200, 201, 202, 204), (
        f"CSV multi-import endpoint {endpoint} rejected setup import: "
        f"{r1.status_code} {r1.text[:1000]}"
    )

    existing_count_before_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    csv_count_before_reset = _count_transactions_by_memo_fragment(csv_memo_tag)
    txt_count_before_reset = _count_transactions_by_memo_fragment(txt_memo_tag)

    assert existing_count_before_reset == 2, (
        f"Setup import should persist exactly 2 existing rows. "
        f"count={existing_count_before_reset}, response={r1.text[:1000]}"
    )

    r2, endpoint2 = _try_csv_import_multiple_two_files_endpoint(
        csv_text_1=replacement_csv_text,
        csv_text_2=txt_text,
        reset=True,
        filename_1=csv_filename,
        filename_2=txt_filename,
    )
    assert endpoint2 == endpoint

    if r2.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint2} requires auth/token in this build")

    assert r2.status_code < 500, (
        f"CSV multi-import endpoint {endpoint2} must not crash on reset mixed CSV/non-CSV batch. "
        f"status={r2.status_code}, body={r2.text[:1000]}"
    )

    existing_count_after_reset = _count_transactions_by_memo_fragment(existing_memo_tag)
    csv_count_after_reset = _count_transactions_by_memo_fragment(csv_memo_tag)
    txt_count_after_reset = _count_transactions_by_memo_fragment(txt_memo_tag)

    assert existing_count_after_reset == existing_count_before_reset, (
        f"reset=true mixed CSV/non-CSV batch must preserve existing rows. "
        f"before={existing_count_before_reset}, after={existing_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert csv_count_after_reset == csv_count_before_reset, (
        f"reset=true mixed CSV/non-CSV batch must not persist valid replacement CSV rows. "
        f"before={csv_count_before_reset}, after={csv_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert txt_count_after_reset == txt_count_before_reset, (
        f"reset=true mixed CSV/non-CSV batch must not persist non-CSV replacement rows. "
        f"before={txt_count_before_reset}, after={txt_count_after_reset}, "
        f"response={r2.text[:1000]}"
    )

    assert r2.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected reset mixed CSV/non-CSV batch status from {endpoint2}: "
        f"{r2.status_code} {r2.text[:1000]}"
    )

    if r2.status_code in (400, 409, 415, 422):
        return

    ct = r2.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint2} should return JSON for reset mixed CSV/non-CSV feedback. "
        f"content-type={ct}, body={r2.text[:1000]}"
    )

    data = r2.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint2} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint2} accepted reset mixed CSV/non-CSV batch without errors. "
        f"Response was: {data!r}"
    )

    results = data.get("results")
    assert isinstance(results, list) and len(results) >= 2, (
        f"Reset mixed CSV/non-CSV batch should return one result per uploaded file. "
        f"Response was: {data!r}"
    )

    assert results[0].get("filename") == csv_filename, (
        f"First result should correspond to first uploaded replacement CSV file. "
        f"Expected {csv_filename!r}, got {results[0].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    assert results[1].get("filename") == txt_filename, (
        f"Second result should correspond to second uploaded non-CSV replacement file. "
        f"Expected {txt_filename!r}, got {results[1].get('filename')!r}. "
        f"Response was: {data!r}"
    )

    first_inserted = results[0].get("inserted")
    if isinstance(first_inserted, int):
        assert first_inserted == 0, (
            f"Preflight failure should report inserted=0 for valid CSV file in reset mixed non-CSV batch. "
            f"Response was: {data!r}"
        )

    assert _csv_response_reports_errors({"results": [results[1]]}), (
        f"Non-CSV replacement file result should report validation errors. Response was: {data!r}"
    )

    text = json.dumps(results[1], default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"Non-CSV replacement file result should explain CSV/file validation failure. "
        f"Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Reset mixed CSV/non-CSV response should include meta. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Reset mixed CSV/non-CSV rejected response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Reset mixed CSV/non-CSV rejected response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )


def test_csv_import_wrapper_rejects_non_csv_filename_with_warning_header():
    memo_tag = f"smoke-wrapper-non-csv-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid content wrong extension
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    endpoints = [
        "/import/csv",
        "/api/import/csv",
        "/api/v1/import/csv",
    ]

    selected_response = None
    selected_endpoint = None

    for endpoint in endpoints:
        files = {
            "file": (
                "smoke_transactions.txt",
                csv_text.encode("utf-8"),
                "text/plain",
            )
        }

        r = client.post(endpoint, files=files)

        if r.status_code not in (404, 405):
            selected_response = r
            selected_endpoint = endpoint
            break

    if selected_response is None:
        pytest.skip("No CSV import wrapper endpoint is available in this build")

    r = selected_response
    endpoint = selected_endpoint

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This wrapper non-CSV test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on non-CSV filename. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count == before_count, (
        f"CSV import endpoint {endpoint} should not persist rows from non-CSV filename. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    warning = r.headers.get("warning") or r.headers.get("Warning")
    assert warning, (
        f"Deprecated CSV import endpoint {endpoint} should return a Warning header on non-CSV filename. "
        f"Headers were: {dict(r.headers)!r}"
    )

    warning_lower = warning.lower()

    assert "deprecated" in warning_lower, (
        f"Warning header should mention deprecation. Warning was: {warning!r}"
    )

    assert "import/multiple" in warning_lower, (
        f"Warning header should tell clients to use /import/multiple. Warning was: {warning!r}"
    )

    assert r.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected wrapper non-CSV filename status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    if r.status_code in (400, 409, 415, 422):
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV import endpoint {endpoint} should return JSON for non-CSV filename feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV import endpoint {endpoint} accepted non-CSV filename but did not report errors. "
        f"Response was: {data!r}"
    )

    meta = data.get("meta")
    assert isinstance(meta, dict), (
        f"Wrapper non-CSV response should include meta. Response was: {data!r}"
    )

    assert meta.get("min_year") is None, (
        f"Wrapper non-CSV response should not expose global min_year as imported. "
        f"Response was: {data!r}"
    )

    assert meta.get("max_year") is None, (
        f"Wrapper non-CSV response should not expose global max_year as imported. "
        f"Response was: {data!r}"
    )

    text = json.dumps(data, default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"CSV import endpoint {endpoint} should explain non-CSV filename rejection. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_rejects_uppercase_non_csv_final_extension():
    memo_tag = f"smoke-multiple-upper-non-csv-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} valid content wrong extension
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_multiple_endpoint(
        csv_text,
        reset=False,
        filename="smoke_transactions.CSV.TXT",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on uppercase non-CSV final extension. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count == before_count, (
        f"CSV multi-import endpoint {endpoint} should not persist rows from uppercase non-CSV final extension. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    assert r.status_code in (400, 409, 415, 422, 200, 201, 202), (
        f"Unexpected uppercase non-CSV final extension status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    if r.status_code in (400, 409, 415, 422):
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON for uppercase non-CSV feedback. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV multi-import endpoint {endpoint} accepted uppercase non-CSV final extension without errors. "
        f"Response was: {data!r}"
    )

    text = json.dumps(data, default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"CSV multi-import endpoint {endpoint} should explain uppercase non-CSV rejection. "
        f"Response was: {data!r}"
    )


def test_csv_import_multiple_accepts_uppercase_csv_extension():
    memo_tag = f"smoke-multiple-upper-csv-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    r, endpoint = _try_csv_import_multiple_endpoint(
        csv_text,
        reset=False,
        filename="smoke_transactions.CSV",
    )

    if r.status_code in (401, 403):
        pytest.skip(f"CSV multi-import endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV multi-import endpoint {endpoint} must not crash on uppercase .CSV extension. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV multi-import endpoint {endpoint} should accept uppercase .CSV extension. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count - before_count == 2, (
        f"CSV multi-import endpoint {endpoint} should persist exactly 2 rows from uppercase .CSV file. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV multi-import endpoint {endpoint} should return JSON for uppercase .CSV import. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV multi-import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Uppercase .CSV response should include results. Response was: {data!r}"
    )

    first_result = results[0]

    assert first_result.get("filename") == "smoke_transactions.CSV", (
        f"Uppercase .CSV response should preserve the uploaded filename. Response was: {data!r}"
    )

    inserted = first_result.get("inserted")
    if isinstance(inserted, int):
        assert inserted == 2, (
            f"Uppercase .CSV import should report inserted=2. Response was: {data!r}"
        )

    skipped_errors = first_result.get("skipped_errors")
    if isinstance(skipped_errors, int):
        assert skipped_errors == 0, (
            f"Uppercase .CSV import should report skipped_errors=0. Response was: {data!r}"
        )


def test_csv_import_wrapper_accepts_uppercase_csv_extension_with_warning_header():
    memo_tag = f"smoke-wrapper-upper-csv-{uuid.uuid4().hex}"

    csv_text = f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2022-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,{memo_tag} buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,{memo_tag} sell
"""

    before_count = _count_transactions_by_memo_fragment(memo_tag)

    endpoints = [
        "/import/csv",
        "/api/import/csv",
        "/api/v1/import/csv",
    ]

    selected_response = None
    selected_endpoint = None

    for endpoint in endpoints:
        files = {
            "file": (
                "smoke_transactions.CSV",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        }

        r = client.post(endpoint, files=files)

        if r.status_code not in (404, 405):
            selected_response = r
            selected_endpoint = endpoint
            break

    if selected_response is None:
        pytest.skip("No CSV import wrapper endpoint is available in this build")

    r = selected_response
    endpoint = selected_endpoint

    if r.status_code in (401, 403):
        pytest.skip(f"CSV import endpoint {endpoint} requires auth/token in this build")

    assert endpoint.endswith("/import/csv"), (
        f"This wrapper uppercase .CSV test should use /import/csv-compatible endpoint, got {endpoint!r}"
    )

    assert r.status_code < 500, (
        f"CSV import endpoint {endpoint} must not crash on uppercase .CSV filename. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202), (
        f"CSV import endpoint {endpoint} should accept uppercase .CSV filename. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    after_count = _count_transactions_by_memo_fragment(memo_tag)
    assert after_count - before_count == 2, (
        f"CSV import endpoint {endpoint} should persist exactly 2 rows from uppercase .CSV file. "
        f"before={before_count}, after={after_count}, response={r.text[:1000]}"
    )

    warning = r.headers.get("warning") or r.headers.get("Warning")
    assert warning, (
        f"Deprecated CSV import endpoint {endpoint} should return a Warning header on uppercase .CSV import. "
        f"Headers were: {dict(r.headers)!r}"
    )

    warning_lower = warning.lower()

    assert "deprecated" in warning_lower, (
        f"Warning header should mention deprecation. Warning was: {warning!r}"
    )

    assert "import/multiple" in warning_lower, (
        f"Warning header should tell clients to use /import/multiple. Warning was: {warning!r}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV import endpoint {endpoint} should return JSON for uppercase .CSV import. "
        f"content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV import endpoint {endpoint} should return a JSON object"

    results = data.get("results")
    assert isinstance(results, list) and results, (
        f"Wrapper uppercase .CSV response should include results. Response was: {data!r}"
    )

    first_result = results[0]

    assert first_result.get("filename") == "smoke_transactions.CSV", (
        f"Wrapper uppercase .CSV response should preserve uploaded filename. Response was: {data!r}"
    )

    inserted = first_result.get("inserted")
    if isinstance(inserted, int):
        assert inserted == 2, (
            f"Wrapper uppercase .CSV import should report inserted=2. Response was: {data!r}"
        )

    skipped_errors = first_result.get("skipped_errors")
    if isinstance(skipped_errors, int):
        assert skipped_errors == 0, (
            f"Wrapper uppercase .CSV import should report skipped_errors=0. Response was: {data!r}"
        )


def test_csv_upload_or_import_accepts_valid_buy_sell_file():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,smoke csv buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,smoke csv sell
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on valid CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (200, 201, 202, 204, 400, 422), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code in (400, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected a valid minimal BUY/SELL CSV: {r.text[:1000]}"
        )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    if "application/json" not in ct:
        # Some import endpoints may redirect or return HTML/plain status; status already proved no crash.
        return

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_success(data), (
        f"CSV endpoint {endpoint} returned success status but did not report valid/imported rows. "
        f"Response was: {data!r}"
    )


def test_csv_upload_or_import_rejects_malformed_file_without_crashing():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,BUY,BTC,not-a-number,EUR,1000,EUR,0,SmokeCSV,bad amount and timestamp
2024-06-01T12:00:00Z,SELL,,0.04,EUR,600,EUR,0,SmokeCSV,missing asset
2024-06-02T12:00:00Z,SELL,ETH,,EUR,700,EUR,0,SmokeCSV,missing amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on malformed CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        # Explicit validation/business rejection is acceptable.
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected malformed CSV endpoint status from {endpoint}: "
        f"{r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    if "application/json" not in ct:
        pytest.fail(
            f"CSV endpoint {endpoint} accepted malformed CSV with non-JSON response: "
            f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
        )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted malformed CSV but did not report errors. "
        f"Response was: {data!r}"
    )

    total_errors = data.get("total_errors")
    if isinstance(total_errors, int):
        assert total_errors >= 1, (
            f"CSV endpoint {endpoint} should report at least one malformed row. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_missing_required_asset_field():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,,0.04,EUR,600,EUR,0,SmokeCSV,missing base asset
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on missing base_asset. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        # Explicit validation/business rejection is acceptable.
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted missing base_asset but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a row with missing base_asset as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert not preview, (
            f"CSV endpoint {endpoint} should not preview a row with missing base_asset as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_missing_required_type_field():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,,BTC,0.04,EUR,600,EUR,0,SmokeCSV,missing transaction type
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on missing transaction type. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted missing transaction type but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a row with missing transaction type as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert not preview, (
            f"CSV endpoint {endpoint} should not preview a row with missing transaction type as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_missing_quote_asset_for_trade():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,,600,EUR,0,SmokeCSV,missing quote asset
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on missing quote_asset. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted missing quote_asset but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a trade row with missing quote_asset as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert not preview, (
            f"CSV endpoint {endpoint} should not preview a trade row with missing quote_asset as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_missing_quote_amount_for_trade():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,,EUR,0,SmokeCSV,missing quote amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on missing quote_amount. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted missing quote_amount but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a trade row with missing quote_amount as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert not preview, (
            f"CSV endpoint {endpoint} should not preview a trade row with missing quote_amount as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_allows_transfer_without_quote_data():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,TRANSFER,BTC,0.04,,,BTC,0.0001,SmokeCSV,wallet transfer without quote data
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on TRANSFER without quote data. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected a valid TRANSFER row without quote data: "
            f"{r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 1, (
            f"CSV endpoint {endpoint} should mark TRANSFER row without quote data as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert preview, (
            f"CSV endpoint {endpoint} should preview the valid TRANSFER row. "
            f"Response was: {data!r}"
        )

        first = preview[0]
        assert str(first.get("type") or "").lower() == "transfer"
        assert str(first.get("base_asset") or "").upper() == "BTC"


def test_csv_upload_or_import_rejects_zero_base_amount_for_trade():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0,EUR,600,EUR,0,SmokeCSV,zero base amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on zero base_amount. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted zero base_amount but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a zero base_amount trade row as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_negative_quote_amount_for_trade():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,-600,EUR,0,SmokeCSV,negative quote amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on negative quote_amount. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted negative quote_amount but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a negative quote_amount trade row as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_negative_fee_amount():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,-1,SmokeCSV,negative fee amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on negative fee_amount. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted negative fee_amount but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a negative fee_amount row as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_positive_fee_without_fee_asset():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,,1,SmokeCSV,fee amount without asset
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on fee_amount without fee_asset. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted positive fee_amount without fee_asset but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark fee_amount without fee_asset as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_fee_asset_without_fee_amount():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,,SmokeCSV,fee asset without amount
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on fee_asset without fee_amount. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted fee_asset without fee_amount but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark fee_asset without fee_amount as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_unsupported_transaction_type():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,AIRDROP,BTC,0.04,EUR,600,EUR,0,SmokeCSV,unsupported transaction type
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on unsupported transaction type. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted unsupported transaction type but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark unsupported transaction type as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_accepts_transfer_aliases():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z,transfer_in,BTC,0.04,,,BTC,0.0001,SmokeCSV,transfer alias in
2024-06-02T12:00:00Z,transfer-out,BTC,0.02,,,BTC,0.0001,SmokeCSV,transfer alias out
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on transfer aliases. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected valid transfer aliases: {r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 2, (
            f"CSV endpoint {endpoint} should mark transfer aliases as valid rows. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert len(preview) >= 2, (
            f"CSV endpoint {endpoint} should preview both transfer alias rows. "
            f"Response was: {data!r}"
        )

        preview_types = {str(row.get("type") or "").lower() for row in preview}
        assert preview_types == {"transfer"}, (
            f"CSV endpoint {endpoint} should normalize transfer aliases to transfer. "
            f"Preview types were: {preview_types!r}. Response was: {data!r}"
        )


def test_csv_upload_or_import_trims_and_normalizes_required_fields():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-06-01T12:00:00Z, SELL , btc ,0.04, eur ,600, eur ,0,SmokeCSV,fields with whitespace
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on whitespace-padded required fields. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected whitespace-padded valid fields: {r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 1, (
            f"CSV endpoint {endpoint} should mark whitespace-padded valid fields as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert preview, (
            f"CSV endpoint {endpoint} should preview the whitespace-padded valid row. "
            f"Response was: {data!r}"
        )

        first = preview[0]
        assert str(first.get("type") or "").lower() == "sell"
        assert str(first.get("base_asset") or "").upper() == "BTC"
        assert str(first.get("quote_asset") or "").upper() == "EUR"


def test_csv_upload_or_import_accepts_semicolon_delimited_file():
    csv_text = """timestamp;type;base_asset;base_amount;quote_asset;quote_amount;fee_asset;fee_amount;exchange;memo
2024-01-01T12:00:00Z;BUY;BTC;0.10;EUR;1000;EUR;0;SmokeCSV;semicolon buy
2024-06-01T12:00:00Z;SELL;BTC;0.04;EUR;600;EUR;0;SmokeCSV;semicolon sell
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on semicolon-delimited CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected valid semicolon-delimited CSV: {r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 2, (
            f"CSV endpoint {endpoint} should mark semicolon-delimited rows as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert len(preview) >= 2, (
            f"CSV endpoint {endpoint} should preview semicolon-delimited rows. "
            f"Response was: {data!r}"
        )

        first = preview[0]
        assert str(first.get("type") or "").lower() == "buy"
        assert str(first.get("base_asset") or "").upper() == "BTC"
        assert str(first.get("quote_asset") or "").upper() == "EUR"


def test_csv_upload_or_import_accepts_quoted_commas_in_fields():
    csv_text = '''timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,"Smoke, CSV","buy, with comma"
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,"Smoke, CSV","sell, with comma"
'''

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on quoted commas. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected valid quoted-comma CSV: {r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 2, (
            f"CSV endpoint {endpoint} should mark quoted-comma rows as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert len(preview) >= 2, (
            f"CSV endpoint {endpoint} should preview quoted-comma rows. "
            f"Response was: {data!r}"
        )

        first = preview[0]
        assert first.get("exchange") == "Smoke, CSV"
        assert first.get("memo") == "buy, with comma"


def test_csv_upload_or_import_accepts_utf8_bom_file():
    csv_text = "\ufeff" + """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,utf8 bom buy
2024-06-01T12:00:00Z,SELL,BTC,0.04,EUR,600,EUR,0,SmokeCSV,utf8 bom sell
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on UTF-8 BOM CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        pytest.fail(
            f"CSV endpoint {endpoint} rejected valid UTF-8 BOM CSV: {r.text[:1000]}"
        )

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid >= 2, (
            f"CSV endpoint {endpoint} should mark UTF-8 BOM rows as valid. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert len(preview) >= 2, (
            f"CSV endpoint {endpoint} should preview UTF-8 BOM rows. "
            f"Response was: {data!r}"
        )

        first = preview[0]
        assert str(first.get("type") or "").lower() == "buy"
        assert str(first.get("base_asset") or "").upper() == "BTC"
        assert str(first.get("quote_asset") or "").upper() == "EUR"


def test_csv_upload_or_import_rejects_empty_file_cleanly():
    csv_text = ""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on empty CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (400, 409, 422), (
        f"CSV endpoint {endpoint} should reject empty CSV with a validation/business error. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for empty-file validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    text = json.dumps(data, default=str).lower()
    assert "empty" in text or "file" in text or "csv" in text, (
        f"CSV endpoint {endpoint} should explain empty-file rejection. "
        f"Response was: {data!r}"
    )


def test_csv_upload_or_import_rejects_non_csv_filename_cleanly():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,valid content wrong extension
"""

    endpoints = [
        "/upload/csv",
        "/import/csv",
        "/api/upload/csv",
        "/api/import/csv",
        "/api/v1/upload/csv",
        "/api/v1/import/csv",
    ]

    selected_response = None
    selected_endpoint = None

    for endpoint in endpoints:
        files = {
            "file": (
                "smoke_transactions.txt",
                csv_text.encode("utf-8"),
                "text/plain",
            )
        }

        r = client.post(endpoint, files=files)

        if r.status_code not in (404, 405):
            selected_response = r
            selected_endpoint = endpoint
            break

    if selected_response is None:
        pytest.skip("No CSV upload/import endpoint is available in this build")

    r = selected_response
    endpoint = selected_endpoint

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on non-CSV filename. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    assert r.status_code in (400, 409, 415, 422), (
        f"CSV endpoint {endpoint} should reject non-CSV filename with a validation/business error. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for non-CSV filename validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    text = json.dumps(data, default=str).lower()
    assert ".csv" in text or "csv" in text or "file" in text, (
        f"CSV endpoint {endpoint} should explain non-CSV filename rejection. "
        f"Response was: {data!r}"
    )


def test_csv_upload_or_import_handles_header_only_file_cleanly():
    csv_text = "timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo\n"

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on header-only CSV. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        # Explicit validation/business rejection is acceptable.
        return

    assert r.status_code in (200, 201, 202, 204), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    if r.status_code == 204:
        return

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for header-only CSV feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark header-only CSV as having valid rows. "
            f"Response was: {data!r}"
        )

    preview = data.get("preview_first_5")
    if isinstance(preview, list):
        assert preview == [], (
            f"CSV endpoint {endpoint} should not preview rows for header-only CSV. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_duplicate_headers_cleanly():
    csv_text = """timestamp,type,base_asset,base_amount,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,999,EUR,1000,EUR,0,SmokeCSV,duplicate header
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on duplicate CSV headers. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for duplicate-header validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted duplicate headers but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark duplicate-header CSV rows as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_blank_headers_cleanly():
    csv_text = """timestamp,type,base_asset,,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,ignored,0.10,EUR,1000,EUR,0,SmokeCSV,blank header
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on blank CSV headers. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for blank-header validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted blank headers but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark blank-header CSV rows as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_rows_with_extra_columns_cleanly():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000,EUR,0,SmokeCSV,extra column test,UNEXPECTED_EXTRA_VALUE
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on CSV rows with extra columns. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for extra-column validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted a row with extra columns but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a row with extra columns as valid. "
            f"Response was: {data!r}"
        )


def test_csv_upload_or_import_rejects_rows_with_missing_columns_cleanly():
    csv_text = """timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T12:00:00Z,BUY,BTC,0.10,EUR,1000
"""

    r, endpoint = _try_csv_upload_endpoint(csv_text)

    if r.status_code in (401, 403):
        pytest.skip(f"CSV endpoint {endpoint} requires auth/token in this build")

    assert r.status_code < 500, (
        f"CSV endpoint {endpoint} must not crash on CSV rows with missing columns. "
        f"status={r.status_code}, body={r.text[:1000]}"
    )

    if r.status_code in (400, 409, 422):
        return

    assert r.status_code in (200, 201, 202), (
        f"Unexpected CSV endpoint status from {endpoint}: {r.status_code} {r.text[:1000]}"
    )

    ct = r.headers.get("content-type", "").lower()
    assert "application/json" in ct, (
        f"CSV endpoint {endpoint} should return JSON for missing-column validation feedback. "
        f"status={r.status_code}, content-type={ct}, body={r.text[:1000]}"
    )

    data = r.json()
    assert isinstance(data, dict), f"CSV endpoint {endpoint} should return a JSON object"

    assert _csv_response_reports_errors(data), (
        f"CSV endpoint {endpoint} accepted a row with missing columns but did not report errors. "
        f"Response was: {data!r}"
    )

    total_valid = data.get("total_valid")
    if isinstance(total_valid, int):
        assert total_valid == 0, (
            f"CSV endpoint {endpoint} should not mark a row with missing columns as valid. "
            f"Response was: {data!r}"
        )


def test_populated_buy_sell_calculation_matches_expected_fifo_gain():
    memo_tag = f"smoke-deterministic-{uuid.uuid4().hex}"
    _insert_deterministic_btc_buy_sell_rows(memo_tag)

    run_id, payload = _call_calculate_v2_and_get_payload(jurisdiction="HR", load_demo=False)

    assert isinstance(payload, dict), "/calculate/v2 must return a JSON object"
    assert run_id > 0, "run_id should be a positive integer"

    r = client.get(f"/history/run/{run_id}/events.csv")
    if r.status_code in (404, 405, 422):
        pytest.skip("events.csv endpoint not available")

    assert r.status_code == 200, f"events.csv failed for populated run: {r.text}"

    ct = r.headers.get("content-type", "").lower()
    assert "text/csv" in ct or "application/csv" in ct, f"Unexpected content type: {ct}"

    lines = r.text.splitlines()
    assert len(lines) >= 2, (
        "A populated BUY -> SELL smoke calculation should produce "
        "a CSV header plus at least one realized event row"
    )

    header = lines[0].lower()
    assert "timestamp" in header
    assert "asset" in header
    assert "gain" in header or "gain_eur" in header

    _assert_csv_contains_expected_btc_fifo_result(r.text)

def test_multilot_fifo_calculation_matches_expected_gain():
    asset = f"SMKEDGE{uuid.uuid4().hex[:8].upper()}"
    memo_tag = f"smoke-multilot-{uuid.uuid4().hex}"

    _insert_deterministic_multilot_fifo_rows(asset=asset, memo_tag=memo_tag)

    run_id, payload = _call_calculate_v2_and_get_payload(jurisdiction="HR", load_demo=False)

    assert isinstance(payload, dict), "/calculate/v2 must return a JSON object"
    assert run_id > 0, "run_id should be a positive integer"

    r = client.get(f"/history/run/{run_id}/events.csv")
    if r.status_code in (404, 405, 422):
        pytest.skip("events.csv endpoint not available")

    assert r.status_code == 200, f"events.csv failed for multi-lot FIFO run: {r.text}"

    ct = r.headers.get("content-type", "").lower()
    assert "text/csv" in ct or "application/csv" in ct, f"Unexpected content type: {ct}"

    _assert_csv_contains_expected_asset_gain(
        csv_text=r.text,
        asset=asset,
        expected_gain=Decimal("1750"),
        expected_proceeds=Decimal("5000"),
        expected_cost=Decimal("3250"),
    )

def test_oversell_without_prior_buy_does_not_crash_or_silent_pass():
    asset = f"SMKSHORT{uuid.uuid4().hex[:8].upper()}"
    memo_tag = f"smoke-oversell-{uuid.uuid4().hex}"

    _insert_deterministic_oversell_row(asset=asset, memo_tag=memo_tag)

    res = client.post("/calculate/v2", json={"jurisdiction": "HR"})

    assert res.status_code < 500, (
        f"/calculate/v2 must not crash on oversell edge case. "
        f"status={res.status_code}, body={res.text[:1000]}"
    )

    if res.status_code in (400, 409, 422):
        # Accept explicit business/validation rejection.
        return

    assert res.status_code == 200, f"Unexpected oversell response: {res.status_code} {res.text[:1000]}"

    payload = res.json()
    assert isinstance(payload, dict), "Oversell response should be a JSON object"

    assert _payload_mentions_problem(payload, asset), (
        "Oversell run returned 200 but did not visibly report a warning/error "
        f"for asset {asset}. Payload was: {payload!r}"
    )


def test_strict_fx_missing_rate_does_not_silent_pass():
    asset = f"SMKFX{uuid.uuid4().hex[:8].upper()}"
    memo_tag = f"smoke-strict-fx-{uuid.uuid4().hex}"

    _insert_deterministic_missing_fx_rows(asset=asset, memo_tag=memo_tag)

    res = client.post(
        "/calculate/v2",
        json={
            "jurisdiction": "HR",
            "strict_fx": True,
            "fx_source": "HNB",
        },
    )

    assert res.status_code < 500, (
        f"/calculate/v2 must not crash when strict FX data is unavailable. "
        f"status={res.status_code}, body={res.text[:1000]}"
    )

    if res.status_code in (400, 409, 422):
        # Accept explicit business/validation rejection.
        return

    assert res.status_code == 200, f"Unexpected strict FX response: {res.status_code} {res.text[:1000]}"

    payload = res.json()
    assert isinstance(payload, dict), "strict FX response should be a JSON object"

    assert _payload_mentions_fx_problem(payload, asset), (
        "strict_fx=True run returned 200 but did not visibly report an FX warning/error "
        f"for asset {asset}. Payload was: {payload!r}"
    )


def test_calculate_creates_run_and_persists():
    run_id, payload = _call_calculate_v2_and_get_payload()
    # Minimal structural checks on response payload
    assert "summary" in payload or "eur_summary" in payload or "totals" in payload, (
        "calculate should include a summary-like section"
    )

    # Verify persistence via DB-backed API manifest (Option A)
    r = client.get(f"/api/v1/runs/{run_id}")
    assert r.status_code == 200, f"Run manifest not found for run_id={run_id}: {r.status_code} {r.text}"

    manifest = r.json()
    assert isinstance(manifest, dict), "run manifest must return a dict"
    assert manifest.get("id") == run_id, "manifest.id must match the run_id returned by /calculate/v2"
    assert "created_at" in manifest, "manifest should include created_at"


def test_calculate_is_idempotent_and_creates_new_runs():
    run_id1, _ = _call_calculate_v2_and_get_payload()
    run_id2, _ = _call_calculate_v2_and_get_payload()
    assert run_id1 != run_id2, "Calling /calculate twice should yield a new run_id the second time"


def test_history_download_zip_contains_manifest_with_run_id():
    run_id, _ = _call_calculate_v2_and_get_payload()
    content, url_used, status, txt = _try_download_zip(run_id)

    # If both endpoints are absent (404/405 etc.), SKIP rather than fail.
    if status in (404, 405, 422, 301, 302) and content is None:
        pytest.skip(f"history download endpoint not available (last tried {url_used}, status={status})")

    assert content is not None, f"Download failed from {url_used} (status={status}): {txt}"

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        names = set(zf.namelist())
        assert "manifest.json" in names, "ZIP must contain manifest.json"
        with zf.open("manifest.json") as fh:
            manifest = json.load(io.TextIOWrapper(fh, encoding="utf-8"))
        assert str(manifest.get("run_id")) == str(run_id), "manifest.run_id must match the requested run"
        assert "created_at" in manifest, "manifest should contain created_at"
        assert "events" in manifest or "items_count" in manifest or "outputs_hash" in manifest


def test_history_events_csv_if_present():
    run_id, payload = _call_calculate_v2_and_get_payload()
    r = client.get(f"/history/run/{run_id}/events.csv")
    if r.status_code in (404, 405, 422):
        pytest.skip("events.csv endpoint not available")
    assert r.status_code == 200, f"events.csv failed: {r.text}"

    ct = r.headers.get("content-type", "").lower()
    assert "text/csv" in ct or "application/csv" in ct, f"Unexpected content type: {ct}"

    lines = r.text.splitlines()
    assert len(lines) >= 1, "CSV should have a header row"

    header = lines[0]
    assert "timestamp" in header
    assert "asset" in header
    assert "gain" in header or "gain_eur" in header

    events = payload.get("events") or payload.get("realized_events") or []
    if events:
        assert len(lines) >= 2, "CSV should have at least one data row when calculation produced events"


def test_audit_history_list_if_present():
    r = client.get("/audit/history?limit=5")
    if r.status_code in (404, 405):
        pytest.skip("audit history endpoint not available")
    assert r.status_code == 200, f"/audit/history failed: {r.text}"
    data = r.json()
    assert isinstance(data, list), "/audit/history must return a list"
    for item in data[:3]:
        if isinstance(item, dict):
            assert "ts" in item or "timestamp" in item, "audit item should include a timestamp"
            assert "action" in item or "event" in item, "audit item should include an action/event"


def test_transaction_model_and_schema_roundtrip():

    db = SessionLocal()
    try:
        t = Transaction(
            timestamp=datetime.now(timezone.utc),
            type=TxType.BUY,
            base_asset="BTC", base_amount=Decimal("0.01"),
            quote_asset="EUR", quote_amount=Decimal("600"),
            fee_asset="EUR", fee_amount=Decimal("1.50"),
            exchange="TestEx", memo="schema check"
        )
        db.add(t); db.commit(); db.refresh(t)

        dto = TransactionRead.model_validate(t)
        data = dto.model_dump()
        assert data["base_asset"] == "BTC"
        assert data["quote_asset"] == "EUR"
        assert Decimal(data["base_amount"]).quantize(Decimal("0.00000001")) == Decimal("0.01000000")
        
    finally:
        db.close()

def _latest_zip_in_support_dir() -> pathlib.Path | None:
    root = pathlib.Path(__file__).resolve().parents[1]
    support_dir = root / "support_bundles"
    if not support_dir.exists():
        return None
    zips = sorted(support_dir.glob("support_bundle_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    return zips[0] if zips else None

def _validate_evolve_and_diagnostics(zip_path: pathlib.Path) -> None:
    assert zip_path and zip_path.exists(), f"Bundle zip not found: {zip_path}"
    with zipfile.ZipFile(zip_path, "r") as zf:
        files = set(zf.namelist())

        def must_exist(suffix: str):
            if not any(name.endswith(suffix) for name in files):
                raise AssertionError(f"Missing required artifact: {suffix}")

        # Policy + version/changelog
        must_exist("AUTO_EVOLVE.md")
        must_exist("_meta/AUTO_EVOLVE.md")
        must_exist("_meta/EVOLVE_RULE.json")
        must_exist("_meta/evolve_changelog.txt")

        # Core forensics
        must_exist("_meta/runtime.json")
        must_exist("_meta/filelist.txt")
        must_exist("_meta/repro.json")
        must_exist("_meta/states.log")

        # Git info may be absent on CI without a .git checkout
        if not any(n.endswith("_meta/git_status.txt") for n in files):
            print("NOTE: _meta/git_status.txt missing (likely no .git dir on this machine)")

        # DB checks: presence of expected/missing tables list
        must_exist("_db/expected_tables.txt")
        must_exist("_db/missing_tables.txt")

        # API: either real responses or skip markers in API context
        if not any(n.startswith("_api/GET_health") for n in files):
            must_exist("_api/api_diag_skipped.txt")

        # Optional: zip truncation note (non-fatal)
        if any(n.endswith("_meta/zip_truncated.txt") for n in files):
            print("NOTE: zip was truncated by safety caps (expected on API path for huge repos).")

        # Validate EVOLVE_RULE.json structure
        evo_name = next(n for n in files if n.endswith("_meta/EVOLVE_RULE.json"))
        data = json.loads(zf.read(evo_name))
        assert "version" in data and isinstance(data["version"], int) and data["version"] >= 1
        assert "artifacts" in data and isinstance(data["artifacts"], list)
        assert len(data["artifacts"]) >= 2, f"Expected artifacts tracked >=2, got {len(data['artifacts'])}"

def test_db_path_is_creatable():
    """
    Verify that the configured SQLite file path is creatable:
    - parent directory exists (or can be created)
    - file can be created/truncated
    """
    import os
    from cryptotaxcalc.db import SQLALCHEMY_DATABASE_URL

    db_path = str(SQLALCHEMY_DATABASE_URL)
    if db_path.startswith("sqlite:///"):
        db_file = db_path.replace("sqlite:///", "", 1)
    elif db_path.startswith("sqlite:////"):
        db_file = db_path.replace("sqlite:////", "", 1)
    else:
        # Non-sqlite URLs skip this check
        return

    parent = os.path.dirname(db_file) or "."
    # parent dir must exist or be creatable
    if not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    # Try touching the file
    try:
        with open(db_file, "ab"):
            pass
    except OSError as e:
        raise AssertionError(f"DB file not creatable at {db_file}: {e}")

@pytest.mark.smoke
def test_support_bundle_contains_evolve_artifacts():
    # Try via API first (preferred)
    token = os.getenv("BUNDLE_TOKEN") or os.getenv("ADMIN_TOKEN") or ""
    tried = []

    def _try_api(headers: dict) -> tuple[int, dict | None, str]:
        r = client.post("/admin/bundle", headers=headers, json={})
        tried.append((headers, r.status_code, r.text[:200]))
        if r.status_code == 200:
            try:
                return r.status_code, r.json(), ""
            except Exception as e:
                return r.status_code, None, f"invalid json: {e}"
        return r.status_code, None, r.text

    # header variations many apps use
    candidates = []
    if token:
        candidates.append({"X-Admin-Token": token})
        candidates.append({"X-Token": token})
        candidates.append({"Authorization": f"Bearer {token}"})
    else:
        candidates.append({})  # no token

    data = None
    status = None
    last_err = ""
    for h in candidates:
        status, data, last_err = _try_api(h)
        if status == 200 and data:
            break

    # If API still refused (401/403), try CLI fallback instead of skipping
    if status in (401, 403) or not data:
        root = pathlib.Path(__file__).resolve().parents[1]
        script = root / "automation" / "collect_support_bundle.py"
        assert script.exists(), f"collector missing: {script}"
        env = os.environ.copy()
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8")

        proc = subprocess.run(
            [os.sys.executable, "-u", str(script),
             "--api-base", os.getenv("API_BASE", "http://127.0.0.1:8000"),
             "--tail-lines", "200",
             "--keep-zips", "5"],
            cwd=str(script.parent),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
            env=env,
        )

        # parse ::zip:: marker
        zip_path = None
        for line in (proc.stdout or "").splitlines():
            s = line.strip()
            if s.startswith("::zip::"):
                zip_path = s.split("::zip::", 1)[1].strip()
                break
        if not zip_path:
            # last-resort: pick newest zip
            time.sleep(1.0)
            zip_path = _latest_zip_in_support_dir()
        else:
            zip_path = pathlib.Path(zip_path)

            # Windows locale/encoding guard: stdout decoding may drop non-ASCII chars in the absolute path.
            # Re-resolve by filename inside the known support_bundles directory.
            if not zip_path.exists():
                candidate = (root / "support_bundles" / zip_path.name)
                if candidate.exists():
                    zip_path = candidate

            # Final fallback: newest zip in support_bundles
            if not zip_path.exists():
                time.sleep(1.0)
                latest = _latest_zip_in_support_dir()
                if latest and latest.exists():
                    zip_path = latest

        assert zip_path and zip_path.exists(), (
            "No bundle zip was produced by API or CLI.\n"
            f"API tries: {tried}\n"
            f"CLI rc={proc.returncode}\nSTDOUT:\n{proc.stdout[-500:]}\nSTDERR:\n{proc.stderr[-500:]}"
        )
        _validate_evolve_and_diagnostics(zip_path)
        return

    # API success path
    zip_path_str = data.get("zip_path")
    zip_path = pathlib.Path(zip_path_str) if zip_path_str else None
    if not zip_path or not zip_path.exists():
        time.sleep(1.0)
        zip_path = _latest_zip_in_support_dir()
    assert zip_path and zip_path.exists(), "No bundle zip was produced by API or found in support_bundles/"
    _validate_evolve_and_diagnostics(zip_path)

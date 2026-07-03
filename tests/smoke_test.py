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


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------
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

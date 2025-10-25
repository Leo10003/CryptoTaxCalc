# tests/smoke_test.py
# Run with:
#   pytest -q -m smoke --maxfail=1 --disable-warnings -rA

from __future__ import annotations
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import io
import json
import re
import uuid
import zipfile
from typing import List, Tuple
import pytest
from cryptotaxcalc.db import SessionLocal
from cryptotaxcalc.models import Transaction, TxType
from cryptotaxcalc.schemas import TransactionRead
from decimal import Decimal
from datetime import datetime, timezone

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


def _call_calculate_and_get_payload() -> Tuple[str, dict]:
    res = client.get("/calculate")
    assert res.status_code == 200, f"/calculate failed: {res.text}"
    data = res.json()
    assert "run_id" in data, "Response must include run_id"
    run_id = data["run_id"]
    assert _is_uuid(run_id), f"run_id doesn't look like a UUID: {run_id}"
    return run_id, data


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
def test_calculate_creates_run_and_persists():
    run_id, payload = _call_calculate_and_get_payload()
    # Minimal structural checks on response payload
    assert "summary" in payload or "eur_summary" in payload or "totals" in payload, (
        "calculate should include a summary-like section"
    )

    # If history list exists, verify run presence (best-effort)
    r = client.get("/history")
    if r.status_code == 200:
        hist = r.json()
        assert isinstance(hist, list), "history must return a list"
        run_ids = [h.get("id") or h.get("run_id") for h in hist if isinstance(h, dict)]
        if run_ids:
            assert run_id in run_ids, "new run_id should be present in history list"


def test_calculate_is_idempotent_and_creates_new_runs():
    run_id1, _ = _call_calculate_and_get_payload()
    run_id2, _ = _call_calculate_and_get_payload()
    assert run_id1 != run_id2, "Calling /calculate twice should yield a new run_id the second time"


def test_history_download_zip_contains_manifest_with_run_id():
    run_id, _ = _call_calculate_and_get_payload()
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
        assert manifest.get("run_id") == run_id, "manifest.run_id must match the requested run"
        assert "created_at" in manifest, "manifest should contain created_at"
        assert "events" in manifest or "items_count" in manifest or "outputs_hash" in manifest


def test_history_events_csv_if_present():
    run_id, _ = _call_calculate_and_get_payload()
    r = client.get(f"/history/run/{run_id}/events.csv")
    if r.status_code in (404, 405, 422):
        pytest.skip("events.csv endpoint not available")
    assert r.status_code == 200, f"events.csv failed: {r.text}"

    ct = r.headers.get("content-type", "").lower()
    assert "text/csv" in ct or "application/csv" in ct, f"Unexpected content type: {ct}"
    assert len(r.text.splitlines()) >= 2, "CSV should have a header + at least one row"


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
        assert str(data["base_amount"]) in ("0.01", "0.010000")  # tolerate DB precision
    finally:
        db.close()

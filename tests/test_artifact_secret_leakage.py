from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request
from sqlalchemy import text

from cryptotaxcalc.app import app
from cryptotaxcalc import admin_ops
from cryptotaxcalc.db import SessionLocal, engine, init_db
from cryptotaxcalc.models import Base, CalcRun, RealizedEvent

pytestmark = pytest.mark.smoke

client = TestClient(app)

FAKE_SECRET = "ctc-secret-value-1234567890"
FAKE_BEARER = "Bearer ctc-secret-value-1234567890"
FAKE_DB_URL = "postgresql://tax_user:ctc-secret-value-1234567890@example.invalid/taxdb"


def _ensure_schema() -> None:
    init_db(engine)
    Base.metadata.create_all(bind=engine)


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/bundle",
            "headers": [],
            "client": ("127.0.0.1", 54321),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


def _create_run() -> int:
    _ensure_schema()
    asset = f"SEC{uuid.uuid4().hex[:8]}".upper()
    with SessionLocal() as db:
        run = CalcRun(
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            finished_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            jurisdiction="HR",
            rule_version="2025.1",
            tax_year=2025,
            lot_method="FIFO",
            fx_set_id=None,
            params_json={"jurisdiction": "HR", "tax_year": 2025},
            run_id=f"artifact-security-{uuid.uuid4()}",
            summary_json={"totals": {"proceeds": "100", "cost_basis": "40", "gain": "60"}},
        )
        db.add(run)
        db.flush()
        run_id = int(run.id)
        db.add(
            RealizedEvent(
                run_id=run_id,
                tx_id=None,
                timestamp="2025-06-01T00:00:00+00:00",
                asset=asset,
                qty_sold=Decimal("1"),
                proceeds=Decimal("100"),
                cost_basis=Decimal("40"),
                gain=Decimal("60"),
                quote_asset="EUR",
                fee_applied=Decimal("0"),
                matches_json=json.dumps([{"from_qty": "1", "lot_cost_total": "40"}]),
            )
        )
        db.commit()
    return run_id


def _delete_run(run_id: int) -> None:
    with SessionLocal() as db:
        db.execute(text("DELETE FROM realized_events WHERE run_id = :run_id"), {"run_id": run_id})
        db.execute(text("DELETE FROM run_digests WHERE run_id = :run_id"), {"run_id": run_id})
        db.execute(text("DELETE FROM run_inputs WHERE run_id = :run_id"), {"run_id": run_id})
        db.execute(text("DELETE FROM calc_runs WHERE id = :run_id"), {"run_id": run_id})
        db.commit()


def _assert_secret_absent_from_response(response) -> None:
    haystacks = [
        response.text,
        json.dumps(dict(response.headers), sort_keys=True),
    ]
    if response.content:
        haystacks.append(response.content.decode("latin-1", errors="ignore"))
    for haystack in haystacks:
        assert FAKE_SECRET not in haystack
        assert FAKE_DB_URL not in haystack
        assert FAKE_BEARER not in haystack


def test_events_csv_export_does_not_leak_env_values_request_tokens_or_database_urls(monkeypatch):
    monkeypatch.setenv("BUNDLE_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("ADMIN_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("DATABASE_URL", FAKE_DB_URL)
    run_id = _create_run()
    try:
        response = client.get(
            f"/export/events_csv?run_id={run_id}",
            headers={"Authorization": FAKE_BEARER, "X-Admin-Token": FAKE_SECRET},
        )

        assert response.status_code == 200, response.text
        _assert_secret_absent_from_response(response)
        rows = list(csv.DictReader(io.StringIO(response.text)))
        assert len(rows) == 1
        assert rows[0]["calc_run_id"] == str(run_id)
    finally:
        _delete_run(run_id)


def test_history_zip_download_manifest_does_not_leak_env_values_or_request_tokens(monkeypatch):
    monkeypatch.setenv("BUNDLE_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("ADMIN_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("DATABASE_URL", FAKE_DB_URL)
    run_id = _create_run()
    try:
        response = client.get(
            f"/history/{run_id}/download",
            headers={"Authorization": FAKE_BEARER, "Cookie": f"session={FAKE_SECRET}"},
        )

        assert response.status_code == 200, response.text
        assert response.headers.get("content-type", "").lower().startswith("application/zip")
        _assert_secret_absent_from_response(response)
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            names = zf.namelist()
            assert names == ["manifest.json"]
            manifest_text = zf.read("manifest.json").decode("utf-8")
            manifest = json.loads(manifest_text)
        assert manifest["id"] == run_id
        assert FAKE_SECRET not in manifest_text
        assert FAKE_DB_URL not in manifest_text
    finally:
        _delete_run(run_id)


def test_error_responses_do_not_echo_authorization_tokens_or_sensitive_env_values(monkeypatch):
    monkeypatch.setenv("BUNDLE_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("DATABASE_URL", FAKE_DB_URL)

    response = client.get(
        "/export/events_csv?run_id=not-an-int",
        headers={"Authorization": FAKE_BEARER, "X-Token": FAKE_SECRET},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid run_id"}
    _assert_secret_absent_from_response(response)


def test_admin_support_bundle_response_redacts_stdout_stderr_and_diagnostics(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    automation_dir = project_root / "automation"
    support_dir = project_root / "support_bundles"
    latest_bundle = support_dir / "bundle_failed"
    meta_dir = latest_bundle / "_meta"
    automation_dir.mkdir(parents=True)
    meta_dir.mkdir(parents=True)
    script = automation_dir / "collect_support_bundle.py"
    script.write_text("# dummy script for admin endpoint secret-leak smoke test\n", encoding="utf-8")
    (meta_dir / "states.log").write_text(f"STATE fatal BUNDLE_TOKEN={FAKE_SECRET}\n", encoding="utf-8")
    (meta_dir / "fatal_error.txt").write_text(f"fatal DATABASE_URL={FAKE_DB_URL}\n", encoding="utf-8")
    (meta_dir / "zip_error.txt").write_text(f"zip Authorization: {FAKE_BEARER}\n", encoding="utf-8")

    monkeypatch.setattr(admin_ops, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(admin_ops, "SUPPORT_BUNDLES_DIR", support_dir)
    monkeypatch.setenv("BUNDLE_TOKEN", FAKE_SECRET)
    monkeypatch.setenv("DATABASE_URL", FAKE_DB_URL)

    def fake_run(*args, **kwargs):
        return SimpleNamespace(
            returncode=2,
            stdout=f"collector stdout BUNDLE_TOKEN={FAKE_SECRET}",
            stderr=f"collector stderr DATABASE_URL={FAKE_DB_URL}",
        )

    monkeypatch.setattr(admin_ops.subprocess, "run", fake_run)

    response = admin_ops.create_support_bundle(request=_request(), _admin=None)
    body_text = response.body.decode("utf-8")
    body = json.loads(body_text)

    assert response.status_code == 500
    assert body["status"] == "error"
    assert FAKE_SECRET not in body_text
    assert FAKE_DB_URL not in body_text
    assert "<redacted>" in body_text
    assert body["stdout"] == "collector stdout BUNDLE_TOKEN=<redacted>"
    assert body["stderr"] == "collector stderr DATABASE_URL=<redacted>"
    assert "<redacted>" in body["diag"]["states"]
    assert "<redacted>" in body["diag"]["fatal_error"]
    assert "<redacted>" in body["diag"]["zip_error"]
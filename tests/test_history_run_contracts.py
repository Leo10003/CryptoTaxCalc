from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import cryptotaxcalc.app as app_module
import cryptotaxcalc.history_routes as history_routes
from cryptotaxcalc.app import app
from cryptotaxcalc.db import init_db
from cryptotaxcalc.models import Base, CalcRun, RealizedEvent, RunDigest

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _isolated_history_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    history_dir = tmp_path / "calc_runs"
    history_dir.mkdir(parents=True)
    monkeypatch.setattr(app_module, "CALC_HISTORY_DIR", history_dir)
    return history_dir


def _write_history_snapshot(
    history_dir: Path,
    *,
    run_id: str,
    created_at: str,
    events: list[dict] | None = None,
    manifest: dict | None = None,
) -> Path:
    payload = {
        "run_id": run_id,
        "created_at": created_at,
        "events": events if events is not None else [],
        "inputs_hash": f"input-{run_id}",
        "outputs_hash": f"output-{run_id}",
        "manifest": manifest or {"trust": {"fx_fallback_used": False}},
    }
    path = history_dir / f"{run_id}.json"
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    return path


def _isolated_history_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    db_path = tmp_path / "history_runs.sqlite"
    temp_engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
    init_db(temp_engine)
    Base.metadata.create_all(bind=temp_engine)
    TempSessionLocal = sessionmaker(bind=temp_engine, autoflush=False, autocommit=False)

    monkeypatch.setattr(app_module, "engine", temp_engine)
    monkeypatch.setattr(app_module, "SessionLocal", TempSessionLocal)
    monkeypatch.setattr(history_routes, "engine", temp_engine)
    monkeypatch.setattr(history_routes, "SessionLocal", TempSessionLocal)
    return temp_engine, TempSessionLocal


def _create_db_run(SessionLocal, *, external_run_id: str) -> int:
    with SessionLocal() as db:
        run = CalcRun(
            started_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            finished_at=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            jurisdiction="HR",
            rule_version="2025.1",
            tax_year=2025,
            lot_method="FIFO",
            fx_set_id=7,
            params_json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
            run_id=external_run_id,
            summary_json={"warnings": ["review me"], "fx_fallback_used": False},
        )
        db.add(run)
        db.flush()
        run_db_id = int(run.id)
        db.add(
            RealizedEvent(
                run_id=run_db_id,
                tx_id=None,
                timestamp="2025-02-01T00:00:00+00:00",
                asset="HISTBTC",
                qty_sold=Decimal("0.5"),
                proceeds=Decimal("600"),
                cost_basis=Decimal("250"),
                gain=Decimal("350"),
                quote_asset="EUR",
                fee_applied=Decimal("5"),
                matches_json='[{"lot":"a"}]',
            )
        )
        db.add(
            RunDigest(
                run_id=run_db_id,
                input_hash="i" * 64,
                output_hash="o" * 64,
                manifest_hash="m" * 64,
                manifest_json="{}",
                created_at="2025-01-01T00:00:02Z",
            )
        )
        db.commit()
    return run_db_id


def test_history_json_index_and_history_runs_contract_list_recent_snapshots_newest_first(monkeypatch, tmp_path):
    history_dir = _isolated_history_dir(monkeypatch, tmp_path)
    older_run_id = f"hist-old-{uuid.uuid4()}"
    newer_run_id = f"hist-new-{uuid.uuid4()}"
    _write_history_snapshot(
        history_dir,
        run_id=older_run_id,
        created_at="2025-01-01T00:00:00Z",
        events=[{"asset": "BTC"}],
    )
    _write_history_snapshot(
        history_dir,
        run_id=newer_run_id,
        created_at="2025-02-01T00:00:00Z",
        events=[{"asset": "ETH"}, {"asset": "ETH"}],
    )

    history_response = client.get("/history")
    runs_response = client.get("/history/runs")

    assert history_response.status_code == 200, history_response.text
    assert history_response.headers["content-type"].startswith("application/json")
    items = history_response.json()
    assert [item["run_id"] for item in items] == [newer_run_id, older_run_id]
    assert [item["events_count"] for item in items] == [2, 1]
    assert items[0]["inputs_hash"] == f"input-{newer_run_id}"
    assert items[0]["outputs_hash"] == f"output-{newer_run_id}"

    assert runs_response.status_code == 200, runs_response.text
    assert runs_response.json()["items"] == items


def test_history_html_contract_keeps_empty_state_and_saved_run_action_links(monkeypatch, tmp_path):
    history_dir = _isolated_history_dir(monkeypatch, tmp_path)

    empty = client.get("/history?format=html")
    assert empty.status_code == 200, empty.text[:500]
    assert empty.headers["content-type"].startswith("text/html")
    assert "Recent runs" in empty.text
    assert "No saved runs found yet" in empty.text

    run_id = f"hist-ui-{uuid.uuid4()}"
    _write_history_snapshot(
        history_dir,
        run_id=run_id,
        created_at="2025-03-01T00:00:00Z",
        events=[{"asset": "BTC"}],
        manifest={
            "trust": {
                "fx_fallback_used": True,
                "fx_fallback_days_count": 2,
                "fee_valuation": {
                    "third_asset_fee_detected": True,
                    "third_asset_fee_valued": False,
                    "missing_price_days_count": 1,
                },
            }
        },
    )

    html = client.get("/history?format=html").text

    assert "Recent runs" in html
    assert "Quick overview of your saved calculation snapshots" in html
    assert "Download bundle" in html
    assert "Events CSV" in html
    assert "View JSON" in html
    assert f'href="/history/run/{run_id}/download"' in html
    assert f'href="/history/run/{run_id}/events.csv"' in html
    assert f'href="/history/run/{run_id}"' in html
    assert "Review" in html
    assert "FX fallback" in html
    assert "Fees partial" in html


def test_history_run_get_and_delete_contracts_are_snapshot_scoped(monkeypatch, tmp_path):
    history_dir = _isolated_history_dir(monkeypatch, tmp_path)
    run_id = f"hist-delete-{uuid.uuid4()}"
    snapshot = _write_history_snapshot(
        history_dir,
        run_id=run_id,
        created_at="2025-04-01T00:00:00Z",
        events=[{"asset": "BTC", "gain": "10"}],
    )

    get_response = client.get(f"/history/run/{run_id}")
    assert get_response.status_code == 200, get_response.text
    assert get_response.json()["run_id"] == run_id
    assert get_response.json()["events"] == [{"asset": "BTC", "gain": "10"}]

    delete_response = client.delete(f"/history/run/{run_id}")
    assert delete_response.status_code == 200, delete_response.text
    assert delete_response.json() == {"status": "deleted", "run_id": run_id}
    assert not snapshot.exists()

    missing_get = client.get(f"/history/run/{run_id}")
    missing_delete = client.delete(f"/history/run/{run_id}")
    assert missing_get.status_code == 404
    assert missing_get.json() == {"detail": "Run not found"}
    assert missing_delete.status_code == 404
    assert missing_delete.json() == {"detail": "Run not found"}


def test_history_events_csv_resolves_external_and_numeric_run_ids_and_keeps_header_for_missing_runs(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_history_db(monkeypatch, tmp_path)
    external_run_id = f"hist-csv-{uuid.uuid4()}"
    try:
        run_db_id = _create_db_run(SessionLocal, external_run_id=external_run_id)

        by_external = client.get(f"/history/run/{external_run_id}/events.csv")
        by_numeric = client.get(f"/history/run/{run_db_id}/events.csv")
        missing = client.get("/history/run/not-a-real-run/events.csv")

        for response in (by_external, by_numeric, missing):
            assert response.status_code == 200, response.text
            assert response.headers["content-type"].startswith("text/csv")
            assert "timestamp,asset,qty_sold,proceeds_eur,cost_basis_eur,gain_eur" in response.text

        external_rows = list(csv.DictReader(io.StringIO(by_external.text)))
        numeric_rows = list(csv.DictReader(io.StringIO(by_numeric.text)))
        assert external_rows == numeric_rows
        assert len(external_rows) == 1
        assert external_rows[0]["asset"] == "HISTBTC"
        assert external_rows[0]["qty_sold"] == "0.5"
        assert external_rows[0]["proceeds_eur"] == "600"
        assert external_rows[0]["cost_basis_eur"] == "250"
        assert external_rows[0]["gain_eur"] == "350"
        assert external_rows[0]["quote_asset"] == "EUR"
        assert external_rows[0]["fee_applied_eur"] == "5"
        assert external_rows[0]["jurisdiction"] == "HR"
        assert external_rows[0]["tax_year"] == "2025"
        assert external_rows[0]["fx_set_id"] == "7"
        assert external_rows[0]["calc_run_id"] == str(run_db_id)
        assert external_rows[0]["run_ref"] == external_run_id
        assert list(csv.DictReader(io.StringIO(missing.text))) == []
    finally:
        engine.dispose()


def test_history_download_zip_contains_manifest_for_external_and_compat_routes(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_history_db(monkeypatch, tmp_path)
    external_run_id = f"hist-zip-{uuid.uuid4()}"
    try:
        run_db_id = _create_db_run(SessionLocal, external_run_id=external_run_id)

        primary = client.get(f"/history/{external_run_id}/download")
        compat = client.get(f"/history/run/{external_run_id}/download")
        debug = client.get(f"/history/{external_run_id}/download?debug=1")

        assert debug.status_code == 200, debug.text
        assert debug.json() == {"run_id": external_run_id, "id": run_db_id}

        for response in (primary, compat):
            assert response.status_code == 200, response.text[:500]
            assert response.headers["content-type"].startswith("application/zip")
            assert f"run_{external_run_id}.zip" in response.headers.get("content-disposition", "")
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                assert zf.namelist() == ["manifest.json"]
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            assert manifest["id"] == run_db_id
            assert manifest["run_id"] == external_run_id
            assert manifest["jurisdiction"] == "HR"
            assert manifest["tax_year"] == 2025
            assert manifest["fx_set_id"] == 7
            assert manifest["items_count"] == 1
            assert manifest["inputs_snapshot_count"] == 0
            assert manifest["outputs_hash"] == "o" * 64
    finally:
        engine.dispose()
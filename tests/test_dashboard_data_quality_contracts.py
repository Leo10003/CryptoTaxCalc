from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import cryptotaxcalc.app as app_module
from cryptotaxcalc.app import app
from cryptotaxcalc.db import init_db
from cryptotaxcalc.models import Base, CalcRun

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _isolated_dashboard_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    db_path = tmp_path / "dashboard_data_quality.sqlite"
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
    return temp_engine, TempSessionLocal


def _add_run(
    SessionLocal,
    *,
    run_id: str,
    summary_json: dict,
    tax_year: int = 2025,
    jurisdiction: str = "HR",
) -> int:
    with SessionLocal() as db:
        run = CalcRun(
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            finished_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
            jurisdiction=jurisdiction,
            rule_version="2025.1",
            tax_year=tax_year,
            lot_method="FIFO",
            params_json={"jurisdiction": jurisdiction, "tax_year": tax_year},
            run_id=run_id,
            summary_json=summary_json,
        )
        db.add(run)
        db.commit()
        return int(run.id)


def test_status_contract_reports_database_connection_demo_flag_and_latest_run_id(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_dashboard_db(monkeypatch, tmp_path)
    try:
        first_id = _add_run(
            SessionLocal,
            run_id=f"status-old-{uuid.uuid4()}",
            summary_json={"warnings": []},
        )
        latest_id = _add_run(
            SessionLocal,
            run_id=f"status-new-{uuid.uuid4()}",
            summary_json={"warnings": []},
        )

        response = client.get("/status")

        assert response.status_code == 200, response.text
        payload = response.json()
        assert set(payload) == {"database", "demo_mode", "last_run_id"}
        assert payload["database"] == "connected"
        assert isinstance(payload["demo_mode"], bool)
        assert payload["last_run_id"] == latest_id
        assert latest_id > first_id
    finally:
        engine.dispose()


def test_export_status_contract_surfaces_latest_blockers_with_user_actions(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_dashboard_db(monkeypatch, tmp_path)
    try:
        _add_run(SessionLocal, run_id=f"export-ok-{uuid.uuid4()}", summary_json={"warnings": []})
        _add_run(
            SessionLocal,
            run_id=f"export-blocked-{uuid.uuid4()}",
            summary_json={
                "warnings": [
                    {"severity": "blocker", "message": "Sold 1 BTC without acquisition history."},
                    "BLOCKER: Sold 2 ETH without acquisition history.",
                    {"severity": "info", "message": "Informational warning should not block."},
                    "normal warning should not block",
                ]
            },
        )

        response = client.get("/export/status")

        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["export_allowed"] is False
        assert payload["title"] == "We need more history before exporting"
        assert "incorrect tax calculations" in payload["message"]
        assert payload["recommended_actions"] == [
            "Upload older CSVs from the same exchange",
            "Upload deposit or transfer history",
            "Ensure your dataset starts before your first sale",
        ]
        assert payload["blockers"] == [
            "Sold 1 BTC without acquisition history.",
            "BLOCKER: Sold 2 ETH without acquisition history.",
        ]
    finally:
        engine.dispose()


def test_missing_history_contract_aggregates_latest_run_warning_events_by_asset(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_dashboard_db(monkeypatch, tmp_path)
    try:
        _add_run(
            SessionLocal,
            run_id=f"missing-old-{uuid.uuid4()}",
            summary_json={
                "warnings": [
                    {
                        "type": "missing_history",
                        "asset": "OLD",
                        "missing_qty": "99",
                        "timestamp": "2024-01-01T00:00:00+00:00",
                    }
                ]
            },
        )
        _add_run(
            SessionLocal,
            run_id=f"missing-latest-{uuid.uuid4()}",
            summary_json={
                "warnings": [
                    {
                        "type": "missing_history",
                        "asset": "BTC",
                        "missing_qty": "0.25",
                        "timestamp": "2025-02-01T00:00:00+00:00",
                    },
                    {
                        "type": "missing_history",
                        "asset": "BTC",
                        "missing_qty": "0.75",
                        "timestamp": "2025-01-15T00:00:00+00:00",
                    },
                    {
                        "type": "missing_history",
                        "asset": "ETH",
                        "missing_qty": "2",
                        "timestamp": "2025-03-01T00:00:00+00:00",
                    },
                    {"type": "price_warning", "asset": "BNB", "missing_qty": "100"},
                    "string warning ignored by missing-history endpoint",
                ]
            },
        )

        response = client.get("/data_quality/missing_history")

        assert response.status_code == 200, response.text
        assets = {item["asset"]: item for item in response.json()["assets"]}
        assert set(assets) == {"BTC", "ETH"}
        assert assets["BTC"] == {
            "asset": "BTC",
            "missing_qty_total": 1.0,
            "first_seen_ts": "2025-01-15T00:00:00+00:00",
            "events": 2,
        }
        assert assets["ETH"] == {
            "asset": "ETH",
            "missing_qty_total": 2.0,
            "first_seen_ts": "2025-03-01T00:00:00+00:00",
            "events": 1,
        }
    finally:
        engine.dispose()


def test_precheck_contract_reports_global_and_per_file_missing_history_guidance_without_persistence():
    response = client.post(
        "/data_quality/precheck",
        files=[
            (
                "files",
                (
                    "btc_disposal_only.csv",
                    (
                        b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount\n"
                        b"2025-01-10T00:00:00Z,sell,BTC,1,EUR,50000\n"
                    ),
                    "text/csv",
                ),
            ),
            (
                "files",
                (
                    "eth_balanced.csv",
                    (
                        b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount\n"
                        b"2025-01-01T00:00:00Z,buy,ETH,2,EUR,2000\n"
                        b"2025-01-10T00:00:00Z,sell,ETH,1,EUR,1200\n"
                    ),
                    "text/csv",
                ),
            ),
        ],
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["issues_detected"] is True
    assert len(payload["assets"]) == 1
    btc = payload["assets"][0]
    assert btc["asset"] == "BTC"
    assert btc["first_sell_ts"] == "2025-01-10T00:00:00"
    assert btc["total_sell_qty"] == "1"
    assert "missing at least 1 BTC acquisition history" in btc["reason"]
    assert "deposit/withdrawal/transfer history" in btc["guidance"]

    files = {item["filename"]: item for item in payload["files"]}
    assert files["btc_disposal_only.csv"]["issues_detected"] is True
    assert files["btc_disposal_only.csv"]["assets"][0]["asset"] == "BTC"
    assert files["eth_balanced.csv"] == {
        "filename": "eth_balanced.csv",
        "issues_detected": False,
        "assets": [],
    }


def test_demo_recent_runs_contract_exposes_latest_run_totals_and_warning_count(monkeypatch, tmp_path):
    engine, SessionLocal = _isolated_dashboard_db(monkeypatch, tmp_path)
    try:
        old_id = _add_run(
            SessionLocal,
            run_id=f"demo-old-{uuid.uuid4()}",
            summary_json={"totals": {"gain": "1"}, "warnings": []},
            tax_year=2024,
        )
        latest_external_run_id = f"demo-latest-{uuid.uuid4()}"
        latest_id = _add_run(
            SessionLocal,
            run_id=latest_external_run_id,
            tax_year=2025,
            jurisdiction="IT",
            summary_json={
                "totals": {
                    "gain_eur": "123.45",
                    "taxable_gain_eur": "100.00",
                    "exempt_gain_eur": "23.45",
                },
                "warnings": [
                    "missing price",
                    {"severity": "blocker", "message": "missing history"},
                ],
            },
        )
        monkeypatch.setattr(app_module, "_demo_allowed_here", lambda: True)

        response = client.get("/demo/runs/recent?limit=1")

        assert response.status_code == 200, response.text
        payload = response.json()
        assert len(payload["runs"]) == 1
        run = payload["runs"][0]
        assert run["id"] == latest_id
        assert run["id"] != old_id
        assert run["run_id"] == latest_external_run_id
        assert run["jurisdiction"] == "IT"
        assert run["tax_year"] == 2025
        assert run["gain_eur"] == "123.45"
        assert run["taxable_gain_eur"] == "100.00"
        assert run["exempt_gain_eur"] == "23.45"
        assert run["warning_count"] == 2
        assert run["finished_at"] is not None
    finally:
        engine.dispose()
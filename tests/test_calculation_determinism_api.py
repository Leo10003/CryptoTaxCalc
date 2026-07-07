from __future__ import annotations

import json
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import cryptotaxcalc.app as app_module
import cryptotaxcalc.audit_digest as audit_digest
import cryptotaxcalc.audit_utils as audit_utils
import cryptotaxcalc.fx_utils as fx_utils
from cryptotaxcalc.app import app
from cryptotaxcalc.db import init_db
from cryptotaxcalc.models import Base

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _isolated_api_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    db_path = tmp_path / "calculation_determinism.sqlite"
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
    monkeypatch.setattr(audit_utils, "engine", temp_engine)
    monkeypatch.setattr(audit_digest, "engine", temp_engine)
    monkeypatch.setattr(fx_utils, "SessionLocal", TempSessionLocal)
    monkeypatch.setitem(app_module.audit.__globals__, "engine", temp_engine)
    return temp_engine, TempSessionLocal


def _csv(*, memo_tag: str, asset: str) -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},2,EUR,1000,EUR,0,Determinism,{memo_tag} buy lot
2025-02-01T00:00:00Z,sell,{asset},0.75,EUR,600,EUR,5,Determinism,{memo_tag} sell
"""


def _post_import(filename: str, csv_text: str):
    return client.post(
        "/import/multiple",
        params={"reset": "false"},
        files=[("files", (filename, csv_text.encode("utf-8"), "text/csv"))],
    )


def _calculate():
    response = client.post(
        "/calculate/v2",
        json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _events(SessionLocal, run_id: int) -> list[dict]:
    with SessionLocal() as db:
        return [
            dict(row)
            for row in db.execute(
                text(
                    """
                    SELECT timestamp, asset, qty_sold, proceeds, cost_basis, gain, quote_asset, fee_applied, matches_json
                    FROM realized_events
                    WHERE run_id = :run_id
                    ORDER BY id
                    """
                ),
                {"run_id": run_id},
            ).mappings().all()
        ]


def _digest(SessionLocal, run_id: int) -> dict:
    with SessionLocal() as db:
        row = db.execute(
            text(
                """
                SELECT input_hash, output_hash, manifest_hash, manifest_json
                FROM run_digests
                WHERE run_id = :run_id
                """
            ),
            {"run_id": run_id},
        ).mappings().one()
        return dict(row)


def _summary_business_payload(payload: dict) -> dict:
    summary = dict(payload["summary"])
    summary.pop("run_id", None)
    summary.pop("timings_ms", None)
    summary.pop("fx_context", None)
    return summary


def test_repeated_calculate_v2_runs_are_business_deterministic_with_stable_digests(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    memo_tag = f"calc-det-{uuid.uuid4().hex}"
    asset = f"DET{uuid.uuid4().hex[:10]}".upper()

    import_response = _post_import("deterministic.csv", _csv(memo_tag=memo_tag, asset=asset))
    assert import_response.status_code == 200, import_response.text
    assert import_response.json()["results"][0]["inserted"] == 2

    first = _calculate()
    second = _calculate()

    assert first["run_id"] != second["run_id"]
    assert _summary_business_payload(first) == _summary_business_payload(second)
    assert first["digests"] == second["digests"]

    first_events = _events(SessionLocal, first["run_id"])
    second_events = _events(SessionLocal, second["run_id"])
    assert len(first_events) == len(second_events) == 1
    assert first_events == second_events
    assert Decimal(str(first_events[0]["proceeds"])) == Decimal("595")
    assert Decimal(str(first_events[0]["cost_basis"])) == Decimal("375.00")
    assert Decimal(str(first_events[0]["gain"])) == Decimal("220.00")

    first_digest = _digest(SessionLocal, first["run_id"])
    second_digest = _digest(SessionLocal, second["run_id"])
    assert first_digest["input_hash"] == second_digest["input_hash"]
    assert first_digest["output_hash"] == second_digest["output_hash"]
    assert first_digest["manifest_hash"] == second_digest["manifest_hash"]

    first_manifest = json.loads(first_digest["manifest_json"])
    second_manifest = json.loads(second_digest["manifest_json"])
    assert first_manifest["run_id"] != second_manifest["run_id"]
    assert first_manifest["outputs"] == second_manifest["outputs"]
    assert first_manifest["inputs"] == second_manifest["inputs"]


def test_changed_input_changes_calculate_v2_input_output_and_manifest_digests(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    asset = f"DCH{uuid.uuid4().hex[:10]}".upper()

    first_import = _post_import("first.csv", _csv(memo_tag=f"calc-det-a-{uuid.uuid4().hex}", asset=asset))
    assert first_import.status_code == 200, first_import.text
    first = _calculate()

    second_import = _post_import(
        "second.csv",
        f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-03-01T00:00:00Z,buy,{asset},1,EUR,300,EUR,0,Determinism,calc-det-extra-{uuid.uuid4().hex} extra buy
2025-04-01T00:00:00Z,sell,{asset},0.5,EUR,500,EUR,0,Determinism,calc-det-extra-{uuid.uuid4().hex} extra sell
""",
    )
    assert second_import.status_code == 200, second_import.text
    second = _calculate()

    assert first["digests"]["input_hash"] != second["digests"]["input_hash"]
    assert first["digests"]["output_hash"] != second["digests"]["output_hash"]
    assert first["digests"]["manifest_hash"] != second["digests"]["manifest_hash"]

    assert len(_events(SessionLocal, first["run_id"])) == 1
    assert len(_events(SessionLocal, second["run_id"])) == 2


def test_changed_calculation_config_changes_input_output_and_manifest_hash(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    memo_tag = f"calc-det-cfg-{uuid.uuid4().hex}"
    asset = f"DCFG{uuid.uuid4().hex[:10]}".upper()

    import_response = _post_import("config.csv", _csv(memo_tag=memo_tag, asset=asset))
    assert import_response.status_code == 200, import_response.text

    hr = client.post("/calculate/v2", json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False})
    xx = client.post("/calculate/v2", json={"jurisdiction": "XX", "tax_year": 2025, "strict_fx": False})
    assert hr.status_code == 200, hr.text
    assert xx.status_code == 200, xx.text
    hr_payload = hr.json()
    xx_payload = xx.json()

    assert hr_payload["digests"]["input_hash"] != xx_payload["digests"]["input_hash"]
    assert hr_payload["digests"]["output_hash"] != xx_payload["digests"]["output_hash"]
    assert hr_payload["digests"]["manifest_hash"] != xx_payload["digests"]["manifest_hash"]
    assert len(_events(SessionLocal, hr_payload["run_id"])) == len(_events(SessionLocal, xx_payload["run_id"])) == 1
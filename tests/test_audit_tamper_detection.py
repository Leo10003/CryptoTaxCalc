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
    db_path = tmp_path / "audit_tamper_detection.sqlite"
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
2025-01-01T00:00:00Z,buy,{asset},2,EUR,1000,EUR,0,Tamper,{memo_tag} buy
2025-02-01T00:00:00Z,sell,{asset},0.75,EUR,600,EUR,5,Tamper,{memo_tag} sell
"""


def _post_import(filename: str, csv_text: str):
    return client.post(
        "/import/multiple",
        params={"reset": "false"},
        files=[("files", (filename, csv_text.encode("utf-8"), "text/csv"))],
    )


def _calculate() -> dict:
    response = client.post(
        "/calculate/v2",
        json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _create_verified_run(SessionLocal) -> int:
    memo_tag = f"audit-tamper-{uuid.uuid4().hex}"
    asset = f"TAMP{uuid.uuid4().hex[:10]}".upper()

    import_response = _post_import("audit_tamper.csv", _csv(memo_tag=memo_tag, asset=asset))
    assert import_response.status_code == 200, import_response.text
    assert import_response.json()["results"][0]["inserted"] == 2

    payload = _calculate()
    run_id = payload["run_id"]

    verify = client.get(f"/audit/verify/{run_id}")
    assert verify.status_code == 200, verify.text
    verify_payload = verify.json()
    assert verify_payload["verified"] is True
    assert verify_payload["stored_manifest_json_valid"] is True

    audit_run = client.get(f"/audit/run/{run_id}")
    assert audit_run.status_code == 200, audit_run.text
    assert audit_run.json()["matches"] is True

    with SessionLocal() as db:
        event_count = int(
            db.execute(
                text("SELECT COUNT(*) FROM realized_events WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).scalar()
            or 0
        )
        digest_count = int(
            db.execute(
                text("SELECT COUNT(*) FROM run_digests WHERE run_id = :run_id"),
                {"run_id": run_id},
            ).scalar()
            or 0
        )

    assert event_count == 1
    assert digest_count == 1
    return run_id


def _verify_payload(run_id: int) -> dict:
    response = client.get(f"/audit/verify/{run_id}")
    assert response.status_code == 200, response.text
    return response.json()


def _audit_run_payload(run_id: int) -> dict:
    response = client.get(f"/audit/run/{run_id}")
    assert response.status_code == 200, response.text
    return response.json()


def test_audit_verify_detects_realized_event_tampering(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    run_id = _create_verified_run(SessionLocal)

    with SessionLocal() as db:
        db.execute(
            text(
                """
                UPDATE realized_events
                SET gain = :gain, proceeds = :proceeds
                WHERE run_id = :run_id
                """
            ),
            {"gain": "999999.99", "proceeds": "1000000.00", "run_id": run_id},
        )
        db.commit()

    payload = _verify_payload(run_id)

    assert payload["verified"] is False
    assert payload["stored"]["input_hash"] == payload["recomputed"]["input_hash"]
    assert payload["stored"]["output_hash"] != payload["recomputed"]["output_hash"]
    assert payload["stored"]["manifest_hash"] != payload["recomputed"]["manifest_hash"]
    assert payload["stored_manifest_json_valid"] is True

    audit_payload = _audit_run_payload(run_id)
    assert audit_payload["matches"] is False


def test_audit_verify_detects_run_input_snapshot_tampering(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    run_id = _create_verified_run(SessionLocal)

    with SessionLocal() as db:
        db.execute(
            text(
                """
                INSERT INTO run_inputs (run_id, tx_hash)
                VALUES (:run_id, :tx_hash)
                """
            ),
            {"run_id": run_id, "tx_hash": f"tampered-input-{uuid.uuid4().hex}"},
        )
        db.commit()

    payload = _verify_payload(run_id)

    assert payload["verified"] is False
    assert payload["stored"]["input_hash"] != payload["recomputed"]["input_hash"]
    assert payload["stored"]["output_hash"] == payload["recomputed"]["output_hash"]
    assert payload["stored"]["manifest_hash"] != payload["recomputed"]["manifest_hash"]
    assert payload["stored_manifest_json_valid"] is True

    audit_payload = _audit_run_payload(run_id)
    assert audit_payload["matches"] is False


def test_audit_verify_detects_stored_manifest_json_tampering(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    run_id = _create_verified_run(SessionLocal)

    with SessionLocal() as db:
        manifest_json = db.execute(
            text("SELECT manifest_json FROM run_digests WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).scalar_one()
        manifest = json.loads(manifest_json)
        manifest["outputs"][0]["gain"] = "999999.99"
        db.execute(
            text(
                """
                UPDATE run_digests
                SET manifest_json = :manifest_json
                WHERE run_id = :run_id
                """
            ),
            {"manifest_json": json.dumps(manifest, separators=(",", ":")), "run_id": run_id},
        )
        db.commit()

    payload = _verify_payload(run_id)

    assert payload["verified"] is False
    assert payload["stored"]["input_hash"] == payload["recomputed"]["input_hash"]
    assert payload["stored"]["output_hash"] == payload["recomputed"]["output_hash"]
    assert payload["stored"]["manifest_hash"] == payload["recomputed"]["manifest_hash"]
    assert payload["stored_manifest_json_valid"] is False
    assert payload["stored_manifest_json_digests"]["output_hash"] != payload["stored"]["output_hash"]

    audit_payload = _audit_run_payload(run_id)
    assert audit_payload["matches"] is False
    assert audit_payload["stored_manifest_json_valid"] is False


def test_audit_verify_detects_corrupt_stored_manifest_json(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    run_id = _create_verified_run(SessionLocal)

    with SessionLocal() as db:
        db.execute(
            text(
                """
                UPDATE run_digests
                SET manifest_json = :manifest_json
                WHERE run_id = :run_id
                """
            ),
            {"manifest_json": "{not valid json", "run_id": run_id},
        )
        db.commit()

    payload = _verify_payload(run_id)

    assert payload["verified"] is False
    assert payload["stored_manifest_json_valid"] is False
    assert isinstance(payload["stored_manifest_json_error"], str)
    assert payload["stored_manifest_json_error"]

    audit_payload = _audit_run_payload(run_id)
    assert audit_payload["matches"] is False
    assert audit_payload["stored_manifest_json_valid"] is False


def test_audit_verify_detects_stored_digest_hash_tampering(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_api_db(monkeypatch, tmp_path)
    run_id = _create_verified_run(SessionLocal)

    with SessionLocal() as db:
        db.execute(
            text(
                """
                UPDATE run_digests
                SET output_hash = :output_hash
                WHERE run_id = :run_id
                """
            ),
            {"output_hash": "0" * 64, "run_id": run_id},
        )
        db.commit()

    payload = _verify_payload(run_id)

    assert payload["verified"] is False
    assert payload["stored"]["input_hash"] == payload["recomputed"]["input_hash"]
    assert payload["stored"]["output_hash"] != payload["recomputed"]["output_hash"]
    assert payload["stored"]["manifest_hash"] == payload["recomputed"]["manifest_hash"]
    assert payload["stored_manifest_json_valid"] is False

    audit_payload = _audit_run_payload(run_id)
    assert audit_payload["matches"] is False
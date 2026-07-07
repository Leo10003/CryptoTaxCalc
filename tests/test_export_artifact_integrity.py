from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from cryptotaxcalc.app import app
from cryptotaxcalc.db import SessionLocal, engine, init_db
from cryptotaxcalc.models import Base

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _ensure_schema() -> None:
    init_db(engine)
    Base.metadata.create_all(bind=engine)


def _csv(*, memo_tag: str, asset: str) -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},2,EUR,1000,EUR,0,ExportIntegrity,{memo_tag} buy
2025-02-01T00:00:00Z,sell,{asset},0.75,EUR,600,EUR,5,ExportIntegrity,{memo_tag} sell
"""


def _post_import_multiple(filename: str, csv_text: str, *, reset: bool = False):
    return client.post(
        "/import/multiple",
        params={"reset": "true" if reset else "false"},
        files=[("files", (filename, csv_text.encode("utf-8"), "text/csv"))],
    )


def _delete_transactions_by_memo_fragment(fragment: str) -> int:
    with SessionLocal() as db:
        result = db.execute(
            text("""
                DELETE FROM transactions
                WHERE memo LIKE :memo_fragment
            """),
            {"memo_fragment": f"%{fragment}%"},
        )
        db.commit()
        return int(result.rowcount or 0)


def _create_calculated_run() -> tuple[int, str]:
    _ensure_schema()
    memo_tag = f"export-integrity-{uuid.uuid4().hex}"
    asset = f"E{uuid.uuid4().hex[:10]}".upper()
    filename = f"export_integrity_{uuid.uuid4().hex}.csv"

    response = _post_import_multiple(filename, _csv(memo_tag=memo_tag, asset=asset), reset=False)
    assert response.status_code == 200, response.text

    calc_response = client.post(
        "/calculate/v2",
        json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
    )
    assert calc_response.status_code == 200, calc_response.text
    payload = calc_response.json()
    run_id = payload["run_id"]
    assert isinstance(run_id, int) and run_id > 0
    return run_id, memo_tag


def _read_csv_response(text_value: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text_value)))


def _event_for_asset(rows: list[dict[str, str]], asset: str) -> dict[str, str]:
    matches = [row for row in rows if row.get("asset") == asset]
    assert len(matches) == 1, rows
    return matches[0]


def _db_event(run_id: int) -> dict:
    with SessionLocal() as db:
        row = db.execute(
            text("""
                SELECT timestamp, asset, qty_sold, proceeds, cost_basis, gain, quote_asset, fee_applied, matches_json
                FROM realized_events
                WHERE run_id = :run_id
                ORDER BY id DESC
                LIMIT 1
            """),
            {"run_id": run_id},
        ).mappings().first()
        assert row is not None
        return dict(row)


def _db_digest(run_id: int) -> dict:
    with SessionLocal() as db:
        row = db.execute(
            text("""
                SELECT input_hash, output_hash, manifest_hash, manifest_json
                FROM run_digests
                WHERE run_id = :run_id
            """),
            {"run_id": run_id},
        ).mappings().first()
        assert row is not None
        return dict(row)


def test_history_events_csv_export_contains_realized_event_and_audit_columns():
    run_id, memo_tag = _create_calculated_run()
    try:
        expected = _db_event(run_id)

        response = client.get(f"/history/run/{run_id}/events.csv")

        assert response.status_code == 200, response.text
        assert "text/csv" in response.headers.get("content-type", "").lower()
        assert f"realized_events_run_{run_id}.csv" in response.headers.get("content-disposition", "")

        rows = _read_csv_response(response.text)
        event = _event_for_asset(rows, expected["asset"])
        assert event["timestamp"] == expected["timestamp"]
        assert Decimal(event["qty_sold"]) == Decimal(str(expected["qty_sold"]))
        assert Decimal(event["proceeds_eur"]) == Decimal(str(expected["proceeds"]))
        assert Decimal(event["cost_basis_eur"]) == Decimal(str(expected["cost_basis"]))
        assert Decimal(event["gain_eur"]) == Decimal(str(expected["gain"]))
        assert event["quote_asset"] == expected["quote_asset"]
        assert Decimal(event["fee_applied_eur"]) == Decimal(str(expected["fee_applied"]))
        assert json.loads(event["matches_json"]) == json.loads(expected["matches_json"])
        assert event["jurisdiction"] == "HR"
        assert event["tax_year"] == "2025"
        assert event["calc_run_id"] == str(run_id)
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_export_events_csv_endpoint_matches_history_csv_for_same_run():
    run_id, memo_tag = _create_calculated_run()
    try:
        history_response = client.get(f"/history/run/{run_id}/events.csv")
        export_response = client.get(f"/export/events_csv?run_id={run_id}")

        assert history_response.status_code == 200, history_response.text
        assert export_response.status_code == 200, export_response.text
        assert "text/csv" in export_response.headers.get("content-type", "").lower()
        assert f"realized_events_run_{run_id}.csv" in export_response.headers.get("content-disposition", "")
        assert _read_csv_response(export_response.text) == _read_csv_response(history_response.text)
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_history_zip_download_contains_manifest_for_the_requested_run():
    run_id, memo_tag = _create_calculated_run()
    try:
        response = client.get(f"/history/{run_id}/download")

        assert response.status_code == 200, response.text
        assert response.headers.get("content-type", "").lower().startswith("application/zip")

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            assert zf.namelist() == ["manifest.json"]
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))

        assert manifest["id"] == run_id
        assert manifest["run_id"] == str(run_id)
        assert manifest["jurisdiction"] == "HR"
        assert manifest["items_count"] >= 1
        assert manifest["inputs_snapshot_count"] >= 2
        assert isinstance(manifest["outputs_hash"], str) and len(manifest["outputs_hash"]) == 64
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_audit_verify_and_audit_run_exports_match_stored_digest_record():
    run_id, memo_tag = _create_calculated_run()
    try:
        digest = _db_digest(run_id)

        verify_response = client.get(f"/audit/verify/{run_id}")
        assert verify_response.status_code == 200, verify_response.text
        verify_payload = verify_response.json()
        assert verify_payload["verified"] is True
        assert verify_payload["stored"] == {
            "input_hash": digest["input_hash"],
            "output_hash": digest["output_hash"],
            "manifest_hash": digest["manifest_hash"],
        }
        assert verify_payload["recomputed"] == verify_payload["stored"]

        audit_response = client.get(f"/audit/run/{run_id}")
        assert audit_response.status_code == 200, audit_response.text
        audit_payload = audit_response.json()
        assert audit_payload["matches"] is True
        assert audit_payload["stored"]["input_hash"] == digest["input_hash"]
        assert audit_payload["stored"]["output_hash"] == digest["output_hash"]
        assert audit_payload["stored"]["manifest_hash"] == digest["manifest_hash"]
        assert audit_payload["manifest"]["run_id"] == run_id
        assert audit_payload["manifest"]["outputs"]
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)
from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from cryptotaxcalc.app import app
from cryptotaxcalc.audit_digest import build_run_manifest, compute_digests
from cryptotaxcalc.db import SessionLocal, engine, init_db
from cryptotaxcalc.models import Base, CalcRun, RealizedEvent, RunDigest, RunInput

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _ensure_schema() -> None:
    init_db(engine)
    Base.metadata.create_all(bind=engine)


def _create_synthetic_run(*, asset: str, proceeds: str, cost_basis: str, gain: str, seconds_offset: int) -> int:
    _ensure_schema()
    external_run_id = f"isolation-{asset.lower()}-{uuid.uuid4()}"
    started_at = datetime(2026, 1, 1, 12, 0, seconds_offset, tzinfo=timezone.utc)
    finished_at = started_at + timedelta(seconds=1)

    with SessionLocal() as db:
        run = CalcRun(
            started_at=started_at,
            finished_at=finished_at,
            jurisdiction="HR",
            rule_version="2025.1",
            tax_year=2025,
            lot_method="FIFO",
            fx_set_id=None,
            params_json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
            run_id=external_run_id,
            summary_json={
                "totals": {"proceeds": proceeds, "cost_basis": cost_basis, "gain": gain},
                "warnings": [],
            },
        )
        db.add(run)
        db.flush()
        run_id = int(run.id)

        db.add(
            RunInput(
                run_id=run_id,
                tx_hash=f"txhash-{asset.lower()}-{uuid.uuid4().hex}",
            )
        )
        db.add(
            RealizedEvent(
                run_id=run_id,
                tx_id=None,
                timestamp=f"2025-06-{seconds_offset + 1:02d}T00:00:00+00:00",
                asset=asset,
                qty_sold=Decimal("1"),
                proceeds=Decimal(proceeds),
                cost_basis=Decimal(cost_basis),
                gain=Decimal(gain),
                quote_asset="EUR",
                fee_applied=Decimal("0"),
                matches_json=json.dumps(
                    [
                        {
                            "from_qty": "1",
                            "lot_cost_per_unit": cost_basis,
                            "lot_cost_total": cost_basis,
                            "proceeds_eur": proceeds,
                            "cost_eur": cost_basis,
                        }
                    ]
                ),
            )
        )
        db.commit()

    manifest = build_run_manifest(run_id)
    digests = compute_digests(manifest)

    with SessionLocal() as db:
        db.add(
            RunDigest(
                run_id=run_id,
                input_hash=digests["input_hash"],
                output_hash=digests["output_hash"],
                manifest_hash=digests["manifest_hash"],
                manifest_json=json.dumps(manifest, separators=(",", ":")),
                created_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            )
        )
        db.commit()

    return run_id


def _delete_run(run_id: int) -> None:
    with SessionLocal() as db:
        for table in ("realized_events", "run_digests", "run_inputs", "calc_runs"):
            db.execute(
                text(
                    f"DELETE FROM {table} WHERE run_id = :run_id"
                    if table != "calc_runs"
                    else "DELETE FROM calc_runs WHERE id = :run_id"
                ),
                {"run_id": run_id},
            )
        db.commit()


def _csv_rows(text_value: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text_value)))


def _asset_rows(rows: list[dict[str, str]], asset: str) -> list[dict[str, str]]:
    return [row for row in rows if row.get("asset") == asset]


def test_events_csv_uses_requested_run_id_not_latest_run():
    old_run_id = _create_synthetic_run(
        asset=f"OLD{uuid.uuid4().hex[:8].upper()}",
        proceeds="100",
        cost_basis="40",
        gain="60",
        seconds_offset=1,
    )
    latest_run_id = _create_synthetic_run(
        asset=f"NEW{uuid.uuid4().hex[:8].upper()}",
        proceeds="200",
        cost_basis="75",
        gain="125",
        seconds_offset=2,
    )
    try:
        old_asset = _csv_rows(client.get(f"/history/run/{old_run_id}/events.csv").text)[0]["asset"]
        latest_asset = _csv_rows(client.get(f"/history/run/{latest_run_id}/events.csv").text)[0]["asset"]

        response = client.get(f"/export/events_csv?run_id={old_run_id}")

        assert response.status_code == 200, response.text
        rows = _csv_rows(response.text)
        assert len(_asset_rows(rows, old_asset)) == 1
        assert _asset_rows(rows, latest_asset) == []
        assert {row["calc_run_id"] for row in rows} == {str(old_run_id)}
    finally:
        _delete_run(old_run_id)
        _delete_run(latest_run_id)


def test_latest_events_csv_resolves_to_latest_run_only():
    old_run_id = _create_synthetic_run(
        asset=f"LDO{uuid.uuid4().hex[:8].upper()}",
        proceeds="111",
        cost_basis="50",
        gain="61",
        seconds_offset=3,
    )
    latest_asset = f"LNW{uuid.uuid4().hex[:8].upper()}"
    latest_run_id = _create_synthetic_run(
        asset=latest_asset,
        proceeds="222",
        cost_basis="80",
        gain="142",
        seconds_offset=4,
    )
    try:
        response = client.get("/export/events_csv?run_id=latest")

        assert response.status_code == 200, response.text
        rows = _csv_rows(response.text)
        assert len(rows) == 1
        assert rows[0]["asset"] == latest_asset
        assert rows[0]["calc_run_id"] == str(latest_run_id)
        assert rows[0]["calc_run_id"] != str(old_run_id)
    finally:
        _delete_run(old_run_id)
        _delete_run(latest_run_id)


def test_preview_data_uses_requested_run_id_not_latest_run():
    old_asset = f"PDO{uuid.uuid4().hex[:8].upper()}"
    latest_asset = f"PNW{uuid.uuid4().hex[:8].upper()}"
    old_run_id = _create_synthetic_run(
        asset=old_asset,
        proceeds="300",
        cost_basis="100",
        gain="200",
        seconds_offset=5,
    )
    latest_run_id = _create_synthetic_run(
        asset=latest_asset,
        proceeds="400",
        cost_basis="125",
        gain="275",
        seconds_offset=6,
    )
    try:
        response = client.get(f"/export/events_csv/preview_data?run_id={old_run_id}&offset=0&limit=50")

        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["run_id"] == old_run_id
        assert payload["total"] == 1
        assert len(payload["items"]) == 1
        assert payload["items"][0]["asset"] == old_asset
        assert payload["items"][0]["asset"] != latest_asset
    finally:
        _delete_run(old_run_id)
        _delete_run(latest_run_id)


def test_audit_and_zip_endpoints_use_requested_run_id_not_latest_run():
    old_run_id = _create_synthetic_run(
        asset=f"ADO{uuid.uuid4().hex[:8].upper()}",
        proceeds="500",
        cost_basis="300",
        gain="200",
        seconds_offset=7,
    )
    latest_run_id = _create_synthetic_run(
        asset=f"ANW{uuid.uuid4().hex[:8].upper()}",
        proceeds="600",
        cost_basis="350",
        gain="250",
        seconds_offset=8,
    )
    try:
        audit_response = client.get(f"/audit/run/{old_run_id}")
        verify_response = client.get(f"/audit/verify/{old_run_id}")
        zip_response = client.get(f"/history/{old_run_id}/download")

        assert audit_response.status_code == 200, audit_response.text
        audit_payload = audit_response.json()
        assert audit_payload["run_id"] == old_run_id
        assert audit_payload["manifest"]["run_id"] == old_run_id
        assert audit_payload["manifest"]["run_id"] != latest_run_id
        assert audit_payload["matches"] is True

        assert verify_response.status_code == 200, verify_response.text
        assert verify_response.json()["run_id"] == old_run_id
        assert verify_response.json()["verified"] is True

        assert zip_response.status_code == 200, zip_response.text
        with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zf:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        assert manifest["id"] == old_run_id
        assert manifest["id"] != latest_run_id
    finally:
        _delete_run(old_run_id)
        _delete_run(latest_run_id)
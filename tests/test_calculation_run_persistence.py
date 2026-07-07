from __future__ import annotations

import json
import uuid
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
2025-01-01T00:00:00Z,buy,{asset},1,EUR,1000,EUR,0,Persistence,{memo_tag} buy
2025-02-01T00:00:00Z,sell,{asset},0.25,EUR,400,EUR,0,Persistence,{memo_tag} sell
"""


def _unsupported_quote_csv(*, memo_tag: str, asset: str) -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},1,BTC,0.05,,,Persistence,{memo_tag} crypto quote buy
2025-02-01T00:00:00Z,sell,{asset},1,BTC,0.07,,,Persistence,{memo_tag} crypto quote sell
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


def _count_transactions_by_memo_fragment(fragment: str) -> int:
    with SessionLocal() as db:
        return int(
            db.execute(
                text("""
                    SELECT COUNT(*)
                    FROM transactions
                    WHERE memo LIKE :memo_fragment
                """),
                {"memo_fragment": f"%{fragment}%"},
            ).scalar()
            or 0
        )


def _latest_calc_run_id() -> int:
    with SessionLocal() as db:
        return int(db.execute(text("SELECT COALESCE(MAX(id), 0) FROM calc_runs")).scalar() or 0)


def _row_mapping(sql: str, params: dict):
    with SessionLocal() as db:
        row = db.execute(text(sql), params).mappings().first()
        return dict(row) if row is not None else None


def _rows(sql: str, params: dict) -> list[dict]:
    with SessionLocal() as db:
        return [dict(row) for row in db.execute(text(sql), params).mappings().all()]


def _as_decimal(value) -> Decimal:
    return Decimal(str(value))


def test_calculate_v2_success_persists_calc_run_events_summary_and_digest():
    _ensure_schema()
    memo_tag = f"persist-success-{uuid.uuid4().hex}"
    asset = f"P{uuid.uuid4().hex[:10]}".upper()
    filename = f"persist_success_{uuid.uuid4().hex}.csv"

    try:
        response = _post_import_multiple(filename, _csv(memo_tag=memo_tag, asset=asset), reset=False)
        assert response.status_code == 200, response.text
        assert _count_transactions_by_memo_fragment(memo_tag) == 2

        calc_response = client.post(
            "/calculate/v2",
            json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
        )
        assert calc_response.status_code == 200, calc_response.text
        payload = calc_response.json()
        run_id = payload["run_id"]
        digests = payload["digests"]

        assert isinstance(run_id, int) and run_id > 0
        assert set(digests) == {"input_hash", "output_hash", "manifest_hash"}
        assert all(isinstance(value, str) and len(value) == 64 for value in digests.values())

        calc_run = _row_mapping(
            """
            SELECT id, finished_at, jurisdiction, tax_year, lot_method, fx_set_id, params_json, summary_json
            FROM calc_runs
            WHERE id = :run_id
            """,
            {"run_id": run_id},
        )
        assert calc_run is not None
        assert calc_run["finished_at"] is not None
        assert calc_run["jurisdiction"] == "HR"
        assert calc_run["tax_year"] == 2025
        assert calc_run["lot_method"] == "FIFO"
        assert calc_run["fx_set_id"] is not None
        assert calc_run["params_json"]
        assert calc_run["summary_json"]

        realized_events = _rows(
            """
            SELECT asset, qty_sold, proceeds, cost_basis, gain, quote_asset, fee_applied, matches_json
            FROM realized_events
            WHERE run_id = :run_id AND asset = :asset
            ORDER BY id
            """,
            {"run_id": run_id, "asset": asset},
        )
        assert len(realized_events) == 1
        event = realized_events[0]
        assert _as_decimal(event["qty_sold"]) == Decimal("0.25")
        assert _as_decimal(event["proceeds"]) == Decimal("400")
        assert _as_decimal(event["cost_basis"]) == Decimal("250")
        assert _as_decimal(event["gain"]) == Decimal("150")
        assert event["quote_asset"] == "EUR"
        assert _as_decimal(event["fee_applied"]) == Decimal("0")
        matches = json.loads(event["matches_json"])
        assert matches[0]["proceeds_eur"] == "400"
        assert matches[0]["cost_eur"] == "250.00"

        digest_row = _row_mapping(
            """
            SELECT run_id, input_hash, output_hash, manifest_hash, manifest_json
            FROM run_digests
            WHERE run_id = :run_id
            """,
            {"run_id": run_id},
        )
        assert digest_row is not None
        assert digest_row["input_hash"] == digests["input_hash"]
        assert digest_row["output_hash"] == digests["output_hash"]
        assert digest_row["manifest_hash"] == digests["manifest_hash"]
        manifest = json.loads(digest_row["manifest_json"])
        assert manifest["run_id"] == run_id
        assert manifest["run"]["jurisdiction"] == "HR"
        assert manifest["outputs"]

        audit_actions = _rows(
            """
            SELECT action
            FROM audit_log
            WHERE target_type = 'calc_runs' AND target_id = :run_id
            ORDER BY id
            """,
            {"run_id": run_id},
        )
        assert {row["action"] for row in audit_actions} >= {"calc:start"}
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_calculate_v2_invalid_jurisdiction_rejects_before_creating_calc_run():
    _ensure_schema()
    before_latest_run_id = _latest_calc_run_id()

    response = client.post(
        "/calculate/v2",
        json={"jurisdiction": "ZZ", "tax_year": 2025},
    )

    assert response.status_code == 400
    assert "Unsupported jurisdiction" in response.text
    assert _latest_calc_run_id() == before_latest_run_id


def test_calculate_v2_unsupported_quote_failure_closes_error_run_without_realized_events_or_digest():
    _ensure_schema()
    memo_tag = f"persist-unsupported-quote-{uuid.uuid4().hex}"
    asset = f"Q{uuid.uuid4().hex[:10]}".upper()
    filename = f"persist_unsupported_quote_{uuid.uuid4().hex}.csv"
    before_latest_run_id = _latest_calc_run_id()

    try:
        response = _post_import_multiple(filename, _unsupported_quote_csv(memo_tag=memo_tag, asset=asset), reset=False)
        assert response.status_code == 200, response.text
        assert _count_transactions_by_memo_fragment(memo_tag) == 2

        calc_response = client.post(
            "/calculate/v2",
            json={"jurisdiction": "HR", "tax_year": 2025, "strict_fx": False},
        )

        assert calc_response.status_code == 400
        assert "Unsupported quote asset 'BTC'" in calc_response.text

        error_run_id = _latest_calc_run_id()
        assert error_run_id > before_latest_run_id
        error_run = _row_mapping(
            """
            SELECT id, finished_at, jurisdiction, summary_json
            FROM calc_runs
            WHERE id = :run_id
            """,
            {"run_id": error_run_id},
        )
        assert error_run is not None
        assert error_run["finished_at"] is not None
        assert error_run["jurisdiction"] == "HR"
        summary_json = error_run["summary_json"]
        if isinstance(summary_json, str):
            summary_json = json.loads(summary_json)
        assert summary_json["status"] == "error"
        assert "Unsupported quote asset 'BTC'" in summary_json["error"]

        assert _rows("SELECT id FROM realized_events WHERE run_id = :run_id", {"run_id": error_run_id}) == []
        assert _row_mapping("SELECT id FROM run_digests WHERE run_id = :run_id", {"run_id": error_run_id}) is None
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)
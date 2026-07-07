from __future__ import annotations

import csv
import io
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


def _multi_year_csv(*, memo_tag: str, asset: str) -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2024-01-01T00:00:00Z,buy,{asset},3,EUR,300,EUR,0,ReportingFilters,{memo_tag} buy lot
2024-02-01T00:00:00Z,sell,{asset},1,EUR,150,EUR,0,ReportingFilters,{memo_tag} sell 2024
2025-02-01T00:00:00Z,sell,{asset},1,EUR,200,EUR,0,ReportingFilters,{memo_tag} sell 2025
"""


def _other_asset_csv(*, memo_tag: str, asset: str) -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},1,EUR,1000,EUR,0,ReportingFilters,{memo_tag} other buy
2025-03-01T00:00:00Z,sell,{asset},1,EUR,1300,EUR,0,ReportingFilters,{memo_tag} other sell
"""


def _post_import_multiple(files: list[tuple[str, str]], *, reset: bool = False):
    multipart = [
        (
            "files",
            (filename, content.encode("utf-8"), "text/csv"),
        )
        for filename, content in files
    ]
    return client.post(
        "/import/multiple",
        params={"reset": "true" if reset else "false"},
        files=multipart,
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


def _setup_reporting_fixture() -> tuple[str, str, str]:
    _ensure_schema()
    memo_tag = f"reporting-filter-{uuid.uuid4().hex}"
    target_asset = f"R{uuid.uuid4().hex[:10]}".upper()
    other_asset = f"S{uuid.uuid4().hex[:10]}".upper()
    response = _post_import_multiple(
        [
            (f"reporting_target_{uuid.uuid4().hex}.csv", _multi_year_csv(memo_tag=memo_tag, asset=target_asset)),
            (f"reporting_other_{uuid.uuid4().hex}.csv", _other_asset_csv(memo_tag=memo_tag, asset=other_asset)),
        ],
        reset=False,
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload.get("inserted") == 5 or payload.get("total_inserted") == 5 or "results" in payload
    return memo_tag, target_asset, other_asset


def _csv_rows(text_value: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text_value)))


def _find_summary_row(rows: list[dict[str, str]], *, section: str, key: str) -> dict[str, str]:
    matches = [row for row in rows if row.get("section") == section and row.get("key") == key]
    assert len(matches) == 1, rows
    return matches[0]


def test_report_summary_year_filter_separates_realized_events_for_same_asset_by_disposal_year():
    memo_tag, target_asset, _ = _setup_reporting_fixture()
    try:
        response_2024 = client.get("/report/summary", params={"year": 2024, "asset": target_asset})
        response_2025 = client.get("/report/summary", params={"year": 2025, "asset": target_asset})

        assert response_2024.status_code == 200, response_2024.text
        assert response_2025.status_code == 200, response_2025.text
        data_2024 = response_2024.json()
        data_2025 = response_2025.json()

        assert data_2024["year"] == 2024
        assert data_2024["summary_by_asset"] == {
            target_asset: {"proceeds": "150", "cost_basis": "100", "gain": "50"}
        }
        assert data_2024["by_month"] == {
            "2024-02": {"proceeds": "150", "cost_basis": "100", "gain": "50"}
        }
        assert data_2024["eur_summary"]["totals_eur"] == {
            "proceeds": "150",
            "cost_basis": "100",
            "gain": "50",
        }

        assert data_2025["year"] == 2025
        assert data_2025["summary_by_asset"] == {
            target_asset: {"proceeds": "200", "cost_basis": "100", "gain": "100"}
        }
        assert data_2025["by_month"] == {
            "2025-02": {"proceeds": "200", "cost_basis": "100", "gain": "100"}
        }
        assert data_2025["eur_summary"]["totals_eur"] == {
            "proceeds": "200",
            "cost_basis": "100",
            "gain": "100",
        }
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_report_summary_asset_filter_excludes_other_assets_from_same_year():
    memo_tag, target_asset, other_asset = _setup_reporting_fixture()
    try:
        response = client.get("/report/summary", params={"year": 2025, "asset": target_asset})

        assert response.status_code == 200, response.text
        data = response.json()
        assert set(data["summary_by_asset"]) == {target_asset}
        assert other_asset not in data["summary_by_asset"]
        assert data["summary_by_quote"] == {
            "EUR": {"proceeds": "200", "cost_basis": "100", "gain": "100"}
        }
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_report_summary_quote_asset_filter_excludes_events_in_other_quotes():
    memo_tag, target_asset, _ = _setup_reporting_fixture()
    try:
        eur_response = client.get(
            "/report/summary",
            params={"year": 2025, "asset": target_asset, "quote_asset": "EUR"},
        )
        usd_response = client.get(
            "/report/summary",
            params={"year": 2025, "asset": target_asset, "quote_asset": "USD"},
        )

        assert eur_response.status_code == 200, eur_response.text
        assert usd_response.status_code == 200, usd_response.text
        eur_data = eur_response.json()
        usd_data = usd_response.json()

        assert eur_data["summary_by_asset"] == {
            target_asset: {"proceeds": "200", "cost_basis": "100", "gain": "100"}
        }
        assert usd_data["summary_by_asset"] == {}
        assert usd_data["eur_summary"]["totals_eur"] == {
            "proceeds": "0",
            "cost_basis": "0",
            "gain": "0",
        }
        assert usd_data["warnings"] == []
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)


def test_export_summary_csv_year_filter_aggregates_only_events_from_requested_year_for_asset():
    memo_tag, target_asset, _ = _setup_reporting_fixture()
    try:
        response_2024 = client.get("/export/summary.csv", params={"year": 2024})
        response_2025 = client.get("/export/summary.csv", params={"year": 2025})

        if response_2024.status_code == 409 or response_2025.status_code == 409:
            blocker_text = response_2024.text if response_2024.status_code == 409 else response_2025.text
            if "Export blocked to protect your tax results" in blocker_text:
                pytest.skip(
                    "/export/summary.csv is blocked by unrelated existing local DB transactions "
                    "with missing acquisition history. This test passes in a clean DB and should "
                    "not mutate or delete unrelated user data."
                )

        assert response_2024.status_code == 200, response_2024.text
        assert response_2025.status_code == 200, response_2025.text
        assert 'filename="summary_2024.csv"' in response_2024.headers.get("content-disposition", "")
        assert 'filename="summary_2025.csv"' in response_2025.headers.get("content-disposition", "")

        rows_2024 = _csv_rows(response_2024.text)
        rows_2025 = _csv_rows(response_2025.text)
        asset_2024 = _find_summary_row(rows_2024, section="by_asset", key=target_asset)
        asset_2025 = _find_summary_row(rows_2025, section="by_asset", key=target_asset)

        assert Decimal(asset_2024["proceeds"]) == Decimal("150")
        assert Decimal(asset_2024["cost_basis"]) == Decimal("100")
        assert Decimal(asset_2024["gain"]) == Decimal("50")
        assert Decimal(asset_2025["proceeds"]) == Decimal("200")
        assert Decimal(asset_2025["cost_basis"]) == Decimal("100")
        assert Decimal(asset_2025["gain"]) == Decimal("100")

        month_keys_2024 = {row["key"] for row in rows_2024 if row["section"] == "by_month"}
        month_keys_2025 = {row["key"] for row in rows_2025 if row["section"] == "by_month"}
        assert "2024-02" in month_keys_2024
        assert "2025-02" not in month_keys_2024
        assert "2025-02" in month_keys_2025
        assert "2024-02" not in month_keys_2025
    finally:
        _delete_transactions_by_memo_fragment(memo_tag)
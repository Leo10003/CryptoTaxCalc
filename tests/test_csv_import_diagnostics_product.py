from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _upload_preview(csv_text: str):
    return client.post(
        "/upload/csv",
        files={
            "file": (
                "bad_transactions.csv",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        },
    )


def test_csv_preview_returns_structured_row_diagnostics_for_missing_base_amount():
    csv_text = "\n".join(
        [
            "timestamp,type,base_asset,base_amount,quote_asset,quote_amount",
            "2024-01-01T00:00:00Z,buy,BTC,,EUR,1000",
        ]
    )

    response = _upload_preview(csv_text)

    assert response.status_code == 200
    payload = response.json()

    assert payload["total_valid"] == 0
    assert payload["total_errors"] == 1
    assert "row 2" in payload["errors"][0].lower()

    details = payload.get("error_details")
    assert isinstance(details, list)
    assert len(details) == 1

    detail = details[0]
    assert detail["row_number"] == 2
    assert detail["field"] == "base_amount"
    assert "required" in detail["message"].lower()
    assert detail["raw_value"] in ("", None)
    assert "numeric" in detail["hint"].lower() or "required" in detail["hint"].lower()

    snippet = detail["snippet"]
    assert snippet["timestamp"] == "2024-01-01T00:00:00Z"
    assert snippet["type"] == "buy"
    assert snippet["base_asset"] == "BTC"


def test_csv_preview_returns_structured_row_diagnostics_for_bad_timestamp():
    csv_text = "\n".join(
        [
            "timestamp,type,base_asset,base_amount,quote_asset,quote_amount",
            "not-a-date,buy,ETH,1,EUR,2000",
        ]
    )

    response = _upload_preview(csv_text)

    assert response.status_code == 200
    payload = response.json()

    assert payload["total_valid"] == 0
    assert payload["total_errors"] == 1

    details = payload.get("error_details")
    assert isinstance(details, list)
    assert len(details) == 1

    detail = details[0]
    assert detail["row_number"] == 2
    assert detail["field"] == "timestamp"
    assert detail["raw_value"] == "not-a-date"
    assert "timestamp" in detail["hint"].lower() or "date" in detail["hint"].lower()


def test_csv_preview_limits_structured_diagnostics_but_reports_full_error_count():
    rows = [
        "timestamp,type,base_asset,base_amount,quote_asset,quote_amount",
    ]

    for index in range(30):
        rows.append(f"2024-01-{index + 1:02d}T00:00:00Z,buy,BTC,,EUR,1000")

    response = _upload_preview("\n".join(rows))

    assert response.status_code == 200
    payload = response.json()

    assert payload["total_valid"] == 0
    assert payload["total_errors"] == 30

    details = payload.get("error_details")
    summary = payload.get("error_summary")

    assert isinstance(details, list)
    assert len(details) == 25

    assert summary["total_error_rows"] == 30
    assert summary["shown_error_rows"] == 25
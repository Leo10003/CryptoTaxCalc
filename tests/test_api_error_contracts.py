from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cryptotaxcalc.app import app, _export_block_if_blockers

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _detail(response):
    payload = response.json()
    assert isinstance(payload, dict)
    assert "detail" in payload
    return payload["detail"]


def test_upload_csv_rejects_non_csv_and_empty_file_with_stable_json_detail():
    non_csv = client.post(
        "/upload/csv",
        files={"file": ("transactions.txt", b"not csv", "text/plain")},
    )
    empty_csv = client.post(
        "/upload/csv",
        files={"file": ("transactions.csv", b"", "text/csv")},
    )

    assert non_csv.status_code == 400
    assert non_csv.headers["content-type"].startswith("application/json")
    assert _detail(non_csv) == "Please upload a .csv file"

    assert empty_csv.status_code == 400
    assert empty_csv.headers["content-type"].startswith("application/json")
    assert _detail(empty_csv) == "Empty file"


def test_upload_csv_format_errors_return_user_actionable_csv_source_metadata():
    response = client.post(
        "/upload/csv",
        files={"file": ("duplicate_headers.csv", b"timestamp,type,timestamp\n2025,buy,2025\n", "text/csv")},
    )

    assert response.status_code == 400
    detail = _detail(response)
    assert isinstance(detail, dict)
    assert detail["message"] == "Duplicate CSV header(s): timestamp"
    assert set(detail) == {"message", "csv_source"}
    csv_source = detail["csv_source"]
    assert csv_source["recognized_source_status"] == "unsupported"
    assert csv_source["recognized_source_id"] is None
    assert isinstance(csv_source["recognized_source_signature"], str)
    assert len(csv_source["recognized_source_signature"]) == 64


def test_import_multiple_reports_per_file_errors_without_stack_traces_or_http_500():
    response = client.post(
        "/import/multiple",
        params={"reset": "false"},
        files=[
            (
                "files",
                (
                    "good.csv",
                    b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount\n2025-01-01,buy,BTC,1,EUR,100\n",
                    "text/csv",
                ),
            ),
            ("files", ("bad.txt", b"not,a,csv\n1,2,3\n", "text/plain")),
        ],
    )

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) >= {"results", "meta"}
    assert isinstance(payload["results"], list)
    assert len(payload["results"]) == 2

    good, bad = payload["results"]
    assert good["filename"] == "good.csv"
    assert good["inserted"] == 0
    assert good["recognized_source_status"] == "supported"
    assert bad["filename"] == "bad.txt"
    assert bad["inserted"] == 0
    assert bad["skipped_errors"] == 1
    assert bad["errors"] == ["Only .csv files are supported"]
    assert bad["recognized_source_status"] == "unsupported"
    assert "traceback" not in response.text.lower()


def test_calculate_v2_invalid_jurisdiction_returns_stable_client_error_without_run_creation_payload():
    response = client.post("/calculate/v2", json={"jurisdiction": "ZZ", "tax_year": 2025})

    assert response.status_code == 400
    detail = _detail(response)
    assert detail == "Unsupported jurisdiction: 'ZZ'. Supported: HR, IT, XX"
    assert "run_id" not in response.json()
    assert "traceback" not in response.text.lower()


def test_events_csv_bad_run_id_errors_are_stable_and_json_encoded():
    invalid = client.get("/export/events_csv?run_id=not-an-int")
    missing = client.get("/export/events_csv?run_id=999999999")

    assert invalid.status_code == 400
    assert invalid.headers["content-type"].startswith("application/json")
    assert _detail(invalid) == "Invalid run_id"

    assert missing.status_code == 400
    assert missing.headers["content-type"].startswith("application/json")
    assert _detail(missing) == "Run metadata not found for id=999999999"


def test_tax_blocking_export_error_contract_is_structured_and_actionable():
    with pytest.raises(Exception) as raised:
        _export_block_if_blockers(
            [
                {
                    "severity": "blocker",
                    "message": "Sold 1.00 BTC but no acquisition history was found for 1.00 BTC.",
                }
            ]
        )

    exc = raised.value
    assert getattr(exc, "status_code", None) == 409
    detail = getattr(exc, "detail", None)
    assert isinstance(detail, dict)
    assert detail == {
        "title": "Export blocked to protect your tax results",
        "reason": "Some assets were sold without any recorded acquisition history.",
        "what_this_means": (
            "Without knowing how you acquired these assets, the system would "
            "assume a zero cost basis, which could significantly overstate taxes."
        ),
        "how_to_fix": [
            "Import earlier trades from the same exchange",
            "Import deposit or transfer history",
            "Ensure CSVs cover your full trading history",
        ],
        "technical_details": "Sold 1.00 BTC but no acquisition history was found for 1.00 BTC.",
    }
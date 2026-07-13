from __future__ import annotations

from fastapi.testclient import TestClient

from cryptotaxcalc.app import app


def test_import_multiple_non_csv_has_client_guidance():
    client = TestClient(app)

    response = client.post(
        "/import/multiple",
        files={
            "files": (
                "transactions.txt",
                b"not,a,csv\n",
                "text/plain",
            )
        },
    )

    assert response.status_code == 200

    payload = response.json()
    result = payload["results"][0]

    assert result["filename"] == "transactions.txt"
    assert result["failed_filename"] == "transactions.txt"
    assert result["import_error_kind"] == "file_type"
    assert result["user_title"] == "Unsupported file type"
    assert "Upload a CSV file" in result["user_guidance"]
    assert result["support_page_url"] == "/support/report-issue"

    assert result["skipped_errors"] == 1
    assert "Only .csv files are supported" in result["errors"]


def test_import_multiple_unsupported_csv_has_client_guidance():
    client = TestClient(app)

    response = client.post(
        "/import/multiple",
        files={
            "files": (
                "mystery_exchange.csv",
                b"foo,bar\n1,2\n",
                "text/csv",
            )
        },
    )

    assert response.status_code == 200

    payload = response.json()
    result = payload["results"][0]

    assert result["filename"] == "mystery_exchange.csv"
    assert result["failed_filename"] == "mystery_exchange.csv"
    assert result["import_error_kind"] == "unsupported_format"
    assert result["user_title"] == "Unsupported CSV format"
    assert "not supported yet" in result["user_guidance"]
    assert "CryptoTaxCalc normalized template" in result["user_guidance"]
    assert result["support_page_url"] == "/support/report-issue"

    assert result["skipped_errors"] >= 1
    assert result["errors"]

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_workspace_template_contains_real_import_diagnostics_renderer():
    response = client.get("/workspace")

    assert response.status_code == 200
    html = response.text

    assert "function renderCsvImportDiagnostics(importResults)" in html
    assert "CSV rows need attention" in html
    assert "error_details" in html
    assert "error_summary" in html
    assert "setWizardErrorHtml(diagnosticsHtml)" in html


def test_import_multiple_returns_row_diagnostics_used_by_workspace_ui():
    csv_text = "\n".join(
        [
            "timestamp,type,base_asset,base_amount,quote_asset,quote_amount",
            "2024-01-01T00:00:00Z,buy,BTC,,EUR,1000",
        ]
    )

    response = client.post(
        "/import/multiple?reset=1",
        files={
            "files": (
                "bad_workspace_import.csv",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        },
    )

    assert response.status_code == 200
    payload = response.json()

    results = payload.get("results")
    assert isinstance(results, list)
    assert len(results) == 1

    result = results[0]
    assert result["filename"] == "bad_workspace_import.csv"
    assert result["inserted"] == 0
    assert result["skipped_errors"] == 1

    details = result.get("error_details")
    assert isinstance(details, list)
    assert len(details) == 1

    detail = details[0]
    assert detail["row_number"] == 2
    assert detail["field"] == "base_amount"
    assert "required" in detail["message"].lower()
    assert detail["hint"]

    summary = result.get("error_summary")
    assert summary["total_error_rows"] == 1
    assert summary["shown_error_rows"] == 1
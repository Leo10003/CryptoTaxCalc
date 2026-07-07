from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cryptotaxcalc.app import app
from cryptotaxcalc.runtime_paths import RESOURCE_ROOT

pytestmark = pytest.mark.smoke

client = TestClient(app)
TEMPLATES_DIR = RESOURCE_ROOT / "templates"


def _html(path: str) -> str:
    response = client.get(path)
    assert response.status_code == 200, response.text[:500]
    assert response.headers["content-type"].startswith("text/html")
    return response.text


def _template(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text(encoding="utf-8")


def test_landing_page_keeps_primary_demo_cta_docs_link_and_core_workflow_labels():
    html = _html("/")

    assert "CryptoTaxCalc" in html
    assert 'href="/demo/dashboard"' in html
    assert "Open the interactive demo dashboard" in html
    assert 'href="/docs"' in html
    assert "Upload multiple CSVs or load demo data." in html
    assert "Generate PDF / CSV + diagnostics bundle." in html
    assert "These results are for information only" in html


def test_workspace_upload_wizard_keeps_csv_file_input_jurisdiction_tax_year_and_data_quality_controls():
    html = _html("/workspace")

    required_fragments = [
        'id="wsWizardForm"',
        'id="wizardFiles"',
        'type="file"',
        'accept=".csv"',
        "multiple",
        'id="wizardDropzone"',
        "Drop your CSV files here",
        "or click to browse (CSV only)",
        'id="wizardPrecheckSummary"',
        'id="wizardUnsupportedPanel"',
        "Unsupported CSV format detected",
        "Download CryptoTaxCalc template CSV",
        'id="wizardJurisdiction"',
        '<option value="HR">Croatia (HR)</option>',
        '<option value="IT">Italy (IT)</option>',
        'id="wizardTaxYearSelect"',
        'id="wizardTaxYear"',
        "Rule set: —",
        "CSV-only import",
        "No API keys",
        "Run → results page → saved to History.",
    ]
    for fragment in required_fragments:
        assert fragment in html


def test_workspace_results_keeps_export_links_integrity_strip_and_blocker_acknowledgement_modal():
    html = _html("/workspace/results?run_id=123")

    required_fragments = [
        'id="rsExports"',
        "Exports are generated from this run.",
        'id="rsExportsScopeText"',
        'id="rsExportRunStamp"',
        'id="rsIntegrityScope"',
        'id="rsIntegrityHash"',
        'id="rsIntegrityInputs"',
        'id="rsIntegrityFx"',
        'id="rsIntegrityWarnings"',
        'id="rsIntegrityFees"',
        'id="btnPdfPreview"',
        'id="btnPdfDownload"',
        'id="btnPdfSubsetPreview"',
        'id="btnPdfSubsetDownload"',
        'id="btnCsvPreview"',
        'id="btnCsvDownload"',
        'id="btnZipPreview"',
        'id="btnZipDownload"',
        'id="rsExportBlockerBackdrop"',
        'aria-labelledby="rsEbTitle"',
        'id="rsEbReason"',
        'id="rsEbMeaning"',
        'id="rsEbFixList"',
        'id="rsEbTech"',
        'id="rsEbAck"',
        "I understand this export may be materially inaccurate due to missing cost basis",
        'id="rsEbProceed"',
        "Proceed anyway",
    ]
    for fragment in required_fragments:
        assert fragment in html


def test_csv_formats_page_keeps_user_guidance_and_supported_source_catalog_surface():
    html = _html("/csv/formats")

    assert "Supported CSV formats" in html
    assert "Export your transaction history in one of the formats below." in html
    assert "CryptoTaxCalc will save the structure for implementation" in html
    assert "Required" in html
    assert "Optional" in html
    assert "Filename hints" in html
    assert "generic" in html.lower()
    assert "timestamp" in html
    assert "base_asset" in html
    assert "quote_amount" in html


def test_admin_unsupported_csv_template_keeps_token_out_of_urls_and_uses_header_auth_only():
    template = _template("admin_csv_unsupported.html")

    assert "Unsupported CSV triage" in template
    assert "Use it to prioritize implementation" in template
    assert 'data-admin-token="{{ token }}"' in template
    assert "Do NOT pass token from query params" not in template
    assert "url.searchParams.delete('token')" in template
    assert "window.history.replaceState" in template
    assert "sessionStorage" in template
    assert "ctc_admin_token" in template
    assert "X-Admin-Token" in template
    assert "/admin/csv/unsupported/remove" in template
    assert "Mark implemented" in template
    assert "Copy signature" in template
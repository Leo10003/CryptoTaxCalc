from __future__ import annotations

from fastapi.testclient import TestClient

from cryptotaxcalc.app import app
from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_support_contact_endpoint_uses_configured_email(monkeypatch):
    monkeypatch.setenv("CTC_SUPPORT_EMAIL", "support@example.test")

    client = TestClient(app)
    response = client.get("/support/contact")

    assert response.status_code == 200
    assert response.json() == {
        "email": "support@example.test",
        "label": "support@example.test",
    }


def test_support_page_explains_how_to_send_downloaded_zip():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200
    html = response.text

    assert "/support/contact" in html
    assert "supportDestination" in html
    assert "Attach the downloaded zip to an email" in html
    assert "your CryptoTaxCalc support contact" in html
    assert "mailto:" in html


def test_results_overlay_explains_how_to_send_downloaded_zip():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"
    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert "/support/contact" in html
    assert "rsHelpSupportDestination" in html
    assert "After download, attach the zip to an email" in html
    assert "your CryptoTaxCalc support contact" in html
    assert "mailto:" in html

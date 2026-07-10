from __future__ import annotations

from fastapi.testclient import TestClient

from cryptotaxcalc.app import app
from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_email_support_endpoint_reports_missing_smtp_config(monkeypatch):
    monkeypatch.delenv("CTC_SUPPORT_EMAIL", raising=False)
    monkeypatch.delenv("SUPPORT_EMAIL", raising=False)
    monkeypatch.delenv("CTC_SMTP_HOST", raising=False)
    monkeypatch.delenv("CTC_SMTP_USERNAME", raising=False)
    monkeypatch.delenv("CTC_SMTP_PASSWORD", raising=False)
    monkeypatch.delenv("CTC_SMTP_FROM", raising=False)

    client = TestClient(app)

    response = client.post(
        "/support/report-issue/email",
        json={
            "user_message": "Email support config test",
            "contact": "test-client",
            "app_context": {"test": "email_support_missing_config"},
        },
    )

    assert response.status_code == 503
    assert "Email support is not configured" in response.json()["detail"]
    assert "CTC_SUPPORT_EMAIL" in response.json()["detail"]
    assert "CTC_SMTP_HOST" in response.json()["detail"]
    assert "CTC_SMTP_PASSWORD" in response.json()["detail"]


def test_support_page_has_email_support_button():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200
    html = response.text

    assert "Email support" in html
    assert "/support/report-issue/email" in html
    assert "emailSupportBtn" in html
    assert "Create support file" in html


def test_results_overlay_has_email_support_button():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"
    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert "Email support" in html
    assert "/support/report-issue/email" in html
    assert "rsHelpEmailSupport" in html
    assert "rsHelpEmailSupportReport" in html

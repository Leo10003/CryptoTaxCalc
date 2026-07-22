from __future__ import annotations

# Consolidated from:
# - tests/test_client_issue_report_page.py
# - tests/test_results_help_overlay_client_safe.py
# - tests/test_support_admin_page.py
# - tests/test_support_config_status.py
# - tests/test_support_contact_instructions.py
# - tests/test_support_secret_leak_guards.py


#========================================================================================
# Source: tests/test_client_issue_report_page.py
#========================================================================================
from fastapi.testclient import TestClient

from cryptotaxcalc.app import app


def test_client_issue_report_page_is_not_admin_ui():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200
    html = response.text

    assert "Report a problem" in html
    assert "/support/report-issue/client" in html
    assert "Create support file" in html
    assert "Support token" not in html
    assert "Recent issue reports" not in html
    assert "X-Admin-Token" not in html
    assert "Back" in html


def test_client_issue_report_endpoint_downloads_zip_without_token():
    client = TestClient(app)

    response = client.post(
        "/support/report-issue/client",
        json={
            "user_message": "Client support smoke test",
            "contact": "test-client",
            "app_context": {"test": "client_issue_report"},
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/zip")
    assert response.headers.get("x-issue-report-filename", "").startswith("issue_report_")
    assert response.content.startswith(b"PK")


#========================================================================================
# Source: tests/test_results_help_overlay_client_safe.py
#========================================================================================
from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_results_help_overlay_is_client_safe():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert 'id="rsHelpOverlay"' in html
    assert 'id="rsHelpCreateReport"' in html
    assert "/support/report-issue/client" in html
    assert "Create support file" in html

    assert "Support token" not in html
    assert "rsHelpToken" not in html
    assert "X-Admin-Token" not in html
    assert "Recent reports" not in html
    assert "rsHelpRefreshHistory" not in html
    assert "rsHelpHistory" not in html
    assert "/support/report-issue/history" not in html


#========================================================================================
# Source: tests/test_support_admin_page.py
#========================================================================================
from fastapi.testclient import TestClient

from cryptotaxcalc.app import app


def test_support_admin_page_renders_safe_shell():
    client = TestClient(app)

    response = client.get("/support/admin")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "").lower()

    html = response.text

    assert "Support admin" in html
    assert "adminToken" in html
    assert "/support/config/status" in html
    assert "/support/report-issue/history?limit=10" in html
    assert "/support/report-issue/download/" in html

    assert "Check email configuration" in html
    assert "Load recent reports" in html
    assert "Email support ready" in html

    assert "CTC_SMTP_PASSWORD" not in html
    assert "ADMIN_TOKEN=" not in html
    assert "TG_BOT_TOKEN" not in html
    assert "TELEGRAM_BOT_TOKEN" not in html
    assert ".env" not in html
    assert "raw CSV files" in html
    assert "database snapshots" in html


def test_client_support_page_does_not_show_admin_console():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200

    html = response.text

    assert "Support admin" not in html
    assert "adminToken" not in html
    assert "/support/config/status" not in html
    assert "/support/report-issue/history?limit=10" not in html


#========================================================================================
# Source: tests/test_support_config_status.py
#========================================================================================
from fastapi.testclient import TestClient

import cryptotaxcalc.app as app_module
from cryptotaxcalc.app import app


def _allow_admin_for_test():
    app.dependency_overrides[app_module.require_bundle_admin] = lambda: None


def _clear_admin_override():
    app.dependency_overrides.pop(app_module.require_bundle_admin, None)


def test_support_config_status_requires_admin_token(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "test-admin-token")

    client = TestClient(app)

    response = client.get("/support/config/status")

    # Existing protected support endpoints may intentionally hide themselves
    # as 404 when unauthorized.
    assert response.status_code in {401, 403, 404}


def test_support_config_status_reports_safe_booleans(monkeypatch):
    monkeypatch.setenv("CTC_SUPPORT_EMAIL", "support@example.test")
    monkeypatch.setenv("CTC_SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("CTC_SMTP_PORT", "2525")
    monkeypatch.setenv("CTC_SMTP_USERNAME", "smtp-user@example.test")
    monkeypatch.setenv("CTC_SMTP_PASSWORD", "super-secret-password")
    monkeypatch.setenv("CTC_SMTP_FROM", "sender@example.test")
    monkeypatch.setenv("CTC_SMTP_TLS", "0")

    _allow_admin_for_test()

    try:
        client = TestClient(app)

        response = client.get("/support/config/status")

        assert response.status_code == 200

        payload = response.json()

        assert payload == {
            "support_email_configured": True,
            "smtp_host_configured": True,
            "smtp_port": 2525,
            "smtp_username_configured": True,
            "smtp_password_configured": True,
            "smtp_from_configured": True,
            "smtp_tls_enabled": False,
            "email_support_ready": True,
            "missing": [],
        }

        payload_text = response.text

        assert "super-secret-password" not in payload_text
        assert "test-admin-token" not in payload_text
        assert "smtp-user@example.test" not in payload_text
        assert "sender@example.test" not in payload_text
        assert "support@example.test" not in payload_text
    finally:
        _clear_admin_override()


def test_support_config_status_reports_missing_email_config(monkeypatch):
    monkeypatch.delenv("CTC_SUPPORT_EMAIL", raising=False)
    monkeypatch.delenv("SUPPORT_EMAIL", raising=False)
    monkeypatch.delenv("CTC_SMTP_HOST", raising=False)
    monkeypatch.delenv("CTC_SMTP_USERNAME", raising=False)
    monkeypatch.delenv("CTC_SMTP_PASSWORD", raising=False)
    monkeypatch.delenv("CTC_SMTP_FROM", raising=False)

    _allow_admin_for_test()

    try:
        client = TestClient(app)

        response = client.get("/support/config/status")

        assert response.status_code == 200

        payload = response.json()

        assert payload["support_email_configured"] is False
        assert payload["smtp_host_configured"] is False
        assert payload["smtp_password_configured"] is False
        assert payload["email_support_ready"] is False

        assert "CTC_SUPPORT_EMAIL" in payload["missing"]
        assert "CTC_SMTP_HOST" in payload["missing"]
        assert "CTC_SMTP_PASSWORD" in payload["missing"]
    finally:
        _clear_admin_override()


#========================================================================================
# Source: tests/test_support_contact_instructions.py
#========================================================================================
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


#========================================================================================
# Source: tests/test_support_secret_leak_guards.py
#========================================================================================
import json
import zipfile

from fastapi.testclient import TestClient

import cryptotaxcalc.app as app_module
from cryptotaxcalc.app import app
from cryptotaxcalc.exporter import build_issue_report_bundle


SECRET_VALUES = {
    "admin-token-leak-test",
    "smtp-password-leak-test",
    "telegram-token-leak-test",
    "github-token-leak-test",
}


def _assert_no_secret_values(text: str) -> None:
    for secret in SECRET_VALUES:
        assert secret not in text


def test_support_pages_do_not_render_secret_values(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "admin-token-leak-test")
    monkeypatch.setenv("CTC_SMTP_PASSWORD", "smtp-password-leak-test")
    monkeypatch.setenv("TG_BOT_TOKEN", "telegram-token-leak-test")
    monkeypatch.setenv("GITHUB_TOKEN", "github-token-leak-test")

    client = TestClient(app)

    for path in ["/support/report-issue", "/support/admin"]:
        response = client.get(path)

        assert response.status_code == 200
        _assert_no_secret_values(response.text)


def test_support_config_status_does_not_return_secret_values(monkeypatch):
    monkeypatch.setenv("CTC_SUPPORT_EMAIL", "support@example.test")
    monkeypatch.setenv("CTC_SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("CTC_SMTP_PORT", "587")
    monkeypatch.setenv("CTC_SMTP_USERNAME", "smtp-user@example.test")
    monkeypatch.setenv("CTC_SMTP_PASSWORD", "smtp-password-leak-test")
    monkeypatch.setenv("CTC_SMTP_FROM", "sender@example.test")
    monkeypatch.setenv("TG_BOT_TOKEN", "telegram-token-leak-test")
    monkeypatch.setenv("GITHUB_TOKEN", "github-token-leak-test")

    app.dependency_overrides[app_module.require_bundle_admin] = lambda: None

    try:
        client = TestClient(app)
        response = client.get("/support/config/status")

        assert response.status_code == 200

        payload = response.json()
        payload_text = json.dumps(payload, sort_keys=True)

        assert payload["smtp_password_configured"] is True
        _assert_no_secret_values(payload_text)

        assert "smtp-user@example.test" not in payload_text
        assert "sender@example.test" not in payload_text
        assert "support@example.test" not in payload_text
    finally:
        app.dependency_overrides.pop(app_module.require_bundle_admin, None)


def test_client_issue_report_bundle_does_not_include_secret_values(monkeypatch, tmp_path):
    monkeypatch.setenv("ADMIN_TOKEN", "admin-token-leak-test")
    monkeypatch.setenv("CTC_SMTP_PASSWORD", "smtp-password-leak-test")
    monkeypatch.setenv("TG_BOT_TOKEN", "telegram-token-leak-test")
    monkeypatch.setenv("GITHUB_TOKEN", "github-token-leak-test")

    bundle_path = build_issue_report_bundle(
        user_message="Secret leak regression test",
        contact="client@example.test",
        app_context={
            "surface": "secret_leak_test",
            "admin_token": "admin-token-leak-test",
            "smtp_password": "smtp-password-leak-test",
            "telegram_token": "telegram-token-leak-test",
            "github_token": "github-token-leak-test",
        },
    )

    with zipfile.ZipFile(bundle_path, "r") as zf:
        names = zf.namelist()

        assert ".env" not in names
        assert not any(name.endswith(".env") for name in names)
        assert not any("site-packages" in name for name in names)
        assert not any(".venv" in name for name in names)

        for name in names:
            if name.endswith("/"):
                continue

            data = zf.read(name)

            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                continue

            _assert_no_secret_values(text)

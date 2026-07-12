from __future__ import annotations

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

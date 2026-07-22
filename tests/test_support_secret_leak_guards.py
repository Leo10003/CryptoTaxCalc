from __future__ import annotations

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

from __future__ import annotations

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

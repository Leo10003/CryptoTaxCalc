from __future__ import annotations

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

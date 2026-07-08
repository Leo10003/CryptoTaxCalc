from __future__ import annotations

import os
import zipfile

import pytest
from fastapi.testclient import TestClient

import cryptotaxcalc.app as app_module
from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke


def test_issue_report_endpoint_requires_bundle_admin(monkeypatch):
    def deny_bundle_admin(**kwargs):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Not found")

    monkeypatch.setattr(app_module, "require_bundle_admin", deny_bundle_admin)

    client = TestClient(app)

    response = client.post(
        "/support/report-issue",
        json={
            "user_message": "Something failed.",
            "contact": "client@example.com",
            "app_context": {"route": "/calculate/v2"},
        },
    )

    assert response.status_code == 404


def test_issue_report_endpoint_creates_bundle_when_authorized(tmp_path, monkeypatch):
    import cryptotaxcalc.exporter as exporter

    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)

    project_root = tmp_path / "project"
    logs_dir = project_root / "logs" / "calc" / "runs" / "77"
    logs_dir.mkdir(parents=True)
    (logs_dir / "trace.json").write_text('{"run_id":77}', encoding="utf-8")

    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    client = TestClient(app)

    response = client.post(
        "/support/report-issue",
        headers={"X-Admin-Token": "test-token"},
        json={
            "user_message": "Calculation failed after CSV import.",
            "contact": "client@example.com",
            "app_context": {"route": "/calculate/v2", "run_id": 77},
        },
    )

    assert response.status_code == 200, response.text

    payload = response.json()

    assert payload["ok"] is True
    assert payload["filename"].startswith("issue_report_")
    assert payload["filename"].endswith(".zip")
    assert payload["download_url"] == f"/support/report-issue/download/{payload['filename']}"
    assert payload["size_bytes"] > 0
    assert payload["raw_data_included"] is False
    assert payload["database_included"] is False

    bundle_path = payload["path"]
    assert os.path.exists(bundle_path)

    with zipfile.ZipFile(bundle_path) as zf:
        names = set(zf.namelist())

    assert "issue_report.json" in names
    assert "logs/calc/runs/77/trace.json" in names
    assert "_meta/bundle_manifest.json" in names

def test_issue_report_download_endpoint_returns_zip(tmp_path, monkeypatch):
    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)

    support_dir = tmp_path / "project" / "support_bundles"
    support_dir.mkdir(parents=True)

    bundle_path = support_dir / "issue_report_test.zip"
    with zipfile.ZipFile(bundle_path, "w") as zf:
        zf.writestr("issue_report.json", "{}")

    monkeypatch.setattr(app_module, "PROJECT_ROOT", tmp_path / "project")

    client = TestClient(app)

    response = client.get(
        "/support/report-issue/download/issue_report_test.zip",
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/zip")
    assert response.content.startswith(b"PK")


def test_issue_report_download_rejects_path_traversal(tmp_path, monkeypatch):
    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)
    monkeypatch.setattr(app_module, "PROJECT_ROOT", tmp_path / "project")

    client = TestClient(app)

    response = client.get(
        "/support/report-issue/download/..%2Fsecret.zip",
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code in {400, 404}


def test_issue_report_download_rejects_non_issue_report_zip(tmp_path, monkeypatch):
    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)
    monkeypatch.setattr(app_module, "PROJECT_ROOT", tmp_path / "project")

    client = TestClient(app)

    response = client.get(
        "/support/report-issue/download/support_bundle_test.zip",
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 400

def test_issue_report_page_renders_client_form():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "").lower()

    html = response.text

    assert "Report an issue" in html
    assert "issueReportForm" in html
    assert "userMessage" in html
    assert "supportToken" in html
    assert "/support/report-issue" in html
    assert "downloadBundle(data.download_url, data.filename)" in html
    assert "Excluded by default: raw CSVs" in html
    assert "Excluded by default: database snapshots" in html
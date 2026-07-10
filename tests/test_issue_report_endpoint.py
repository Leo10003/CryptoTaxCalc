from __future__ import annotations

import json
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
    assert "diagnostics_inventory.json" in names
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

    assert "Report a problem" in html
    assert "issueReportForm" in html
    assert "userMessage" in html
    assert "Create support file" in html
    assert "/support/report-issue/client" in html

    assert "response.blob()" in html
    assert "X-Issue-Report-Filename" in html
    assert "downloadBlob(blob, filename)" in html

    assert "Excluded by default: raw CSVs" in html
    assert "Excluded by default: database snapshots" in html

    assert "Back" in html
    assert "Back to workspace" in html

    assert "Support token" not in html
    assert "supportToken" not in html
    assert "X-Admin-Token" not in html
    assert "tokenHeaders" not in html
    assert "Recent issue reports" not in html
    assert "refreshHistoryBtn" not in html
    assert "historyList" not in html
    assert "/support/report-issue/history" not in html

def test_issue_report_history_endpoint_lists_safe_index_rows(tmp_path, monkeypatch):
    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)

    project_root = tmp_path / "project"
    meta_dir = project_root / "support_bundles" / "_meta"
    meta_dir.mkdir(parents=True)

    index_path = meta_dir / "issue_reports.jsonl"
    index_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "created_at": "2026-07-08T21:00:00Z",
                        "kind": "issue_report_index_entry",
                        "filename": "issue_report_old.zip",
                        "path": str(project_root / "support_bundles" / "issue_report_old.zip"),
                        "size_bytes": 100,
                        "sha256": "a" * 64,
                        "included_file_count": 2,
                        "missing_expected_file_count": 3,
                        "trace_file_count": 1,
                        "raw_data_included": False,
                        "database_included": False,
                    }
                ),
                "not-json",
                json.dumps(
                    {
                        "created_at": "2026-07-08T22:00:00Z",
                        "kind": "issue_report_index_entry",
                        "filename": "issue_report_new.zip",
                        "path": str(project_root / "support_bundles" / "issue_report_new.zip"),
                        "size_bytes": 200,
                        "sha256": "b" * 64,
                        "included_file_count": 4,
                        "missing_expected_file_count": 1,
                        "trace_file_count": 2,
                        "raw_data_included": False,
                        "database_included": False,
                        "user_message": "must not be exposed",
                        "contact": "client@example.com",
                    }
                ),
                json.dumps(
                    {
                        "created_at": "2026-07-08T23:00:00Z",
                        "filename": "../issue_report_escape.zip",
                    }
                ),
                json.dumps(
                    {
                        "created_at": "2026-07-08T23:30:00Z",
                        "filename": "support_bundle_not_allowed.zip",
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(app_module, "PROJECT_ROOT", project_root)

    client = TestClient(app)

    response = client.get(
        "/support/report-issue/history",
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 200, response.text

    payload = response.json()

    assert payload["ok"] is True
    assert payload["count"] == 2

    reports = payload["reports"]

    assert reports[0]["filename"] == "issue_report_new.zip"
    assert reports[0]["download_url"] == "/support/report-issue/download/issue_report_new.zip"
    assert reports[0]["size_bytes"] == 200
    assert reports[0]["included_file_count"] == 4
    assert reports[0]["missing_expected_file_count"] == 1
    assert reports[0]["trace_file_count"] == 2
    assert reports[0]["raw_data_included"] is False
    assert reports[0]["database_included"] is False

    response_text = response.text
    assert "must not be exposed" not in response_text
    assert "client@example.com" not in response_text
    assert "../issue_report_escape.zip" not in response_text
    assert "support_bundle_not_allowed.zip" not in response_text


def test_issue_report_history_endpoint_handles_missing_index(tmp_path, monkeypatch):
    def allow_bundle_admin(**kwargs):
        return None

    monkeypatch.setattr(app_module, "require_bundle_admin", allow_bundle_admin)
    monkeypatch.setattr(app_module, "PROJECT_ROOT", tmp_path / "project")

    client = TestClient(app)

    response = client.get(
        "/support/report-issue/history",
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 200, response.text

    payload = response.json()

    assert payload == {
        "ok": True,
        "count": 0,
        "reports": [],
    }
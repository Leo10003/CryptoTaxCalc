from __future__ import annotations

import json
import zipfile

import pytest

from cryptotaxcalc import exporter

pytestmark = pytest.mark.smoke


def test_issue_report_bundle_includes_safe_diagnostics_only(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    # Diagnostic files that should be included.
    logs_root = project_root / "logs"
    workspace_dir = logs_root / "workspace"
    calc_run_dir = logs_root / "calc" / "runs" / "123"
    csv_sources_dir = project_root / "storage_raw" / "csv_sources"

    workspace_dir.mkdir(parents=True)
    calc_run_dir.mkdir(parents=True)
    csv_sources_dir.mkdir(parents=True)

    (logs_root / "latest_error_location.json").write_text(
        json.dumps({"component": "calc", "latest_error_json": "logs/calc/latest_error.json"}),
        encoding="utf-8",
    )
    (logs_root / "latest_error_location.txt").write_text(
        "component=calc\ncomponent_log_dir=logs/calc\n",
        encoding="utf-8",
    )
    (workspace_dir / "errors.txt").write_text("workspace failure\n", encoding="utf-8")
    (workspace_dir / "last_error.json").write_text(
        json.dumps({"stage": "workspace", "error_message": "boom"}),
        encoding="utf-8",
    )
    (workspace_dir / "errors.jsonl").write_text(
        json.dumps({"stage": "workspace"}) + "\n",
        encoding="utf-8",
    )
    (logs_root / "calc" / "last_run.json").parent.mkdir(parents=True, exist_ok=True)
    (logs_root / "calc" / "last_run.json").write_text(
        json.dumps({"run_id": 123, "events_count": 1}),
        encoding="utf-8",
    )
    (calc_run_dir / "trace.json").write_text(
        json.dumps({"run_id": 123, "jurisdiction": "HR"}),
        encoding="utf-8",
    )
    (csv_sources_dir / "unsupported_structures.json").write_text(
        json.dumps({"signatures": {"abc": {"headers": ["weird", "csv"]}}}),
        encoding="utf-8",
    )

    # Sensitive files that must NOT be included by the default issue report.
    (project_root / ".env").write_text("SECRET=value\n", encoding="utf-8")
    (project_root / "cryptotaxcalc.sqlite").write_text("fake-db", encoding="utf-8")
    raw_dir = project_root / "storage_raw" / "imports"
    raw_dir.mkdir(parents=True)
    (raw_dir / "client_transactions.csv").write_text("timestamp,type\n", encoding="utf-8")

    bundle_path = exporter.build_issue_report_bundle(
        user_message="Calculation failed after importing Binance CSV.",
        contact="client@example.com",
        app_context={"route": "/calculate/v2", "ui": "workspace"},
        output_dir=tmp_path / "out",
    )

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as zf:
        names = set(zf.namelist())

        assert "issue_report.json" in names
        assert "README_ISSUE_REPORT.txt" in names
        assert "_meta/bundle_manifest.json" in names

        assert "logs/latest_error_location.json" in names
        assert "logs/latest_error_location.txt" in names
        assert "logs/workspace/errors.txt" in names
        assert "logs/workspace/last_error.json" in names
        assert "logs/workspace/errors.jsonl" in names
        assert "logs/calc/last_run.json" in names
        assert "logs/calc/runs/123/trace.json" in names
        assert "storage_raw/csv_sources/unsupported_structures.json" in names

        forbidden = {
            ".env",
            "cryptotaxcalc.sqlite",
            "storage_raw/imports/client_transactions.csv",
        }
        assert forbidden.isdisjoint(names)

        with zf.open("issue_report.json") as fh:
            report = json.loads(fh.read().decode("utf-8"))

        assert report["kind"] == "issue_report"
        assert report["user_message"] == "Calculation failed after importing Binance CSV."
        assert report["contact"] == "client@example.com"
        assert report["app_context"] == {"route": "/calculate/v2", "ui": "workspace"}

        with zf.open("_meta/bundle_manifest.json") as fh:
            manifest = json.loads(fh.read().decode("utf-8"))

        manifest_paths = {item["path"] for item in manifest["files"]}

        assert "logs/calc/runs/123/trace.json" in manifest_paths
        assert "storage_raw/csv_sources/unsupported_structures.json" in manifest_paths
        assert manifest["issue_report"]["raw_data_included"] is False
        assert manifest["issue_report"]["database_included"] is False


def test_issue_report_bundle_handles_missing_logs(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()

    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    bundle_path = exporter.build_issue_report_bundle(
        user_message="Something went wrong.",
        output_dir=tmp_path / "out",
    )

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as zf:
        names = set(zf.namelist())

        assert "issue_report.json" in names
        assert "README_ISSUE_REPORT.txt" in names
        assert "_meta/bundle_manifest.json" in names

        with zf.open("issue_report.json") as fh:
            report = json.loads(fh.read().decode("utf-8"))

        assert report["user_message"] == "Something went wrong."
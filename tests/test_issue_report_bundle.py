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
        assert "diagnostics_inventory.json" in names
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

        with zf.open("diagnostics_inventory.json") as fh:
            inventory = json.loads(fh.read().decode("utf-8"))

        assert inventory["kind"] == "diagnostics_inventory"
        assert "logs/calc/runs/123/trace.json" in inventory["included_files"]
        assert "storage_raw/csv_sources/unsupported_structures.json" in inventory["included_files"]
        assert inventory["privacy_omissions"]["raw_import_csv_files"] == "excluded_by_default"
        assert inventory["privacy_omissions"]["database_snapshots"] == "excluded_by_default"

        assert report["kind"] == "issue_report"
        assert report["user_message"] == "Calculation failed after importing Binance CSV."
        assert report["contact"] == "client@example.com"
        assert report["app_context"] == {"route": "/calculate/v2", "ui": "workspace"}
        env = report["environment"]
        assert env["exporter_version"]
        assert env["python_version"]
        assert env["project_root_name"] == "project"
        assert env["diagnostics_present"]["logs/calc/last_run.json"] is True
        assert env["diagnostics_present"]["logs/workspace/errors.txt"] is True
        assert env["calc_trace_count"] == 1

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
        assert "diagnostics_inventory.json" in names
        assert "README_ISSUE_REPORT.txt" in names
        assert "_meta/bundle_manifest.json" in names

        with zf.open("issue_report.json") as fh:
            report = json.loads(fh.read().decode("utf-8"))

        with zf.open("diagnostics_inventory.json") as fh:
            inventory = json.loads(fh.read().decode("utf-8"))

        assert "logs/calc/last_run.json" in inventory["missing_expected_files"]
        assert "logs/workspace/errors.txt" in inventory["missing_expected_files"]
        assert inventory["trace_files"] == []

        assert report["user_message"] == "Something went wrong."

        assert report["environment"]["project_root_name"] == "project"
        assert report["environment"]["calc_trace_count"] == 0
        assert report["environment"]["diagnostics_present"]["logs/calc/last_run.json"] is False

def test_issue_report_bundle_redacts_user_supplied_secrets(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()

    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    bundle_path = exporter.build_issue_report_bundle(
        user_message=(
            "Calculation failed. "
            "Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456 "
            "ADMIN_TOKEN=super-secret-token "
            "password: hunter2"
        ),
        contact="client@example.com token=contact-secret",
        app_context={
            "route": "/calculate/v2",
            "headers": {
                "Authorization": "Bearer nestedtokenabcdefghijklmnop",
                "X-Api-Key": "api_key=nested-secret",
            },
            "notes": ["refresh_token=list-secret-token"],
        },
        output_dir=tmp_path / "out",
    )

    with zipfile.ZipFile(bundle_path) as zf:
        with zf.open("issue_report.json") as fh:
            report_text = fh.read().decode("utf-8")
            report = json.loads(report_text)

    assert "abcdefghijklmnopqrstuvwxyz123456" not in report_text
    assert "super-secret-token" not in report_text
    assert "hunter2" not in report_text
    assert "contact-secret" not in report_text
    assert "nestedtokenabcdefghijklmnop" not in report_text
    assert "nested-secret" not in report_text
    assert "list-secret-token" not in report_text

    assert "Authorization: Bearer [REDACTED]" in report["user_message"]
    assert "ADMIN_TOKEN=[REDACTED]" in report["user_message"]
    assert "password: [REDACTED]" in report["user_message"]
    assert report["contact"] == "client@example.com token=[REDACTED]"
    assert report["app_context"]["headers"]["Authorization"] == "Bearer [REDACTED]"
    assert report["app_context"]["headers"]["X-Api-Key"] == "api_key=[REDACTED]"
    assert report["app_context"]["notes"] == ["refresh_token=[REDACTED]"]

def test_issue_report_bundle_redacts_secrets_from_diagnostic_files(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    log_dir = project_root / "logs" / "workspace"
    log_dir.mkdir(parents=True)

    source_log = log_dir / "errors.txt"
    source_log.write_text(
        "Failure while calling API. "
        "Authorization: Bearer logfiletokensecret123456 "
        "api_key=log-api-key-secret "
        "password: log-password-secret\n",
        encoding="utf-8",
    )

    calc_dir = project_root / "logs" / "calc"
    calc_dir.mkdir(parents=True)
    source_json = calc_dir / "last_run.json"
    source_json.write_text(
        json.dumps(
            {
                "run_id": 123,
                "debug_header": "Bearer jsonbearersecret123456",
                "nested": {"token": "token=json-token-secret"},
            }
        ),
        encoding="utf-8",
    )

    bundle_path = exporter.build_issue_report_bundle(
        user_message="Calculation failed.",
        output_dir=tmp_path / "out",
    )

    # Source files are not mutated.
    assert "logfiletokensecret123456" in source_log.read_text(encoding="utf-8")
    assert "log-api-key-secret" in source_log.read_text(encoding="utf-8")
    assert "json-token-secret" in source_json.read_text(encoding="utf-8")

    with zipfile.ZipFile(bundle_path) as zf:
        bundled_log = zf.read("logs/workspace/errors.txt").decode("utf-8")
        bundled_json = zf.read("logs/calc/last_run.json").decode("utf-8")

    assert "logfiletokensecret123456" not in bundled_log
    assert "log-api-key-secret" not in bundled_log
    assert "log-password-secret" not in bundled_log
    assert "jsonbearersecret123456" not in bundled_json
    assert "json-token-secret" not in bundled_json

    assert "Authorization: Bearer [REDACTED]" in bundled_log
    assert "api_key=[REDACTED]" in bundled_log
    assert "password: [REDACTED]" in bundled_log
    assert "Bearer [REDACTED]" in bundled_json
    assert "token=[REDACTED]" in bundled_json

def test_issue_report_environment_snapshot_avoids_sensitive_values(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()

    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    bundle_path = exporter.build_issue_report_bundle(
        user_message="Need help.",
        app_context={"full_path_should_not_leak": str(tmp_path)},
        output_dir=tmp_path / "out",
    )

    with zipfile.ZipFile(bundle_path) as zf:
        report_text = zf.read("issue_report.json").decode("utf-8")
        report = json.loads(report_text)

    env = report["environment"]

    assert "python_version" in env
    assert "system" in env
    assert "diagnostics_present" in env

    assert "cwd" not in env
    assert "project_root" not in env
    assert "environment_variables" not in env
    assert "username" not in env
    assert "hostname" not in env

    # Environment snapshot must not include full temp/project paths.
    assert str(project_root) not in json.dumps(env)
    assert str(tmp_path) not in json.dumps(env)
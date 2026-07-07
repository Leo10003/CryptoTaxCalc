from __future__ import annotations

import json
import logging
import uuid

from starlette.testclient import TestClient

from cryptotaxcalc.app import app
from cryptotaxcalc.logging_setup import get_logger, log_exception_and_record_latest


def test_component_logger_writes_text_and_json_logs(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTOTAXCALC_LOGS_DIR", str(tmp_path / "logs"))

    component = f"test_{uuid.uuid4().hex}"
    logger = get_logger(component, level=logging.INFO)

    logger.info("diagnostic hello", extra={"ctc_request_id": "req-test-123"})

    for handler in logger.handlers:
        handler.flush()

    comp_dir = tmp_path / "logs" / component
    text_log = comp_dir / "events.log"
    json_log = comp_dir / "events.jsonl"

    assert text_log.exists()
    assert json_log.exists()

    assert "diagnostic hello" in text_log.read_text(encoding="utf-8")

    lines = json_log.read_text(encoding="utf-8").splitlines()
    assert lines
    payload = json.loads(lines[-1])
    assert payload["component"] == component
    assert payload["message"] == "diagnostic hello"
    assert payload["request_id"] == "req-test-123"


def test_latest_error_json_contains_stacktrace_and_context(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTOTAXCALC_LOGS_DIR", str(tmp_path / "logs"))

    component = f"err_{uuid.uuid4().hex}"

    try:
        raise RuntimeError("boom for diagnostics")
    except RuntimeError as exc:
        log_exception_and_record_latest(
            component,
            exc,
            message="calculation failed",
            context={"run_id": 123, "stage": "fifo"},
        )

    latest = tmp_path / "logs" / component / "latest_error.json"
    assert latest.exists()

    payload = json.loads(latest.read_text(encoding="utf-8"))
    assert payload["component"] == component
    assert payload["message"] == "calculation failed"
    assert payload["exception_type"] == "RuntimeError"
    assert "boom for diagnostics" in payload["stacktrace"]
    assert payload["context"]["run_id"] == 123
    assert payload["context"]["stage"] == "fifo"


def test_workspace_error_log_pointer_contains_error_text_log_path(tmp_path, monkeypatch):
    import cryptotaxcalc.calc_runner as calc_runner

    workspace_dir = tmp_path / "logs" / "workspace"
    errors_txt = workspace_dir / "errors.txt"
    pointer = tmp_path / "logs" / "workspace_error_log_path.txt"

    monkeypatch.setattr(calc_runner, "WORKSPACE_LOG_DIR", workspace_dir)
    monkeypatch.setattr(calc_runner, "WORKSPACE_ERRORS_TXT", errors_txt)
    monkeypatch.setattr(calc_runner, "WORKSPACE_ERROR_PATH_POINTER", pointer)

    class DummyConfig:
        jurisdiction = "HR"
        tax_year = 2024

    try:
        raise ValueError("workspace failed")
    except ValueError as exc:
        calc_runner.log_workspace_error(
            stage="unit_test",
            cfg=DummyConfig(),
            run_id=42,
            error=exc,
            extra={"asset": "BTC"},
        )

    assert errors_txt.exists()
    assert pointer.exists()

    assert pointer.read_text(encoding="utf-8") == str(errors_txt.resolve())

    text = errors_txt.read_text(encoding="utf-8")
    assert "stage=unit_test" in text
    assert "run_id=42" in text
    assert "ValueError: workspace failed" in text

    latest = workspace_dir / "last_error.json"
    payload = json.loads(latest.read_text(encoding="utf-8"))

    assert payload["stage"] == "unit_test"
    assert payload["run_id"] == 42
    assert payload["error_text_log_path"] == str(errors_txt.resolve())
    assert payload["error_log_pointer_path"] == str(pointer.resolve())


def test_http_responses_include_request_id_header():
    client = TestClient(app)

    response = client.get("/health", headers={"X-Request-ID": "req-contract-123"})

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "req-contract-123"


def test_root_latest_error_location_points_to_component_log_folder(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTOTAXCALC_LOGS_DIR", str(tmp_path / "logs"))

    component = f"pointer_{uuid.uuid4().hex}"

    try:
        raise RuntimeError("root pointer check")
    except RuntimeError as exc:
        log_exception_and_record_latest(
            component,
            exc,
            message="pointer failure",
            context={"stage": "unit_test"},
        )

    logs_root = tmp_path / "logs"
    txt_pointer = logs_root / "latest_error_location.txt"
    json_pointer = logs_root / "latest_error_location.json"

    assert txt_pointer.exists()
    assert json_pointer.exists()

    payload = json.loads(json_pointer.read_text(encoding="utf-8"))

    component_dir = logs_root / component
    latest_error = component_dir / "latest_error.json"

    assert payload["component"] == component
    assert payload["component_log_dir"] == str(component_dir.resolve())
    assert payload["latest_error_json"] == str(latest_error.resolve())
    assert payload["text_log"] == str((component_dir / "events.log").resolve())
    assert payload["json_log"] == str((component_dir / "events.jsonl").resolve())

    txt = txt_pointer.read_text(encoding="utf-8")
    assert f"component={component}" in txt
    assert f"component_log_dir={component_dir.resolve()}" in txt
    assert f"latest_error_json={latest_error.resolve()}" in txt
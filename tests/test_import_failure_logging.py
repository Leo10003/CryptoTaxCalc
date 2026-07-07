from __future__ import annotations

import json
import uuid

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_import_multiple_logs_preflight_csv_validation_errors(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTOTAXCALC_LOGS_DIR", str(tmp_path / "logs"))

    csv_text = "\n".join(
        [
            "timestamp,type,base_asset,base_amount,quote_asset,quote_amount",
            "2024-01-01T00:00:00Z,buy,BTC,,EUR,1000",
        ]
    )

    response = client.post(
        "/import/multiple?reset=1",
        files={
            "files": (
                f"bad_import_{uuid.uuid4().hex}.csv",
                csv_text.encode("utf-8"),
                "text/csv",
            )
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["results"][0]["inserted"] == 0
    assert payload["results"][0]["skipped_errors"] == 1

    import_latest = tmp_path / "logs" / "import" / "latest_error.json"
    assert import_latest.exists()

    latest_payload = json.loads(import_latest.read_text(encoding="utf-8"))
    assert latest_payload["component"] == "import"
    assert "preflight" in latest_payload["message"].lower()
    assert latest_payload["context"]["endpoint"] == "/import/multiple"
    assert latest_payload["context"]["stage"] == "preflight_parse_errors"
    assert latest_payload["context"]["reset"] is True
    assert latest_payload["context"]["error_count"] == 1
    assert "base_amount" in " ".join(latest_payload["context"]["errors_sample"])

    root_pointer = tmp_path / "logs" / "latest_error_location.json"
    assert root_pointer.exists()

    pointer_payload = json.loads(root_pointer.read_text(encoding="utf-8"))
    assert pointer_payload["component"] == "import"
    assert pointer_payload["component_log_dir"] == str((tmp_path / "logs" / "import").resolve())
    assert pointer_payload["latest_error_json"] == str(import_latest.resolve())


def test_upload_csv_logs_preview_parser_exceptions(tmp_path, monkeypatch):
    monkeypatch.setenv("CRYPTOTAXCALC_LOGS_DIR", str(tmp_path / "logs"))

    def boom(*args, **kwargs):
        raise RuntimeError("preview parser exploded")

    monkeypatch.setattr("cryptotaxcalc.app.parse_csv_with_meta", boom)

    response = client.post(
        "/upload/csv",
        files={
            "file": (
                f"preview_boom_{uuid.uuid4().hex}.csv",
                b"timestamp,type,base_asset,base_amount\n",
                "text/csv",
            )
        },
    )

    assert response.status_code == 500

    import_latest = tmp_path / "logs" / "import" / "latest_error.json"
    assert import_latest.exists()

    latest_payload = json.loads(import_latest.read_text(encoding="utf-8"))
    assert latest_payload["component"] == "import"
    assert latest_payload["exception_type"] == "RuntimeError"
    assert "preview parser exploded" in latest_payload["stacktrace"]
    assert latest_payload["context"]["endpoint"] == "/upload/csv"
    assert latest_payload["context"]["stage"] == "preview_parse"

    root_pointer = tmp_path / "logs" / "latest_error_location.txt"
    assert root_pointer.exists()
    txt = root_pointer.read_text(encoding="utf-8")
    assert "component=import" in txt
    assert "component_log_dir=" in txt
    assert "latest_error_json=" in txt
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from cryptotaxcalc.app import app
from cryptotaxcalc import admin_ops, security

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _request(host: str = "127.0.0.1") -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/bundle",
            "headers": [],
            "client": (host, 54321),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


def _call_require_bundle_admin(
    *,
    x_admin_token: str | None = None,
    x_token: str | None = None,
    authorization: str | None = None,
    query_token: str | None = None,
    host: str = "127.0.0.1",
) -> None:
    security.require_bundle_admin(
        request=_request(host=host),
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        token=query_token,
    )


def test_bundle_admin_dependency_is_not_discoverable_when_admin_surfaces_are_disabled(monkeypatch):
    monkeypatch.setattr(security, "ENABLE_ADMIN_ENDPOINTS", False)
    monkeypatch.setattr(security, "ENABLE_ADMIN_SCRIPTS", False)
    monkeypatch.setattr(security, "ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(security, "BUNDLE_TOKEN", "bundle-token")

    with pytest.raises(HTTPException) as raised:
        _call_require_bundle_admin(x_admin_token="bundle-token")

    assert raised.value.status_code == 404
    assert raised.value.detail == "Not found"


def test_bundle_admin_dependency_fails_closed_when_enabled_without_any_configured_token(monkeypatch):
    monkeypatch.setattr(security, "ENABLE_ADMIN_ENDPOINTS", True)
    monkeypatch.setattr(security, "ENABLE_ADMIN_SCRIPTS", True)
    monkeypatch.setattr(security, "ADMIN_ALLOW_REMOTE", False)
    monkeypatch.setattr(security, "ADMIN_TOKEN", "")
    monkeypatch.setattr(security, "BUNDLE_TOKEN", "")

    with pytest.raises(HTTPException) as raised:
        _call_require_bundle_admin(x_admin_token="anything")

    assert raised.value.status_code == 500
    assert raised.value.detail == "Admin token is not configured"


def test_bundle_admin_dependency_rejects_query_tokens_by_default_and_accepts_header_or_bearer(monkeypatch):
    monkeypatch.setattr(security, "ENABLE_ADMIN_ENDPOINTS", True)
    monkeypatch.setattr(security, "ENABLE_ADMIN_SCRIPTS", True)
    monkeypatch.setattr(security, "ADMIN_ALLOW_REMOTE", False)
    monkeypatch.setattr(security, "ADMIN_HEADER_ONLY", True)
    monkeypatch.setattr(security, "ALLOW_QUERY_TOKENS", False)
    monkeypatch.setattr(security, "ADMIN_TOKEN", "admin-token")
    monkeypatch.setattr(security, "BUNDLE_TOKEN", "bundle-token")

    with pytest.raises(HTTPException) as raised:
        _call_require_bundle_admin(query_token="bundle-token")
    assert raised.value.status_code == 401
    assert raised.value.detail == "Unauthorized"

    with pytest.raises(HTTPException) as raised:
        _call_require_bundle_admin(x_admin_token="wrong-token")
    assert raised.value.status_code == 401
    assert raised.value.detail == "Unauthorized"

    _call_require_bundle_admin(x_admin_token="bundle-token")
    _call_require_bundle_admin(x_admin_token="admin-token")
    _call_require_bundle_admin(authorization="Bearer bundle-token")


def test_admin_bundle_endpoint_is_not_publicly_successful_in_default_smoke_environment():
    response = client.post("/admin/bundle")

    assert response.status_code in {401, 404}
    assert response.status_code != 200


def test_create_support_bundle_success_response_reports_zip_without_exposing_tokens(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    automation_dir = project_root / "automation"
    support_dir = project_root / "support_bundles"
    automation_dir.mkdir(parents=True)
    support_dir.mkdir()
    script = automation_dir / "collect_support_bundle.py"
    script.write_text("# dummy script for admin endpoint smoke test\n", encoding="utf-8")
    zip_path = support_dir / "support_bundle_test.zip"
    zip_path.write_bytes(b"PK\x05\x06" + b"\x00" * 18)

    monkeypatch.setattr(admin_ops, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(admin_ops, "SUPPORT_BUNDLES_DIR", support_dir)
    monkeypatch.setenv("BUNDLE_TOKEN", "super-secret-bundle-token")

    def fake_run(*args, **kwargs):
        return SimpleNamespace(
            returncode=0,
            stdout=f"bundle created\n::zip::{zip_path}\n",
            stderr="",
        )

    monkeypatch.setattr(admin_ops.subprocess, "run", fake_run)

    payload = admin_ops.create_support_bundle(request=_request(), _admin=None)

    assert payload["status"] == "ok"
    assert payload["zip_path"] == str(zip_path)
    assert payload["zip_exists"] is True
    assert payload["return_code"] == 0
    assert str(script) == payload["script"]
    assert "super-secret-bundle-token" not in json.dumps(payload)


def test_create_support_bundle_failure_response_includes_structured_diagnostics(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    automation_dir = project_root / "automation"
    support_dir = project_root / "support_bundles"
    latest_bundle = support_dir / "bundle_failed"
    meta_dir = latest_bundle / "_meta"
    automation_dir.mkdir(parents=True)
    meta_dir.mkdir(parents=True)
    script = automation_dir / "collect_support_bundle.py"
    script.write_text("# dummy script for admin endpoint smoke test\n", encoding="utf-8")
    (meta_dir / "states.log").write_text("STATE preflight\nSTATE fatal\n", encoding="utf-8")
    (meta_dir / "fatal_error.txt").write_text("fatal detail", encoding="utf-8")
    (meta_dir / "zip_error.txt").write_text("zip detail", encoding="utf-8")

    monkeypatch.setattr(admin_ops, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(admin_ops, "SUPPORT_BUNDLES_DIR", support_dir)

    def fake_run(*args, **kwargs):
        return SimpleNamespace(
            returncode=2,
            stdout="collector stdout",
            stderr="collector stderr",
        )

    monkeypatch.setattr(admin_ops.subprocess, "run", fake_run)

    response = admin_ops.create_support_bundle(request=_request(), _admin=None)
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 500
    assert body["status"] == "error"
    assert body["message"] == "Script failed"
    assert body["zip_exists"] is False
    assert body["return_code"] == 2
    assert body["stdout"] == "collector stdout"
    assert body["stderr"] == "collector stderr"
    assert body["diag"]["states"] == "STATE preflight\nSTATE fatal\n"
    assert body["diag"]["fatal_error"] == "fatal detail"
    assert body["diag"]["zip_error"] == "zip detail"
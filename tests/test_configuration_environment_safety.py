from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.requests import Request

pytestmark = pytest.mark.smoke

SECURITY_ENV_KEYS = [
    "CTC_ENV",
    "ENVIRONMENT",
    "ENABLE_ADMIN_ENDPOINTS",
    "ENABLE_ADMIN_SCRIPTS",
    "ALLOW_QUERY_TOKENS",
    "ADMIN_HEADER_ONLY",
    "ADMIN_ALLOW_REMOTE",
    "ADMIN_TOKEN",
    "BUNDLE_TOKEN",
    "MAX_PREVIEW_BYTES",
    "MAX_UPLOAD_BYTES",
]


def _reload_security(monkeypatch: pytest.MonkeyPatch, env: dict[str, str | None]):
    for key in SECURITY_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)

    import cryptotaxcalc.security as security

    return importlib.reload(security)


def _request_from(host: str = "127.0.0.1") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/admin/test",
            "headers": [],
            "client": (host, 12345),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


def test_security_defaults_are_fail_closed_when_environment_is_empty(monkeypatch):
    security = _reload_security(monkeypatch, {})

    assert security.CTC_ENV == "development"
    assert security.IS_PROD is False
    assert security.ENABLE_ADMIN_ENDPOINTS is False
    assert security.ENABLE_ADMIN_SCRIPTS is False
    assert security.ALLOW_QUERY_TOKENS is False
    assert security.ADMIN_HEADER_ONLY is True
    assert security.ADMIN_ALLOW_REMOTE is False
    assert security.ADMIN_TOKEN == ""
    assert security.BUNDLE_TOKEN == ""
    assert security.MAX_PREVIEW_BYTES == 5 * 1024 * 1024
    assert security.MAX_UPLOAD_BYTES == 50 * 1024 * 1024
    assert security.MAX_PREVIEW_BYTES < security.MAX_UPLOAD_BYTES

    with pytest.raises(HTTPException) as raised:
        security.require_admin(_request_from("127.0.0.1"))

    assert raised.value.status_code == 404
    assert raised.value.detail == "Not found"


def test_admin_auth_accepts_local_header_token_but_blocks_remote_by_default(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "ENABLE_ADMIN_ENDPOINTS": "1",
            "ENABLE_ADMIN_SCRIPTS": "1",
            "ADMIN_TOKEN": "secret-token",
        },
    )

    assert security.ADMIN_ALLOW_REMOTE is False

    assert security.require_admin(
        _request_from("127.0.0.1"),
        x_admin_token="secret-token",
        x_token=None,
        authorization=None,
        token=None,
    ) is None

    with pytest.raises(HTTPException) as raised:
        security.require_admin(
            _request_from("203.0.113.10"),
            x_admin_token="secret-token",
            x_token=None,
            authorization=None,
            token=None,
        )

    assert raised.value.status_code == 404
    assert raised.value.detail == "Not found"


def test_production_never_accepts_query_string_admin_tokens_even_when_flags_are_misconfigured(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "CTC_ENV": "production",
            "ENABLE_ADMIN_ENDPOINTS": "1",
            "ALLOW_QUERY_TOKENS": "1",
            "ADMIN_HEADER_ONLY": "0",
            "ADMIN_ALLOW_REMOTE": "1",
            "ADMIN_TOKEN": "secret-token",
        },
    )

    assert security.IS_PROD is True
    assert (
        security._resolve_supplied_token(
            x_admin_token=None,
            x_token=None,
            authorization=None,
            query_token="secret-token",
        )
        == ""
    )
    assert (
        security._resolve_supplied_token(
            x_admin_token="secret-token",
            x_token=None,
            authorization=None,
            query_token="wrong-place",
        )
        == "secret-token"
    )
    assert (
        security._resolve_supplied_token(
            x_admin_token=None,
            x_token=None,
            authorization="Bearer secret-token",
            query_token="wrong-place",
        )
        == "secret-token"
    )


def test_runtime_paths_resolve_to_project_and_resource_roots_without_temp_or_home_leakage():
    from cryptotaxcalc.runtime_paths import (
        AUTOMATION,
        GIT_SCRIPT,
        LOG_DIR,
        PROJECT_ROOT,
        RESOURCE_ROOT,
        SUPPORT_BUNDLES_DIR,
    )

    assert PROJECT_ROOT.is_absolute()
    assert RESOURCE_ROOT.is_absolute()
    assert (PROJECT_ROOT / "src" / "cryptotaxcalc").exists()
    assert (RESOURCE_ROOT / "templates").exists()
    assert (RESOURCE_ROOT / "static").exists()

    assert AUTOMATION == RESOURCE_ROOT / "automation"
    assert GIT_SCRIPT == PROJECT_ROOT / "automation" / "git_auto_push.ps1"
    assert LOG_DIR == PROJECT_ROOT / "automation" / "logs"
    assert SUPPORT_BUNDLES_DIR == PROJECT_ROOT / "support_bundles"

    assert "pytest-" not in str(PROJECT_ROOT)
    assert "pytest-" not in str(RESOURCE_ROOT)


def test_all_template_response_calls_use_request_first_signature():
    """
    CI uses a Starlette/FastAPI version where old calls like:

        templates.TemplateResponse("page.html", {"request": request})

    can crash with TypeError: unhashable type: 'dict'.

    This static guard prevents local/CI drift by requiring request-first calls.
    """
    project_root = Path(__file__).resolve().parents[1]
    source_root = project_root / "src" / "cryptotaxcalc"

    violations: list[str] = []

    for path in sorted(source_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            func = node.func
            if not isinstance(func, ast.Attribute):
                continue

            if func.attr != "TemplateResponse":
                continue

            if not node.args:
                violations.append(f"{path.relative_to(project_root)}:{node.lineno} has no positional request argument")
                continue

            first_arg = node.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                violations.append(
                    f"{path.relative_to(project_root)}:{node.lineno} uses old template-name-first TemplateResponse signature"
                )
            elif isinstance(first_arg, ast.Dict):
                violations.append(
                    f"{path.relative_to(project_root)}:{node.lineno} passes context dict as first TemplateResponse argument"
                )

    assert violations == []
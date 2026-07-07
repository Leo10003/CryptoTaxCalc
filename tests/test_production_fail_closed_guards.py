from __future__ import annotations

import importlib

import pytest
from fastapi import HTTPException
from starlette.requests import Request

pytestmark = pytest.mark.smoke

SECURITY_ENV_KEYS = (
    "CTC_ENV",
    "ENVIRONMENT",
    "DEBUG",
    "APP_DEBUG",
    "ENABLE_ADMIN_ENDPOINTS",
    "ENABLE_ADMIN_SCRIPTS",
    "ALLOW_QUERY_TOKENS",
    "ADMIN_HEADER_ONLY",
    "ADMIN_ALLOW_REMOTE",
    "ADMIN_TOKEN",
    "BUNDLE_TOKEN",
    "EXPORT_AUTO",
    "AUTO_EXPORT",
)


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


def test_production_defaults_keep_admin_disabled_without_token(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "CTC_ENV": "production",
            "ENABLE_ADMIN_ENDPOINTS": "0",
            "ENABLE_ADMIN_SCRIPTS": "0",
            "ADMIN_TOKEN": "",
        },
    )

    assert security.IS_PROD is True
    assert security.ENABLE_ADMIN_ENDPOINTS is False
    assert security.ENABLE_ADMIN_SCRIPTS is False
    assert security.ADMIN_TOKEN == ""

    with pytest.raises(HTTPException) as raised:
        security.require_admin(_request_from("127.0.0.1"))

    assert raised.value.status_code == 404
    assert raised.value.detail == "Not found"


def test_production_ignores_query_string_tokens_even_when_query_tokens_flag_is_set(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "CTC_ENV": "production",
            "ENABLE_ADMIN_ENDPOINTS": "1",
            "ENABLE_ADMIN_SCRIPTS": "1",
            "ADMIN_TOKEN": "prod-secret",
            "ALLOW_QUERY_TOKENS": "1",
            "ADMIN_HEADER_ONLY": "0",
            "ADMIN_ALLOW_REMOTE": "1",
        },
    )

    assert security.IS_PROD is True
    assert security.ALLOW_QUERY_TOKENS is True
    assert security.ADMIN_HEADER_ONLY is False

    assert (
        security._resolve_supplied_token(
            x_admin_token=None,
            x_token=None,
            authorization=None,
            query_token="prod-secret",
        )
        == ""
    )

    with pytest.raises(HTTPException) as raised:
        security.require_admin(
            _request_from("127.0.0.1"),
            x_admin_token=None,
            x_token=None,
            authorization=None,
            token="prod-secret",
        )

    assert raised.value.status_code == 403


def test_production_accepts_header_or_bearer_token_when_admin_is_explicitly_enabled(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "CTC_ENV": "production",
            "ENABLE_ADMIN_ENDPOINTS": "1",
            "ENABLE_ADMIN_SCRIPTS": "1",
            "ADMIN_ALLOW_REMOTE": "1",
            "ADMIN_TOKEN": "prod-secret",
        },
    )

    assert security.require_admin(
        _request_from("203.0.113.10"),
        x_admin_token="prod-secret",
        x_token=None,
        authorization=None,
        token=None,
    ) is None

    assert security.require_admin(
        _request_from("203.0.113.10"),
        x_admin_token=None,
        x_token=None,
        authorization="Bearer prod-secret",
        token=None,
    ) is None


def test_production_blocks_remote_admin_when_remote_access_is_not_explicitly_enabled(monkeypatch):
    security = _reload_security(
        monkeypatch,
        {
            "CTC_ENV": "production",
            "ENABLE_ADMIN_ENDPOINTS": "1",
            "ENABLE_ADMIN_SCRIPTS": "1",
            "ADMIN_ALLOW_REMOTE": "0",
            "ADMIN_TOKEN": "prod-secret",
        },
    )

    with pytest.raises(HTTPException) as raised:
        security.require_admin(
            _request_from("203.0.113.10"),
            x_admin_token="prod-secret",
            x_token=None,
            authorization=None,
            token=None,
        )

    assert raised.value.status_code == 404
    assert raised.value.detail == "Not found"


def test_production_like_environment_does_not_enable_auto_export_by_default(monkeypatch):
    for key in SECURITY_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("CTC_ENV", "production")

    import cryptotaxcalc.app as app_module

    reloaded = importlib.reload(app_module)

    assert getattr(reloaded, "auto_export", False) is False
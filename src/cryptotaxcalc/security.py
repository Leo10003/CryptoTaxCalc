from __future__ import annotations

import os
import hmac
from fastapi import Header, HTTPException, Query, Request


def _truthy_env(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


# Runtime environment mode (used to harden dangerous endpoints in production).
CTC_ENV = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
IS_PROD = CTC_ENV in {"prod", "production"}


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or not val.strip():
        return default
    return _truthy_env(val)


# Defaults: admin endpoints are always opt-in (safer). Scripts + query tokens are always opt-in.
ENABLE_ADMIN_ENDPOINTS = _env_bool("ENABLE_ADMIN_ENDPOINTS", default=False)
ENABLE_ADMIN_SCRIPTS = _env_bool("ENABLE_ADMIN_SCRIPTS", default=False)
ALLOW_QUERY_TOKENS = _env_bool("ALLOW_QUERY_TOKENS", default=False)

# Auth posture defaults (safe everywhere; override explicitly only if you truly need it).
ADMIN_HEADER_ONLY = _env_bool("ADMIN_HEADER_ONLY", default=True)
ADMIN_ALLOW_REMOTE = _env_bool("ADMIN_ALLOW_REMOTE", default=False)

# Admin token (must be set explicitly whenever admin endpoints are enabled).
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()
BUNDLE_TOKEN = (os.getenv("BUNDLE_TOKEN") or "").strip()

# Upload safety caps (prevent preview OOM and accidental huge uploads).
MAX_PREVIEW_BYTES = int(os.getenv("MAX_PREVIEW_BYTES") or str(5 * 1024 * 1024))    # 5MB
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES") or str(50 * 1024 * 1024))     # 50MB


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.strip().split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def _admin_not_found() -> None:
    # 404 reduces endpoint discovery in production
    raise HTTPException(status_code=404, detail="Not found")


_LOCALHOSTS = {"127.0.0.1", "::1"}


def _is_local_admin_request(request: Request) -> bool:
    """Return True if the request originates from localhost."""
    try:
        host = request.client.host if request.client else ""
    except Exception:
        host = ""
    return host in _LOCALHOSTS


def _resolve_supplied_token(
    *,
    x_admin_token: str | None,
    x_token: str | None,
    authorization: str | None,
    query_token: str | None,
) -> str:
    bearer = _extract_bearer_token(authorization)
    if bearer:
        return bearer
    if x_admin_token:
        return x_admin_token
    if x_token:
        return x_token

    # Legacy support (off by default): query-string tokens leak via browser history and server logs.
    # Never allow in production; allow only when explicitly enabled (and header-only mode is disabled).
    if (not IS_PROD) and (not ADMIN_HEADER_ONLY) and query_token and ALLOW_QUERY_TOKENS:
        return query_token

    return ""


def require_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use Authorization or X-Admin-Token header"),
) -> None:
    if not ENABLE_ADMIN_ENDPOINTS:
        _admin_not_found()

    # Default posture: admin surfaces are localhost-only unless explicitly allowed.
    if not ADMIN_ALLOW_REMOTE and not _is_local_admin_request(request):
        _admin_not_found()

    supplied = _resolve_supplied_token(
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        query_token=token,
    )
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not ADMIN_TOKEN:
        # Fail closed if admin endpoints are enabled but no token is configured.
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN is not configured")

    if not hmac.compare_digest(supplied, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin_scripts(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use Authorization or X-Admin-Token header"),
) -> None:
    require_admin(
        request=request,
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        token=token,
    )
    if not ENABLE_ADMIN_SCRIPTS:
        _admin_not_found()


def require_bundle_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    x_token: str | None = Header(default=None, alias="X-Token"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    token: str | None = Query(default=None, description="Deprecated: use Authorization or X-Admin-Token header"),
) -> None:
    if not ENABLE_ADMIN_ENDPOINTS:
        _admin_not_found()
    if not ENABLE_ADMIN_SCRIPTS:
        _admin_not_found()

    if not ADMIN_ALLOW_REMOTE and not _is_local_admin_request(request):
        _admin_not_found()

    supplied = _resolve_supplied_token(
        x_admin_token=x_admin_token,
        x_token=x_token,
        authorization=authorization,
        query_token=token,
    )
    if not supplied:
        raise HTTPException(status_code=401, detail="Unauthorized")

    allowed = [t for t in (ADMIN_TOKEN, BUNDLE_TOKEN) if t]
    if not allowed:
        raise HTTPException(status_code=500, detail="Admin token is not configured")

    if not any(hmac.compare_digest(supplied, t) for t in allowed):
        raise HTTPException(status_code=401, detail="Unauthorized")


__all__ = [
    "_truthy_env",
    "CTC_ENV",
    "IS_PROD",
    "ENABLE_ADMIN_ENDPOINTS",
    "ENABLE_ADMIN_SCRIPTS",
    "ALLOW_QUERY_TOKENS",
    "ADMIN_HEADER_ONLY",
    "ADMIN_ALLOW_REMOTE",
    "ADMIN_TOKEN",
    "BUNDLE_TOKEN",
    "MAX_PREVIEW_BYTES",
    "MAX_UPLOAD_BYTES",
    "require_admin",
    "require_admin_scripts",
    "require_bundle_admin",
    "_admin_not_found",
]

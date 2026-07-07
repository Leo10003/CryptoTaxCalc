from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ..runtime_paths import RESOURCE_ROOT
from ..security import IS_PROD, _admin_not_found, require_admin_scripts
from ..csv_source_registry import list_unsupported_signatures, remove_unsupported_signature


templates = Jinja2Templates(directory=str((RESOURCE_ROOT / "templates").resolve()))
router = APIRouter(tags=["admin"])


@router.get("/admin/csv/unsupported", tags=["admin"])
def admin_csv_unsupported(
    limit: int = Query(200, ge=1, le=2000),
    _admin: None = Depends(require_admin_scripts),
) -> Dict[str, Any]:
    if IS_PROD:
        _admin_not_found()

    items = list_unsupported_signatures(limit=limit)
    return {"items": items, "limit": int(limit)}


@router.get(
    "/admin/csv/unsupported/ui",
    response_class=HTMLResponse,
    include_in_schema=False,
    tags=["admin"],
)
def admin_csv_unsupported_ui(
    request: Request,
    limit: int = Query(200, ge=1, le=2000),
    _admin: None = Depends(require_admin_scripts),
) -> HTMLResponse:
    if IS_PROD:
        _admin_not_found()

    items = list_unsupported_signatures(limit=limit)

    # Do NOT pass token from query params (keeps secrets out of URLs).
    token = ""

    return templates.TemplateResponse(
        request,
        "admin_csv_unsupported.html",
        {
            "items": items,
            "limit": int(limit),
            "token": token,
        },
    )


class AdminRemoveUnsupportedSignatureRequest(BaseModel):
    signature: str


@router.post("/admin/csv/unsupported/remove", tags=["admin"])
def admin_csv_unsupported_remove(
    req: AdminRemoveUnsupportedSignatureRequest,
    _admin: None = Depends(require_admin_scripts),
) -> Dict[str, Any]:
    if IS_PROD:
        _admin_not_found()

    signature = (req.signature or "").strip()
    if not signature:
        raise HTTPException(status_code=400, detail="Missing signature")

    removed = remove_unsupported_signature(signature)
    return {"removed": bool(removed), "signature": signature}

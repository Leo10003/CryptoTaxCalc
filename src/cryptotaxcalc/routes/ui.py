from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from ..csv_source_registry import list_supported_sources_catalog
from ..runtime_paths import PROJECT_ROOT, RESOURCE_ROOT


router = APIRouter(tags=["ui"])

templates = Jinja2Templates(directory=str((RESOURCE_ROOT / "templates").resolve()))
STATIC_DIR = (RESOURCE_ROOT / "static").resolve()


@router.get("/csv/formats", response_class=HTMLResponse, include_in_schema=False)
def csv_formats_page(request: Request):
    """
    User-facing catalog of supported CSV formats (required headers, optional headers, filename hints).
    """
    try:
        sources = list_supported_sources_catalog()
    except Exception:
        sources = []
    return templates.TemplateResponse(
        "csv_formats.html",
        {
            "request": request,
            "sources": sources,
        },
    )


@router.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """
    Serve a favicon if available under /static or project root.
    """
    for candidate in [
        PROJECT_ROOT / "favicon.ico",
        STATIC_DIR / "favicon.ico",
        PROJECT_ROOT / "logo" / "favicon.ico",
    ]:
        if candidate.exists():
            return FileResponse(candidate, media_type="image/x-icon")
    raise HTTPException(status_code=404, detail="favicon not found")


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
def landing_page(request: Request):
    return templates.TemplateResponse(request, "landing.html", {"request": request})


@router.get("/workspace", response_class=HTMLResponse, include_in_schema=False)
def workspace_page(request: Request):
    """
    Main workspace for real users (non-demo).
    Injects user_display_name for personalization in the hero.
    """
    user_display_name = None

    if hasattr(request.state, "user") and request.state.user:
        u = request.state.user
        if hasattr(u, "full_name") and u.full_name:
            user_display_name = u.full_name.strip()
        elif hasattr(u, "email") and u.email:
            user_display_name = u.email.split("@")[0]

    if user_display_name is None:
        user_display_name = "User"

    return templates.TemplateResponse(
        "workspace.html",
        {
            "request": request,
            "user_display_name": user_display_name,
        },
    )


@router.get("/workspace/results", response_class=HTMLResponse, include_in_schema=False)
def workspace_results_page(
    request: Request,
    run_id: int | None = Query(None, description="Calc run DB id (from /calculate/v2)"),
):
    user_display_name = None

    if hasattr(request.state, "user") and request.state.user:
        u = request.state.user
        if hasattr(u, "full_name") and u.full_name:
            user_display_name = u.full_name.strip()
        elif hasattr(u, "email") and u.email:
            user_display_name = u.email.split("@")[0]

    if user_display_name is None:
        user_display_name = "User"

    return templates.TemplateResponse(
        "workspace_results.html",
        {
            "request": request,
            "user_display_name": user_display_name,
            "run_id": run_id,
        },
    )

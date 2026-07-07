from __future__ import annotations

import csv as _csv
import io
import json
import tempfile
import zipfile
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from sqlalchemy import select, text
from sqlalchemy.orm import Session as SASession

# NOTE: Import shared singletons/helpers from app.py to avoid behavior changes during this split.
from .app import (  # pylint: disable=cyclic-import
    CalcRun,
    SessionLocal,
    engine,
    get_session,
    templates,
    _build_manifest,
    _delete_calc_run,
    _list_calc_runs_meta,
    _load_calc_run,
    _resolve_db_run_id,
)

router = APIRouter(tags=["history"])


@router.get("/history", tags=["history"])
def history_index(
    request: Request,
    format: str = Query(
        "json",
        description="json for API clients and tests, html for the Recent runs page",
    ),
):
    """
    Recent runs index.

    - format=json (default): returns a plain JSON list (tests and API clients).
    - format=html: renders the Recent runs page.
    """
    items = _list_calc_runs_meta()

    if format.lower() == "html":
        return templates.TemplateResponse(
            request,
            "history.html",
            {"runs": items},
        )
    return JSONResponse(items)


@router.get("/history/{run_id}/download", summary="Download calculation run as ZIP", tags=["history"])
def history_download(run_id: str, request: Request, session: SASession = Depends(get_session)):
    debug = request.query_params.get("debug") == "1"
    with SessionLocal() as session:
        rid_int = _resolve_db_run_id(session, run_id)

    with SessionLocal() as session:
        # Get run_id row to pull started_at (preferred for manifest.created_at)
        row = session.execute(
            select(CalcRun.started_at).where(CalcRun.id == rid_int)
        ).first()
        started_at_dt = row[0] if row else None

    created_at_iso = (
        started_at_dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        if started_at_dt else
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    manifest = {
        "id": rid_int,
        "run_id": run_id,
        "created_at": created_at_iso,
        # keep whatever else you already put in your manifest
        # e.g. "files": files_list, "notes": ..., etc.
    }

    # Build a minimal bundle for this run (reusing your existing bundle builder if you have one)
    # Here we just produce a tiny ZIP with a manifest
    buf = io.BytesIO()

    # Build manifest using a fresh, open session
    with SessionLocal() as s:
        manifest = _build_manifest(s, rid_int, run_id)

    # Now write the manifest into the zip
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.writestr(
            "manifest.json",
            json.dumps(manifest, ensure_ascii=False, separators=(",", ":"))
        )

    if debug:
        return JSONResponse(content={"run_id": run_id, "id": rid_int})
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    tmp.write(buf.getvalue()); tmp.flush(); tmp.close()
    filename = f"run_{run_id}.zip"
    return FileResponse(tmp.name, media_type="application/zip", filename=filename)


@router.get("/history/run/{run_id}/download", include_in_schema=False)
def history_download_compat(
    run_id: str,
    request: Request,
    session: SASession = Depends(get_session),
):
    return history_download(run_id, request, session)


@router.get("/history/runs", response_class=JSONResponse, tags=["history"])
def history_list_runs():
    """
    List all stored calculation runs with light metadata.
    """
    return JSONResponse({"items": _list_calc_runs_meta()})


@router.get("/history/run/{run_id}", response_class=JSONResponse, tags=["history"])
def history_get_run(run_id: str):
    data = _load_calc_run(run_id)
    if not data:
        raise HTTPException(status_code=404, detail="Run not found")
    return JSONResponse(data)


@router.delete("/history/run/{run_id}", response_class=JSONResponse, tags=["history"])
def history_delete_run(run_id: str):
    ok = _delete_calc_run(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Run not found")
    return JSONResponse({"status": "deleted", "run_id": run_id})


@router.get("/history/run/{run_id}/events.csv")
def history_events_csv(run_id: str, session: SASession = Depends(get_session)):
    """
    Return realized events for a run as CSV.
    Always emit a CSV header even when there are zero rows.

    This uses the same column structure as /export/events_csv.
    """
    # Resolve external run_id → internal integer id
    try:
        rid_int = _resolve_db_run_id(session, run_id)
    except HTTPException:
        # fall back if caller passed the numeric id directly
        try:
            rid_int = int(run_id)
        except Exception:
            header = "timestamp,asset,qty_sold,proceeds_eur,cost_basis_eur,gain_eur,quote_asset,fee_applied_eur,matches_json,jurisdiction,tax_year,fx_set_id,calc_run_id,run_ref\n"
            return Response(header, media_type="text/csv; charset=utf-8")

    with engine.begin() as conn:
        # Run metadata (for audit context)
        run_meta = conn.execute(
            text(
                """
                SELECT id, jurisdiction, tax_year, fx_set_id, run_id AS run_ref
                FROM calc_runs
                WHERE id = :rid
                """
            ),
            {"rid": rid_int},
        ).mappings().first()

        # Events for this run
        rows = conn.execute(
            text(
                """
                SELECT
                    timestamp,
                    asset,
                    qty_sold,
                    proceeds,
                    cost_basis,
                    gain,
                    quote_asset,
                    fee_applied,
                    matches_json
                FROM realized_events
                WHERE run_id = :rid
                ORDER BY id
                """
            ),
            {"rid": rid_int},
        ).mappings().all()

    output = io.StringIO()
    w = _csv.writer(output)

    # Always emit header
    w.writerow([
        "timestamp",
        "asset",
        "qty_sold",
        "proceeds_eur",
        "cost_basis_eur",
        "gain_eur",
        "quote_asset",
        "fee_applied_eur",
        "matches_json",
        "jurisdiction",
        "tax_year",
        "fx_set_id",
        "calc_run_id",
        "run_ref",
    ])

    if not rows or not run_meta:
        output.seek(0)
        filename = f"realized_events_run_{rid_int}.csv"
        return StreamingResponse(
            iter([output.read()]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}\"'}
        )

    for r in rows:
        w.writerow([
            r.get("timestamp") or "",
            r.get("asset") or "",
            r.get("qty_sold") or "",
            r.get("proceeds") or "",
            r.get("cost_basis") or "",
            r.get("gain") or "",
            r.get("quote_asset") or "",
            r.get("fee_applied") or "",
            r.get("matches_json") or "",
            run_meta.get("jurisdiction") or "",
            run_meta.get("tax_year") or "",
            run_meta.get("fx_set_id") or "",
            run_meta.get("id") or rid_int,
            run_meta.get("run_ref") or "",
        ])

    output.seek(0)

    filename = f"realized_events_run_{rid_int}.csv"
    return StreamingResponse(
        iter([output.read()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    )

from __future__ import annotations

import hashlib
from csv import DictReader
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO

from fastapi import APIRouter, Depends, File, HTTPException, Query, Response, UploadFile
from sqlalchemy import text

from ..db import SessionLocal
from ..fx_utils import ensure_fx_rates_schema
from ..security import require_admin

router = APIRouter()


@router.post("/fx/upload")
async def fx_upload(file: UploadFile = File(...), _admin: None = Depends(require_admin)) -> dict:
    """
    Upload a CSV of daily EURUSD rates.
    Required headers: date, usd_per_eur
      - date format: YYYY-MM-DD
      - usd_per_eur: decimal number (USD per 1 EUR)
    We normalize and store as: base='USD', quote='EUR', rate=<EUR per 1 USD>.
    A new fx_batches row is created; imported rows get its batch_id.
    """
    # Basic validation
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    raw = await file.read()
    try:
        text_csv = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV (utf-8).")

    reader = DictReader(StringIO(text_csv))
    required = {"date", "usd_per_eur"}
    if not reader.fieldnames or required - {h.strip().lower() for h in reader.fieldnames}:
        raise HTTPException(status_code=400, detail="CSV must include headers: date, usd_per_eur")

    # Map canonical → actual header casing
    header_map = {h.strip().lower(): h for h in (reader.fieldnames or [])}

    inserted = 0
    updated = 0
    errors = 0

    now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    with SessionLocal() as session:
        # Make sure fx_rates has the columns this code relies on
        try:
            ensure_fx_rates_schema(session)
        except Exception:
            # Not fatal; continue and let the operations fail if schema is truly broken
            pass

        # Start a new batch
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": "ECB CSV", "h": None},
        ).lastrowid

        for row in reader:
            try:
                raw_date = (row.get(header_map["date"]) or "").strip()
                raw_rate = (row.get(header_map["usd_per_eur"]) or "").strip()

                if not raw_date or not raw_rate:
                    raise ValueError("Missing date or usd_per_eur")

                # Parse date
                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                d_iso = d.isoformat()

                # CSV gives USD per 1 EUR → normalize to EUR per 1 USD
                usd_per_eur = Decimal(raw_rate)  # may raise InvalidOperation
                if usd_per_eur <= 0:
                    raise ValueError("usd_per_eur must be > 0")

                rate_eur_per_usd = (Decimal("1") / usd_per_eur)

                # Upsert normalized row
                exists = session.execute(
                    text("SELECT 1 FROM fx_rates WHERE date = :d AND base='USD' AND quote='EUR'"),
                    {"d": d_iso},
                ).scalar()

                if exists:
                    session.execute(
                        text(
                            """UPDATE fx_rates
                               SET rate = :r, batch_id = :b, base='USD', quote='EUR'
                               WHERE date = :d AND base='USD' AND quote='EUR'"""
                        ),
                        {"r": str(rate_eur_per_usd), "b": bid, "d": d_iso},
                    )
                    updated += 1
                else:
                    session.execute(
                        text(
                            """INSERT INTO fx_rates (date, base, quote, rate, batch_id)
                               VALUES (:d, 'USD', 'EUR', :r, :b)"""
                        ),
                        {"d": d_iso, "r": str(rate_eur_per_usd), "b": bid},
                    )
                    inserted += 1

            except (InvalidOperation, ValueError):
                errors += 1
                continue
            except Exception:
                # Any unexpected row-level failure shouldn't kill the batch
                errors += 1
                continue

        session.commit()

        # Compute a deterministic hash of what was just imported (by date asc)
        rows_for_hash = session.execute(
            text("SELECT date, base, quote, rate FROM fx_rates WHERE batch_id = :b ORDER BY date"),
            {"b": bid},
        ).fetchall()

        h = hashlib.sha256()
        for r in rows_for_hash:
            line = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            {"rh": rates_hash, "bid": bid},
        )
        session.commit()

    return {"inserted": inserted, "updated": updated, "errors": errors, "batch_id": int(bid)}


@router.get("/prices/template.csv", summary="Download CSV template for daily price uploads", tags=["prices"])
def prices_template_csv() -> Response:
    """
    Template for /prices/upload.
    rate is QUOTE per 1 BASE (e.g., EUR per 1 BNB; USD per 1 ETH).
    """
    sample = (
        "date,base,quote,rate\n"
        "2025-01-01,BNB,EUR,250.12\n"
        "2025-01-01,ETH,USD,3450.50\n"
    )
    headers = {"Content-Disposition": 'attachment; filename="prices_template.csv"'}
    return Response(content=sample, media_type="text/csv; charset=utf-8", headers=headers)


@router.post("/prices/upload", summary="Upload daily prices for assets (used for third-asset fee valuation)", tags=["prices"])
async def prices_upload(
    file: UploadFile = File(...),
    source: str = Query("PRICE CSV", description="Label stored in fx_batches.source"),
    _admin: None = Depends(require_admin),
) -> dict:
    """
    Upload daily prices into fx_rates as base=<ASSET>, quote='EUR', rate=<EUR per 1 base>.

    CSV headers:
      - date: YYYY-MM-DD
      - base: asset symbol (e.g., BNB, ETH)
      - quote: EUR or USD-like (USD/USDT/USDC/BUSD). USD-like quotes are converted to EUR using fx_rates USD/EUR.
      - rate: quote per 1 base (e.g., EUR per 1 BNB; USD per 1 ETH)
    """
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    raw = await file.read()
    try:
        text_csv = raw.decode("utf-8-sig", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Unable to decode CSV (utf-8).")

    reader = DictReader(StringIO(text_csv))
    required = {"date", "base", "quote", "rate"}
    if not reader.fieldnames or required - {h.strip().lower() for h in reader.fieldnames}:
        raise HTTPException(status_code=400, detail="CSV must include headers: date, base, quote, rate")

    header_map = {h.strip().lower(): h for h in (reader.fieldnames or [])}

    inserted = 0
    updated = 0
    errors = 0
    fx_missing = 0
    fx_missing_days: set[str] = set()

    now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    USD_LIKE = {"USD", "USDT", "USDC", "BUSD"}

    with SessionLocal() as session:
        # Ensure fx_rates schema exists (idempotent)
        try:
            ensure_fx_rates_schema(session)
        except Exception:
            pass

        # Start a new batch for these rates/prices
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": (source or "PRICE CSV"), "h": None},
        ).lastrowid

        from datetime import timedelta

        for row in reader:
            try:
                raw_date = (row.get(header_map["date"]) or "").strip()
                raw_base = (row.get(header_map["base"]) or "").strip()
                raw_quote = (row.get(header_map["quote"]) or "").strip()
                raw_rate = (row.get(header_map["rate"]) or "").strip()

                if not raw_date or not raw_base or not raw_quote or not raw_rate:
                    raise ValueError("Missing required fields")

                d = datetime.strptime(raw_date, "%Y-%m-%d").date()
                d_iso = d.isoformat()

                base = raw_base.upper()
                quote = raw_quote.upper()

                rate_in_quote = Decimal(raw_rate)
                if rate_in_quote <= 0:
                    raise ValueError("rate must be > 0")

                # Normalize into EUR rates for downstream valuation.
                if quote == "EUR":
                    rate_eur = rate_in_quote
                elif quote in USD_LIKE:
                    # Convert quote (USD-like) -> EUR using USD->EUR rate for same day (allow up to 7-day lookback).
                    min_iso = (d - timedelta(days=7)).isoformat()
                    fx_row = session.execute(
                        text(
                            "SELECT rate FROM fx_rates "
                            "WHERE base='USD' AND quote='EUR' AND date <= :d AND date >= :min_d "
                            "ORDER BY date DESC LIMIT 1"
                        ),
                        {"d": d_iso, "min_d": min_iso},
                    ).first()
                    if not fx_row or fx_row[0] is None:
                        fx_missing += 1
                        fx_missing_days.add(d_iso)
                        raise ValueError("Missing USD->EUR FX rate for conversion")
                    eur_per_usd = Decimal(str(fx_row[0]))
                    rate_eur = (rate_in_quote * eur_per_usd)
                else:
                    raise ValueError(f"Unsupported quote '{quote}'. Use EUR or USD/USDT/USDC/BUSD.")

                # Upsert into fx_rates (base=<asset>, quote='EUR')
                exists = session.execute(
                    text("SELECT 1 FROM fx_rates WHERE date = :d AND base = :b AND quote = :q"),
                    {"d": d_iso, "b": base, "q": "EUR"},
                ).scalar()

                if exists:
                    session.execute(
                        text(
                            "UPDATE fx_rates SET rate = :r, batch_id = :bid "
                            "WHERE date = :d AND base = :b AND quote = :q"
                        ),
                        {"r": str(rate_eur), "bid": bid, "d": d_iso, "b": base, "q": "EUR"},
                    )
                    updated += 1
                else:
                    session.execute(
                        text(
                            "INSERT INTO fx_rates (date, base, quote, rate, batch_id) "
                            "VALUES (:d, :b, :q, :r, :bid)"
                        ),
                        {"d": d_iso, "b": base, "q": "EUR", "r": str(rate_eur), "bid": bid},
                    )
                    inserted += 1

            except (InvalidOperation, ValueError):
                errors += 1
                continue
            except Exception:
                errors += 1
                continue

        session.commit()

        # Hash the imported/updated rows for this batch
        rows_for_hash = session.execute(
            text("SELECT date, base, quote, rate FROM fx_rates WHERE batch_id = :b ORDER BY date, base, quote"),
            {"b": bid},
        ).fetchall()

        h = hashlib.sha256()
        for r in rows_for_hash:
            line = f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n"
            h.update(line.encode("utf-8"))
        rates_hash = h.hexdigest()

        session.execute(
            text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
            {"rh": rates_hash, "bid": bid},
        )
        session.commit()

    return {
        "inserted": inserted,
        "updated": updated,
        "errors": errors,
        "fx_missing": fx_missing,
        "fx_missing_days_count": len(fx_missing_days),
        "fx_missing_days_sample": sorted(list(fx_missing_days))[:10],
        "batch_id": int(bid),
    }

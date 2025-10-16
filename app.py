# app.py
# Tiny starter API so we can verify everything works on Windows.

from fastapi import FastAPI

app = FastAPI(title="CryptoTaxCalc", version="0.0.1")

@app.get("/health")
def health():
    """
    Health check endpoint: tells us the server is running.
    """
    return {"status": "ok"}

@app.get("/version")
def version():
    """
    Shows the backend version; useful to confirm deployments.
    """
    return {"name": "CryptoTaxCalc", "version": "0.0.1"}

from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import Dict, Any
from csv_normalizer import parse_csv

# ... your existing app, /health and /version endpoints remain unchanged ...

@app.post("/upload/csv")
async def upload_csv(file: UploadFile = File(...)) -> Dict[str, Any]:
    """
    Accepts a CSV upload, parses it into normalized Transaction objects,
    and returns a small preview and error list.
    """
    # 1) Basic validation: extension and content-type (not bulletproof, but helpful)
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    # 2) Read file bytes
    data = await file.read()
    if len(data) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    # 3) Parse and validate rows
    valid_rows, errors = parse_csv(data)

    # 4) Return a small preview so user sees what we parsed
    preview = [vr.model_dump() for vr in valid_rows[:5]]
    return {
        "filename": filename,
        "total_valid": len(valid_rows),
        "total_errors": len(errors),
        "preview_first_5": preview,
        "errors": errors[:5]  # return only the first few errors to keep response small
    }

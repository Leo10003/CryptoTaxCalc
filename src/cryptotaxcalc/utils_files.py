# utils_files.py
import hashlib, os, datetime
from pathlib import Path
from fastapi import UploadFile

STORAGE_DIR = Path("storage_raw")
STORAGE_DIR.mkdir(exist_ok=True)

def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

def persist_uploaded_file(file: UploadFile, content: bytes) -> tuple[str, str]:
    """
    Save original bytes to disk with a SHA256 filename. Returns (path, sha256).
    Safe to call multiple times; it won't overwrite if the same hash exists.
    """
    digest = sha256_bytes(content)
    datedir = STORAGE_DIR / datetime.datetime.utcnow().strftime("%Y-%m-%d")
    datedir.mkdir(parents=True, exist_ok=True)
    ext = "".join(Path(file.filename).suffixes) or ""
    path = datedir / f"{digest}{ext}"
    if not path.exists():
        path.write_bytes(content)
    return str(path), digest

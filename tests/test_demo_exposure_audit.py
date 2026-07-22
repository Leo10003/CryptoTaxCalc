from __future__ import annotations

import zipfile
from pathlib import Path

from tools.demo_exposure_audit import audit_zip, sanitize_zip


def test_demo_exposure_audit_flags_risky_files(tmp_path: Path):
    demo_zip = tmp_path / "demo.zip"

    with zipfile.ZipFile(demo_zip, "w") as zf:
        zf.writestr("Demo/build.log", "C:\\Users\\leona\\Desktop\\CryptoTaxCalc")
        zf.writestr("Demo/cryptotaxcalc.db-wal", "wal")
        zf.writestr("Demo/_internal/templates/admin_csv_unsupported.html", "<html></html>")
        zf.writestr("Demo/_internal/static/media/render_backgrounds_gpu.py", "print('exposed')")
        zf.writestr("Demo/_internal/templates/workspace_results.html", "safe visible UI")

    findings = audit_zip(demo_zip)
    reasons = "\n".join(f.reason for f in findings)

    assert "blocked exact filename" in reasons
    assert "SQLite sidecar/runtime file" in reasons
    assert "admin/operator template" in reasons
    assert "plain Python helper" in reasons


def test_demo_exposure_sanitizer_removes_risky_files(tmp_path: Path):
    demo_zip = tmp_path / "demo.zip"
    clean_zip = tmp_path / "demo_clean.zip"

    with zipfile.ZipFile(demo_zip, "w") as zf:
        zf.writestr("Demo/build.log", "local path")
        zf.writestr("Demo/cryptotaxcalc.db-shm", "shm")
        zf.writestr("Demo/_internal/templates/admin_csv_unsupported.html", "<html></html>")
        zf.writestr("Demo/_internal/static/media/render_backgrounds_gpu.py", "print('exposed')")
        zf.writestr("Demo/START_DEMO.bat", "start CryptoTaxCalc_Demo.exe")
        zf.writestr("Demo/CryptoTaxCalc_Demo.exe", b"fake exe bytes")

    report = sanitize_zip(demo_zip, clean_zip)

    assert clean_zip.exists()
    assert len(report["removed_files"]) == 4

    with zipfile.ZipFile(clean_zip, "r") as zf:
        names = set(zf.namelist())

    assert "Demo/START_DEMO.bat" in names
    assert "Demo/CryptoTaxCalc_Demo.exe" in names
    assert "Demo/build.log" not in names
    assert "Demo/cryptotaxcalc.db-shm" not in names
    assert "Demo/_internal/templates/admin_csv_unsupported.html" not in names
    assert "Demo/_internal/static/media/render_backgrounds_gpu.py" not in names

    assert audit_zip(clean_zip) == []

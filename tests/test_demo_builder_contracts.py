from __future__ import annotations

import py_compile
from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

DEMO_BUILDER = Path("src/cryptotaxcalc/demo_builder.py")


def _source() -> str:
    return DEMO_BUILDER.read_text(encoding="utf-8", errors="replace")


def test_demo_builder_is_valid_python():
    py_compile.compile(str(DEMO_BUILDER), doraise=True)


def test_demo_builder_onedir_layout_and_pyinstaller_diagnostics():
    text = _source()

    assert "PyInstaller did not create expected demo EXE folder" in text
    assert "actual_dist_entries" in text
    assert "if not dist_dir.exists()" in text
    assert "shutil.copytree(dist_dir, DIST_TMP" in text
    assert "No exe produced" in text

    assert "START_DEMO.bat" in text
    assert "CryptoTaxCalc_Demo_dist" in text
    assert "EXE_READY" in text
    assert "EXE_TMP" in text
    assert "stale_exe.unlink" in text
    assert "Running the exe without its _internal folder can fail silently" in text
    assert "OUT_DIR" not in text


def test_demo_builder_skips_missing_optional_demo_data_paths():
    text = _source()

    assert "def add_data_if_exists(src_path: Path, dest_name: str) -> None:" in text
    assert "Skipping missing demo build data path" in text
    assert 'add_data_if_exists(demo_dir, "demo")' in text

    # The old unconditional PyInstaller add-data entry broke builds when ./demo was absent.
    assert 'f"{demo_dir};demo",' not in text


def test_demo_builder_creates_investor_safe_latest_zip_after_raw_latest_zip():
    text = _source()

    assert 'ZIP_LATEST = BUNDLES / "CryptoTaxCalc_Demo_LATEST.zip"' in text
    assert 'ZIP_INVESTOR_SAFE_LATEST = BUNDLES / "CryptoTaxCalc_Demo_INVESTOR_SAFE_LATEST.zip"' in text
    assert 'INVESTOR_SAFE_REPORT = BUNDLES / "CryptoTaxCalc_Demo_INVESTOR_SAFE_report.json"' in text

    assert "def _write_investor_safe_latest_zip(source_zip: Path) -> dict:" in text
    assert "audit_mod.sanitize_zip(source_zip, ZIP_INVESTOR_SAFE_LATEST)" in text
    assert "audit_mod.audit_zip(ZIP_INVESTOR_SAFE_LATEST)" in text

    raw_copy_idx = text.index("shutil.copy2(zip_path, ZIP_LATEST)")
    safe_copy_idx = text.index("investor_safe_report = _write_investor_safe_latest_zip(ZIP_LATEST)")
    assert raw_copy_idx < safe_copy_idx

    assert '"investor_safe_zip_latest": str(ZIP_INVESTOR_SAFE_LATEST),' in text
    assert '"investor_safe_removed_files": len(' in text


def test_demo_builder_exposes_investor_safe_download_endpoint():
    text = _source()

    assert '@router.get("/download_investor_zip", include_in_schema=True)' in text
    assert "def download_investor_zip(_admin: None = Depends(require_demo_builder_admin)):" in text
    assert 'detail="No investor-safe bundle zip found"' in text
    assert "filename=ZIP_INVESTOR_SAFE_LATEST.name" in text


def test_demo_builder_removes_timestamped_staging_folder_after_zip_creation():
    text = _source()

    zip_marker = "with zipfile.ZipFile(zip_path"
    latest_marker = "shutil.copy2(zip_path, ZIP_LATEST)"
    cleanup_marker = "shutil.rmtree(bundle_dir, ignore_errors=True)"

    assert zip_marker in text
    assert latest_marker in text

    zip_idx = text.index(zip_marker)
    latest_idx = text.index(latest_marker)
    cleanup_idx = text.find(cleanup_marker, zip_idx, latest_idx)

    assert cleanup_idx != -1, "bundle_dir cleanup must happen after zip creation and before latest ZIP publication"
    assert zip_idx < cleanup_idx < latest_idx

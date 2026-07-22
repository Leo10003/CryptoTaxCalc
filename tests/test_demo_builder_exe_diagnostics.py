from __future__ import annotations

from pathlib import Path


def test_demo_builder_checks_pyinstaller_dist_folder_before_copy():
    text = Path("src/cryptotaxcalc/demo_builder.py").read_text(encoding="utf-8", errors="replace")

    assert "PyInstaller did not create expected demo EXE folder" in text
    assert "actual_dist_entries" in text
    assert "if not dist_dir.exists()" in text
    assert "shutil.copytree(dist_dir, DIST_TMP" in text
    assert "No exe produced" in text

from __future__ import annotations

from pathlib import Path


def test_demo_builder_does_not_ship_root_level_exe_copies():
    text = Path("src/cryptotaxcalc/demo_builder.py").read_text(encoding="utf-8", errors="replace")

    assert "START_DEMO.bat" in text
    assert "CryptoTaxCalc_Demo_dist" in text
    assert "EXE_READY" in text
    assert "EXE_TMP" in text
    assert "stale_exe.unlink" in text
    assert "Running the exe without its _internal folder can fail silently" in text
    assert "OUT_DIR" not in text

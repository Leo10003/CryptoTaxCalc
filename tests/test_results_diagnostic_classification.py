from __future__ import annotations

from pathlib import Path


def test_results_diagnostics_classify_harmless_fx_checks_as_info():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "PRICE AUTOSYNC DIAG" in html
    assert "ERRORS=0" in html
    assert "Autosync completed without errors." in html

    assert "FX CHECK:" in html
    assert "ROWS IN FX_RATES" in html
    assert "FX coverage exists in the local rate table." in html

    assert "FX coverage needs review before exporting." in html

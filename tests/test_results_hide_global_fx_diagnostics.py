from __future__ import annotations

from pathlib import Path


def test_results_hides_harmless_global_fx_table_diagnostics():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "function shouldShowDiagnostic(w)" in html
    assert "FX CHECK:" in html
    assert "ROWS IN FX_RATES" in html
    assert ".filter(shouldShowDiagnostic)" in html

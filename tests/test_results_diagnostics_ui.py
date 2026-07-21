from __future__ import annotations

from pathlib import Path


TEMPLATE = Path("templates/workspace_results.html")


def _html() -> str:
    return TEMPLATE.read_text(encoding="utf-8", errors="replace")


def test_results_diagnostics_classify_harmless_items_as_info():
    html = _html()

    assert "PRICE AUTOSYNC DIAG" in html
    assert "ERRORS=0" in html
    assert "Autosync completed without errors." in html

    assert "FX CHECK:" in html
    assert "ROWS IN FX_RATES" in html
    assert "FX coverage exists in the local rate table." in html

    assert "FX coverage needs review before exporting." in html


def test_results_diagnostics_hide_harmless_global_fx_table_checks():
    html = _html()

    assert "function shouldShowDiagnostic(w)" in html
    assert "FX CHECK:" in html
    assert "ROWS IN FX_RATES" in html
    assert ".filter(shouldShowDiagnostic)" in html


def test_results_warning_counts_ignore_info_diagnostics():
    html = _html()

    assert "function warningReviewItems(rawWarnings)" in html
    assert "function warningReviewCount(rawWarnings)" in html
    assert "rawWarnings.filter(warningRequiresExportConfirmation)" in html
    assert "warningReviewCount(currentWarnings)" in html

    assert "Array.isArray(currentWarnings) ? currentWarnings.filter(Boolean).length : 0" not in html


def test_export_confirmation_ignores_info_only_diagnostics():
    html = _html()

    assert "function warningRequiresExportConfirmation(w)" in html
    assert "classified.level === 'warn' || classified.level === 'action'" in html
    assert ".filter(warningRequiresExportConfirmation)" in html
    assert "This soft pre-export prompt is only for non-blocking warning cases." in html
    assert "warning/info cases" not in html


def test_overview_warning_cards_use_warning_review_items_not_info_diagnostics():
    html = _html()

    assert "function warningReviewItems(rawWarnings)" in html
    assert "function warningReviewCount(rawWarnings)" in html
    assert "warningReviewCount(currentWarnings)" in html
    assert "warningReviewItems(currentWarnings)" in html

    # Info tab should still show visible info diagnostics.
    assert "filter(shouldShowDiagnostic)" in html

    # Old raw count must not drive overview warning totals.
    assert "Array.isArray(currentWarnings) ? currentWarnings.filter(Boolean).length : 0" not in html

from __future__ import annotations

from pathlib import Path


TEMPLATE = Path("templates/workspace_results.html")


def _html() -> str:
    return TEMPLATE.read_text(encoding="utf-8", errors="replace")


def _section_between(html: str, start: str, end: str) -> str:
    a = html.find(start)
    assert a != -1, f"Missing start marker: {start}"
    b = html.find(end, a)
    assert b != -1, f"Missing end marker after {start}: {end}"
    return html[a:b]


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


def test_overview_data_quality_uses_warning_only_items():
    html = _html()

    body = _section_between(
        html,
        "function setDataQualityStatus(warnings)",
        "function setRunMeta(text)",
    )

    assert "warningReviewItems(warnings).map(w => String(w))" in body
    assert "prioritizeWarnings(warnings)" not in body
    assert "WARN ${list.length}" in body


def test_info_tab_still_shows_visible_info_diagnostics():
    html = _html()

    body = _section_between(
        html,
        "function renderWarningsList()",
        "const feeValWarnCount = feeValIncompleteCount;",
    )

    assert "currentWarnings.filter(Boolean).map(w => String(w)).filter(shouldShowDiagnostic)" in body
    assert "warningReviewItems(currentWarnings)" not in body
    assert "renderWarningsSummary(raw)" in body

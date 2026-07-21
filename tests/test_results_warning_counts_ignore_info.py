from __future__ import annotations

from pathlib import Path


def test_results_warning_counts_ignore_info_diagnostics():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "function warningReviewItems(rawWarnings)" in html
    assert "function warningReviewCount(rawWarnings)" in html
    assert "rawWarnings.filter(warningRequiresExportConfirmation)" in html
    assert "warningReviewCount(currentWarnings)" in html

    assert "Array.isArray(currentWarnings) ? currentWarnings.filter(Boolean).length : 0" not in html

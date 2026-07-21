from __future__ import annotations

from pathlib import Path


def test_export_confirmation_ignores_info_only_diagnostics():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "function warningRequiresExportConfirmation(w)" in html
    assert "classified.level === 'warn' || classified.level === 'action'" in html

    assert ".filter(warningRequiresExportConfirmation)" in html
    assert "This soft pre-export prompt is only for non-blocking warning cases." in html
    assert "warning/info cases" not in html

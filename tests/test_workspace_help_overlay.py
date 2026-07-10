from __future__ import annotations

from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_workspace_help_button_has_support_page_fallback():
    template_path = RESOURCE_ROOT / "templates" / "workspace.html"

    assert template_path.exists()

    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert 'href="/support/report-issue"' in html
    assert 'data-help-open="true"' in html


def test_workspace_results_help_button_has_overlay_and_fallback():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    assert template_path.exists()

    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert 'href="/support/report-issue"' in html
    assert 'data-help-open="true"' in html
    assert 'id="rsHelpOverlay"' in html
    assert 'id="rsHelpCreateReport"' in html
    assert 'id="rsHelpRefreshHistory"' not in html
    assert 'id="rsHelpHistory"' not in html
    assert "/support/report-issue/history" not in html
    assert "Support token" not in html
    assert "rsHelpToken" not in html
    assert "/support/report-issue/history?limit=5" not in html
    assert "workspace_results_help_overlay" in html
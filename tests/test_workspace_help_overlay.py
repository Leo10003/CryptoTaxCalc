from __future__ import annotations

from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_workspace_results_help_button_opens_support_overlay():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    assert template_path.exists()

    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert 'data-help-open="true"' in html
    assert 'id="rsHelpOverlay"' in html
    assert 'id="rsHelpCreateReport"' in html
    assert 'id="rsHelpRefreshHistory"' in html
    assert "/support/report-issue" in html
    assert "/support/report-issue/history?limit=5" in html
    assert "workspace_results_help_overlay" in html
from __future__ import annotations

from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_results_help_overlay_is_client_safe():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert 'id="rsHelpOverlay"' in html
    assert 'id="rsHelpCreateReport"' in html
    assert "/support/report-issue/client" in html
    assert "Create support file" in html

    assert "Support token" not in html
    assert "rsHelpToken" not in html
    assert "X-Admin-Token" not in html
    assert "Recent reports" not in html
    assert "rsHelpRefreshHistory" not in html
    assert "rsHelpHistory" not in html
    assert "/support/report-issue/history" not in html

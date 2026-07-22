from __future__ import annotations

from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_workspace_renders_import_guidance_fields():
    template_path = RESOURCE_ROOT / "templates" / "workspace.html"
    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert "renderCsvImportDiagnostics(importResults)" in html
    assert "user_guidance" in html
    assert "user_title" in html
    assert "import_error_kind" in html
    assert "failed_filename" in html
    assert "support_page_url" in html
    assert "Create support report" in html

    assert "ws-import-guidance-card" in html
    assert "ws-import-guidance-details" in html
    assert "CSV import needs attention" in html


def test_workspace_import_guidance_does_not_reintroduce_admin_ui():
    template_path = RESOURCE_ROOT / "templates" / "workspace.html"
    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert "Support token" not in html
    assert "X-Admin-Token" not in html
    assert "/support/report-issue/history" not in html

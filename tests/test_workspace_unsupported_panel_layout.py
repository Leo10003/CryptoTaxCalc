from __future__ import annotations

from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


def test_workspace_unsupported_csv_panel_is_viewport_safe():
    template_path = RESOURCE_ROOT / "templates" / "workspace.html"
    html = template_path.read_text(encoding="utf-8", errors="replace")

    assert "CTC_PATCH: compact unsupported CSV panel" in html
    assert "#wizardUnsupportedPanel" in html
    assert "max-height: clamp(180px, 30vh, 320px)" in html
    assert "overflow-y: auto" in html
    assert "#wizardFileList" in html
    assert "max-height: clamp(150px, 24vh, 280px)" in html
    assert ".ws-wizard-dialog" in html
    assert "max-height: min(92vh, 860px)" in html

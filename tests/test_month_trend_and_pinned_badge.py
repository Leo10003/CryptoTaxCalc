from __future__ import annotations

from pathlib import Path


def test_month_trend_arrow_uses_delta_and_pinned_badge_does_not_clip():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "const trendIcon = delta > 0 ? '▲' : (delta < 0 ? '▼' : '→');" in html
    assert "const trendLabel = delta > 0 ? 'up' : (delta < 0 ? 'down' : 'flat');" in html
    assert "const trendIcon = d > 0" not in html
    assert "const trendLabel = d > 0" not in html

    assert '#legendMonth .rs-legend-row.is-pinned > div:first-child::after' in html
    assert "min-width: max-content" in html
    assert "white-space: nowrap" in html
    assert "overflow: visible" in html
    assert "const tipW = hasPinned2 ? 300 : 170;" in html

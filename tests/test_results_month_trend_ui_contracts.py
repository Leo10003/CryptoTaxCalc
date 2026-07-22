from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

TEMPLATE = Path("templates/workspace_results.html")


def _html() -> str:
    return TEMPLATE.read_text(encoding="utf-8", errors="replace")


def test_month_trend_arrow_uses_delta_variable_and_not_stale_d():
    html = _html()

    # Avoid asserting raw arrow glyphs here; PowerShell/codepage conversions can
    # corrupt them when writing test files. The important contract is that the
    # month trend uses the computed delta variable, not the stale undefined d.
    assert "const trendIcon = delta > 0 ?" in html
    assert "delta < 0 ?" in html
    assert "const trendLabel = delta > 0 ? 'up' : (delta < 0 ? 'down' : 'flat');" in html
    assert "tipD.textContent = `${trendIcon} ${trendLabel}: ${deltaTxt}${pctTxt}`;" in html

    assert "const trendIcon = d > 0" not in html
    assert "const trendLabel = d > 0" not in html


def test_month_pinned_badge_does_not_clip_and_tooltip_has_room():
    html = _html()

    assert '#legendMonth .rs-legend-row.is-pinned > div:first-child::after' in html
    assert "min-width: max-content" in html
    assert "white-space: nowrap" in html
    assert "overflow: visible" in html
    assert "const tipW = hasPinned2 ? 300 : 170;" in html

from __future__ import annotations

from pathlib import Path


def test_results_delta_marker_is_directional():
    html = Path("templates/workspace_results.html").read_text(encoding="utf-8", errors="replace")

    assert "const trendIcon = d > 0 ? '▲' : (d < 0 ? '▼' : '→');" in html
    assert "const trendLabel = d > 0 ? 'up' : (d < 0 ? 'down' : 'flat');" in html
    assert "tipD.textContent = `${trendIcon} ${trendLabel}: ${deltaTxt}${pctTxt}`;" in html
    assert "Î”" not in html

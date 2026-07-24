from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

APP = Path("src/cryptotaxcalc/app.py")


def test_partial_history_summary_helper_exists_and_preserves_detailed_warnings():
    text = APP.read_text(encoding="utf-8", errors="replace")

    assert "def _summarize_missing_history_warnings(warnings: list) -> dict | None:" in text
    assert '"type": "partial_history_summary"' in text
    assert '"type") == "partial_history_summary"' in text
    assert "return [summary] + warnings_list" in text

    # Detailed missing_history warnings must remain available for audit/debugging.
    assert 'w.get("type") != "missing_history"' in text
    assert "warnings_list = list(warnings or [])" in text


def test_calculation_and_exports_apply_partial_history_summary_before_blocker_check():
    text = APP.read_text(encoding="utf-8", errors="replace")

    assert "warnings = _with_partial_history_summary(warnings)" in text

    # The summary should be added before blocker export checks so export UX sees it too.
    assert (
        "events, summary, warnings = compute_fifo(tx_models)\n"
        "        warnings = _with_partial_history_summary(warnings)\n"
        "        _export_block_if_blockers(warnings)"
    ) in text or (
        "events, summary, warnings = compute_fifo(tx_models)\n"
        "    warnings = _with_partial_history_summary(warnings)\n"
        "    _export_block_if_blockers(warnings)"
    ) in text

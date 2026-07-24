from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

APP = Path("src/cryptotaxcalc/app.py")


def test_tax_summary_preserves_structured_warning_dicts_for_results_ui():
    text = APP.read_text(encoding="utf-8", errors="replace")

    assert "warnings = [x for x in w if x is not None]" in text
    assert "Converting dicts with str(x) makes the browser render raw Python text." in text
    assert "warnings = [str(x) for x in w if x is not None]" not in text


def test_partial_history_summary_is_available_for_structured_missing_history_warnings():
    text = APP.read_text(encoding="utf-8", errors="replace")

    assert 'def _with_partial_history_summary(warnings: list) -> list:' in text
    assert '"type": "partial_history_summary"' in text
    assert "return [summary] + warnings_list" in text

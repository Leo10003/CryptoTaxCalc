from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

APP = Path("src/cryptotaxcalc/app.py")


def test_app_imports_re_for_optional_year_query_helper():
    text = APP.read_text(encoding="utf-8", errors="replace")

    assert "import re" in text
    assert "def _optional_year_query_to_int(year: object) -> int | None:" in text
    assert 're.fullmatch(r"\\d{4}", s)' in text

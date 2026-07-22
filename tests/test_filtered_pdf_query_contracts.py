from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

TEMPLATE = Path("templates/workspace_results.html")


def test_filtered_pdf_query_omits_empty_all_year_and_asset_filters():
    html = TEMPLATE.read_text(encoding="utf-8", errors="replace")

    assert "function buildSubsetPdfQuery()" in html
    assert "Never send year= or asset=." in html
    assert "if (/^\\d{4}$/.test(y)) params.year = y;" in html
    assert "if (a && a !== 'ALL' && a !== 'ALL ASSETS') params.asset = a;" in html

    assert "const qs = buildSubsetPdfQuery();" in html


def test_filtered_pdf_download_url_appends_download_with_correct_separator():
    html = TEMPLATE.read_text(encoding="utf-8", errors="replace")

    assert "function appendQueryParam(url, key, value)" in html
    assert "String(url || '').includes('?') ? '&' : '?'" in html
    assert "appendQueryParam(url, 'download', '1')" in html
    assert "url + '&download=1'" not in html

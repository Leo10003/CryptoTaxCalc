from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cryptotaxcalc.app import app
from cryptotaxcalc.runtime_paths import RESOURCE_ROOT


pytestmark = pytest.mark.smoke


def test_support_issue_report_page_serves_utf8_charset():
    client = TestClient(app)

    response = client.get("/support/report-issue")

    assert response.status_code == 200
    content_type = response.headers.get("content-type", "").lower()

    assert "text/html" in content_type
    assert "charset=utf-8" in content_type
    assert '<meta charset="utf-8">' in response.text


def test_workspace_results_template_declares_utf8_charset():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    assert template_path.exists(), f"{template_path} is missing"

    html = template_path.read_text(encoding="utf-8").lower()

    assert '<meta charset="utf-8">' in html


def test_workspace_results_template_has_no_common_mojibake_sequences():
    template_path = RESOURCE_ROOT / "templates" / "workspace_results.html"

    assert template_path.exists(), f"{template_path} is missing"

    html = template_path.read_text(encoding="utf-8")

    forbidden = [
        "â",      # generic UTF-8-as-cp1252 mojibake marker
        "Â",      # common mojibake marker for symbols such as £ / NBSP
        "\u009d", # cp1252 control remnant from broken right quote
        "\u008f",
        "\u008d",
        "�",      # replacement character
        "”¦",
    ]

    found = [s for s in forbidden if s in html]

    assert not found, f"{template_path} contains mojibake sequences: {found}"
    assert "Generating PDF" in html
    assert "Generating PDF”¦" not in html
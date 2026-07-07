from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_workspace_results_explains_core_tax_numbers():
    response = client.get("/workspace/results")

    assert response.status_code == 200
    html = response.text

    assert "What these numbers mean" in html

    assert "id=\"rsExplainProceeds\"" in html
    assert "id=\"rsExplainCost\"" in html
    assert "id=\"rsExplainGain\"" in html
    assert "id=\"rsExplainTaxable\"" in html
    assert "id=\"rsExplainExempt\"" in html
    assert "id=\"rsExplainWarnings\"" in html

    assert "function updateResultsExplanation(totals)" in html
    assert "updateResultsExplanation(totals);" in html

    assert "What you received from taxable disposals" in html
    assert "FIFO cost basis" in html
    assert "jurisdiction rules" in html
    assert "reviewed before filing or exporting" in html
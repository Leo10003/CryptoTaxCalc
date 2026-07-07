from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_workspace_results_shows_warning_summary_counts_in_info_section():
    response = client.get("/workspace/results")

    assert response.status_code == 200
    html = response.text

    assert 'id="rsWarningsSummary"' in html
    assert 'aria-label="Warning summary"' in html
    assert "function renderWarningsSummary(rawWarnings)" in html
    assert "renderWarningsSummary(raw);" in html
    assert "renderWarningsSummary(list);" in html

    assert "actionCount ? 'is-action' : 'is-info'" in html
    assert "warnCount ? 'is-warn' : 'is-info'" in html
    assert "0 action" in html
    assert "0 warnings" in html
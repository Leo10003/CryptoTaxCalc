from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_workspace_results_explains_event_row_issues_in_events_table():
    response = client.get("/workspace/results")

    assert response.status_code == 200
    html = response.text

    assert "function explainEventIssue({ asset, ts, proceedsN, costN, gainN })" in html
    assert "missing asset" in html
    assert "invalid timestamp" in html
    assert "missing proceeds" in html
    assert "missing cost basis" in html
    assert "missing gain" in html

    assert "const issueText = explainEventIssue({ asset, ts, proceedsN, costN, gainN });" in html
    assert "rows.push({ ev, isIssue, issueText, proceedsN, costN, gainN });" in html

    assert "rs-event-issue-note" in html
    assert "Issue: ${issues.join(', ')}" in html
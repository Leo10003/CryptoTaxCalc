from __future__ import annotations

# Consolidated from:
# - tests/test_workspace_results_event_issue_notes_ui.py
# - tests/test_workspace_results_explanation_ui.py
# - tests/test_workspace_results_export_warning_ui.py
# - tests/test_workspace_results_warning_summary_ui.py


#========================================================================================
# Source: tests/test_workspace_results_event_issue_notes_ui.py
#========================================================================================
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


#========================================================================================
# Source: tests/test_workspace_results_explanation_ui.py
#========================================================================================
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


#========================================================================================
# Source: tests/test_workspace_results_export_warning_ui.py
#========================================================================================
import pytest
from starlette.testclient import TestClient

from cryptotaxcalc.app import app

pytestmark = pytest.mark.smoke

client = TestClient(app)


def test_workspace_results_prompts_before_export_when_non_blocking_warnings_exist():
    response = client.get("/workspace/results")

    assert response.status_code == 200
    html = response.text

    assert "function buildSoftExportWarningDetail()" in html
    assert "Review warnings before export" in html
    assert "severity: 'warning'" in html
    assert "Action-level warnings should remain governed by backend export blockers." in html

    assert "const softWarning = buildSoftExportWarningDetail();" in html
    assert "showExportBlockerModal(softWarning, url);" in html

    assert "const nextUrl = isSoftWarning ? _exportPendingUrl : _withForce(_exportPendingUrl);" in html
    assert "I understand the warnings shown above and still want to proceed with this export." in html


#========================================================================================
# Source: tests/test_workspace_results_warning_summary_ui.py
#========================================================================================
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

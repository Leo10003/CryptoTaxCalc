from __future__ import annotations

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
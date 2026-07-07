from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMOKE_WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "smoke.yml"


def _normalize_shell_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def test_github_smoke_workflow_runs_marker_based_smoke_suite():
    assert SMOKE_WORKFLOW.exists(), (
        "Expected GitHub smoke workflow at .github/workflows/smoke.yml. "
        "This workflow is what runs the smoke-marked purpose-critical tests in CI."
    )

    workflow_text = SMOKE_WORKFLOW.read_text(encoding="utf-8")
    normalized = _normalize_shell_text(workflow_text)

    assert "pytest" in normalized, "Smoke workflow must run pytest."

    assert re.search(r"\bpytest\b.*\s-m\s+smoke\b", normalized), (
        "Smoke workflow must run the marker-based smoke suite, for example: "
        "`pytest -q -m smoke --maxfail=1 --disable-warnings -rA`. "
        "Running only tests/smoke_test.py would exclude purpose-critical smoke-marked tests."
    )

    forbidden_direct_file_commands = (
        "pytest -q tests/smoke_test.py",
        "pytest tests/smoke_test.py",
        "python -m pytest tests/smoke_test.py",
    )

    assert not any(command in normalized for command in forbidden_direct_file_commands), (
        "Smoke workflow must not target only tests/smoke_test.py. "
        "Use `pytest -q -m smoke` so all smoke-marked correctness, audit, FX, import, "
        "export, and run-isolation tests run in GitHub Actions."
    )
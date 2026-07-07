from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = PROJECT_ROOT / "tests"

PURPOSE_CRITICAL_FILE_PATTERNS = (
    "audit",
    "calculation_run",
    "exchange_adapter",
    "export_artifact",
    "golden",
    "import_atomicity",
    "jurisdiction_golden",
    "production_failure",
    "reporting_year",
    "run_isolation",
    "strict_fx",
    "tax_engine",
)

EXEMPT_FILES = {Path(__file__).name}


def _is_purpose_critical(path: Path) -> bool:
    name = path.name
    if name in EXEMPT_FILES:
        return False
    return any(pattern in name for pattern in PURPOSE_CRITICAL_FILE_PATTERNS)


def _node_is_pytest_mark_smoke(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "smoke"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "pytest"
    )


def _node_contains_smoke_marker(node: ast.AST) -> bool:
    if _node_is_pytest_mark_smoke(node):
        return True
    return any(_node_is_pytest_mark_smoke(child) for child in ast.walk(node))


def _module_has_smoke_pytestmark(tree: ast.Module) -> bool:
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in stmt.targets):
                if _node_contains_smoke_marker(stmt.value):
                    return True
        elif isinstance(stmt, ast.AnnAssign):
            target = stmt.target
            if isinstance(target, ast.Name) and target.id == "pytestmark" and stmt.value is not None:
                if _node_contains_smoke_marker(stmt.value):
                    return True
    return False


def test_purpose_critical_test_files_are_selected_by_smoke_workflow():
    purpose_critical_files = sorted(
        path
        for path in TESTS_DIR.glob("test_*.py")
        if _is_purpose_critical(path)
    )

    assert purpose_critical_files, "No purpose-critical test files matched the smoke guard patterns"

    missing_smoke_marker: list[str] = []
    for path in purpose_critical_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if not _module_has_smoke_pytestmark(tree):
            missing_smoke_marker.append(str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"))

    assert not missing_smoke_marker, (
        "Purpose-critical test files must declare module-level "
        "`pytestmark = pytest.mark.smoke` so GitHub's smoke workflow runs them. "
        f"Missing marker in: {missing_smoke_marker}"
    )
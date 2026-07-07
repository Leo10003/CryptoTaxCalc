from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CRITICAL_RUNTIME_PACKAGES = [
    "anyio",
    "fastapi",
    "httpx",
    "jinja2",
    "pydantic",
    "python-multipart",
    "sqlalchemy",
    "starlette",
]

CRITICAL_TEST_PACKAGES = [
    "pytest",
]

CRITICAL_PACKAGES = CRITICAL_RUNTIME_PACKAGES + CRITICAL_TEST_PACKAGES


def _normalize_package_name(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def _requirements_pins() -> dict[str, str]:
    requirements = PROJECT_ROOT / "requirements.txt"
    assert requirements.exists(), "requirements.txt is required for CI/local dependency parity"

    pins: dict[str, str] = {}
    for raw_line in requirements.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("-e "):
            continue

        match = re.match(r"^([A-Za-z0-9_.-]+)==([^;\s]+)", line)
        if match:
            package, version = match.groups()
            pins[_normalize_package_name(package)] = version

    return pins


def _workflow_text() -> str:
    workflow_dir = PROJECT_ROOT / ".github" / "workflows"
    assert workflow_dir.exists(), ".github/workflows must exist"

    workflow_files = sorted([*workflow_dir.glob("*.yml"), *workflow_dir.glob("*.yaml")])
    assert workflow_files, "At least one GitHub Actions workflow file is required"

    return "\n\n".join(path.read_text(encoding="utf-8") for path in workflow_files)


def test_critical_dependencies_are_exactly_pinned_in_requirements_txt():
    pins = _requirements_pins()

    missing = [package for package in CRITICAL_PACKAGES if package not in pins]

    assert missing == []
    for package in CRITICAL_PACKAGES:
        assert pins[package]
        assert not pins[package].startswith(">=")
        assert not pins[package].startswith("~=")
        assert "*" not in pins[package]


def test_ci_workflow_installs_from_requirements_txt_before_running_smoke_tests():
    text = _workflow_text()
    normalized = re.sub(r"\s+", " ", text.lower())

    assert "requirements.txt" in normalized
    assert re.search(r"(python -m pip|pip)\s+install\s+(-r|--requirement)\s+requirements\.txt", normalized)

    smoke_index = normalized.find("-m smoke")
    requirements_index = normalized.find("requirements.txt")

    assert smoke_index != -1, "CI workflow must run pytest with -m smoke"
    assert requirements_index != -1
    assert requirements_index < smoke_index, "CI must install requirements.txt before running smoke tests"


def test_ci_smoke_command_matches_local_hardened_smoke_command():
    text = _workflow_text()
    normalized = re.sub(r"\s+", " ", text.lower())

    assert "pytest" in normalized
    assert "-m smoke" in normalized
    assert "--maxfail=1" in normalized
    assert "--disable-warnings" in normalized
    assert "-ra" in normalized or "-r a" in normalized

    assert "pytest -q -m smoke --maxfail=1 --disable-warnings -ra" in normalized or (
        "pytest" in normalized
        and "-q" in normalized
        and "-m smoke" in normalized
        and "--maxfail=1" in normalized
        and "--disable-warnings" in normalized
        and "-ra" in normalized
    )


def test_ci_runs_ui_template_warning_parity_check_before_full_smoke():
    text = _workflow_text()
    normalized = re.sub(r"\s+", " ", text.lower())

    assert "test_ui_template_critical_contracts.py" in normalized
    assert "test_configuration_environment_safety.py" in normalized
    assert "-w error::deprecationwarning" in normalized

    warning_check_index = normalized.find("-w error::deprecationwarning")
    full_smoke_index = normalized.find("-m smoke")

    assert warning_check_index != -1
    assert full_smoke_index != -1
    assert warning_check_index < full_smoke_index
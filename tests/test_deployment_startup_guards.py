from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from starlette.testclient import TestClient

pytestmark = pytest.mark.smoke


REQUIRED_MODULES = (
    "cryptotaxcalc.app",
    "cryptotaxcalc.db",
    "cryptotaxcalc.models",
    "cryptotaxcalc.calc_runner",
    "cryptotaxcalc.fifo_engine",
    "cryptotaxcalc.runtime_paths",
    "cryptotaxcalc.routes.ui",
    "cryptotaxcalc.routes.csv_admin",
    "cryptotaxcalc.history_routes",
)

REQUIRED_ENDPOINTS = (
    "/",
    "/health",
    "/version",
    "/workspace",
    "/workspace/results",
    "/csv/formats",
    "/status",
    "/export/status",
    "/data_quality/precheck",
    "/data_quality/missing_history",
    "/calculate/v2",
    "/history",
    "/history/runs",
)

REQUIRED_TEMPLATE_FILES = (
    "landing.html",
    "workspace.html",
    "workspace_results.html",
    "csv_formats.html",
    "history.html",
    "admin_csv_unsupported.html",
)


def test_deployment_required_modules_import_cleanly():
    for module_name in REQUIRED_MODULES:
        module = importlib.import_module(module_name)
        assert module is not None


def test_deployment_runtime_resources_exist_and_are_not_runtime_state_dirs():
    from cryptotaxcalc.runtime_paths import PROJECT_ROOT, RESOURCE_ROOT

    assert PROJECT_ROOT.exists()
    assert RESOURCE_ROOT.exists()
    assert PROJECT_ROOT.is_absolute()
    assert RESOURCE_ROOT.is_absolute()

    assert (PROJECT_ROOT / "src" / "cryptotaxcalc").exists()
    assert (RESOURCE_ROOT / "templates").exists()
    assert (RESOURCE_ROOT / "static").exists()

    runtime_state_dirs = {
        PROJECT_ROOT / "artifacts",
        PROJECT_ROOT / "backups",
        PROJECT_ROOT / "logs",
        PROJECT_ROOT / "support_bundles",
    }

    assert RESOURCE_ROOT not in runtime_state_dirs


def test_deployment_required_templates_exist():
    from cryptotaxcalc.runtime_paths import RESOURCE_ROOT

    templates_dir = RESOURCE_ROOT / "templates"

    missing = [
        name
        for name in REQUIRED_TEMPLATE_FILES
        if not (templates_dir / name).exists()
    ]

    assert missing == []


def test_deployment_required_get_routes_are_reachable_without_server_error():
    from cryptotaxcalc.app import app

    client = TestClient(app)

    get_endpoints = (
        "/",
        "/health",
        "/version",
        "/workspace",
        "/workspace/results",
        "/csv/formats",
        "/status",
        "/export/status",
        "/data_quality/missing_history",
        "/history",
        "/history/runs",
    )

    failures: list[str] = []

    for endpoint in get_endpoints:
        response = client.get(endpoint)
        if response.status_code >= 500:
            failures.append(f"{endpoint} returned {response.status_code}")

    assert failures == []


def test_deployment_operational_get_endpoints_respond_without_server_error():
    from cryptotaxcalc.app import app

    client = TestClient(app)

    endpoints = (
        "/health",
        "/version",
        "/",
        "/workspace",
        "/csv/formats",
        "/status",
        "/export/status",
        "/data_quality/missing_history",
    )

    for endpoint in endpoints:
        response = client.get(endpoint)
        assert response.status_code < 500, f"{endpoint} returned {response.status_code}"
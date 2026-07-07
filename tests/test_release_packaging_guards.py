from __future__ import annotations

import fnmatch
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_RELEASE_PATTERNS = (
    "artifacts/*",
    "backups/*",
    "logs/*",
    "support_bundles/*",
    "*.db",
    "*.db-*",
    "*.sqlite",
    "*.sqlite-*",
    "*.sqlite3",
    "*.sqlite3-*",
    "__pycache__/*",
    "*.pyc",
    ".pytest_cache/*",
)

REQUIRED_RELEASE_FILES = (
    "pyproject.toml",
    "requirements.txt",
    "src/cryptotaxcalc/__init__.py",
    "src/cryptotaxcalc/app.py",
    "templates/landing.html",
)


def _git_executable() -> str:
    candidates = [
        os.environ.get("GIT_EXE"),
        os.environ.get("GIT"),
        shutil.which("git"),
        shutil.which("git.exe"),
        r"C:\Program Files\Git\bin\git.exe",
        r"C:\Program Files\Git\cmd\git.exe",
        r"C:\Program Files (x86)\Git\bin\git.exe",
        r"C:\Program Files (x86)\Git\cmd\git.exe",
    ]

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate

    pytest.skip("git executable is not available in this test environment")


def _git(*args: str) -> str:
    result = subprocess.run(
        [_git_executable(), *args],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return result.stdout


def _tracked_files() -> list[str]:
    return [
        line.strip().replace("\\", "/")
        for line in _git("ls-files").splitlines()
        if line.strip()
    ]


def _matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/")
    return any(fnmatch.fnmatch(normalized, pattern) for pattern in patterns)


def test_release_required_files_are_tracked():
    tracked = set(_tracked_files())

    missing = [
        path
        for path in REQUIRED_RELEASE_FILES
        if path not in tracked
    ]

    assert missing == []


def test_release_tracked_files_exclude_runtime_state_and_cache_files():
    tracked = _tracked_files()

    offenders = [
        path
        for path in tracked
        if _matches_any(path, FORBIDDEN_RELEASE_PATTERNS)
    ]

    assert offenders == []


def test_git_archive_does_not_include_runtime_state_or_cache_files():
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "release.tar"

        subprocess.run(
            [_git_executable(), "archive", "--format=tar", f"--output={archive_path}", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

        with tarfile.open(archive_path, "r") as archive:
            names = [
                member.name.replace("\\", "/")
                for member in archive.getmembers()
                if member.name and not member.isdir()
            ]

    offenders = [
        name
        for name in names
        if _matches_any(name, FORBIDDEN_RELEASE_PATTERNS)
    ]

    assert offenders == []


def test_git_archive_contains_runtime_resources_needed_by_app():
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / "release.tar"

        subprocess.run(
            [_git_executable(), "archive", "--format=tar", f"--output={archive_path}", "HEAD"],
            cwd=PROJECT_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

        with tarfile.open(archive_path, "r") as archive:
            names = {
                member.name.replace("\\", "/")
                for member in archive.getmembers()
                if member.name and not member.isdir()
            }

    missing = [
        path
        for path in REQUIRED_RELEASE_FILES
        if path not in names
    ]

    assert missing == []


def test_release_package_metadata_declares_src_layout_and_runtime_entrypoint():
    pyproject = PROJECT_ROOT / "pyproject.toml"
    text = pyproject.read_text(encoding="utf-8")

    assert "src" in text
    assert "cryptotaxcalc" in text
    assert "uvicorn" in text or "cryptotaxcalc.app:app" in text
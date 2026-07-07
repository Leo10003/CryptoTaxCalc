from __future__ import annotations

import os
import shutil
import fnmatch
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.smoke

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_TRACKED_PATH_PREFIXES = (
    "artifacts/",
    "backups/",
    "logs/",
    "support_bundles/",
)

FORBIDDEN_TRACKED_SUFFIXES = (
    ".db",
    ".sqlite",
    ".sqlite3",
    ".db-shm",
    ".db-wal",
    ".sqlite-shm",
    ".sqlite-wal",
    ".sqlite3-shm",
    ".sqlite3-wal",
)

REQUIRED_GITIGNORE_PATTERNS = (
    "artifacts/",
    "backups/",
    "support_bundles/",
    "logs/",
    "*.db",
    "*.sqlite",
    "*.sqlite3",
    "*.db-*",
    "*.sqlite3-*",
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


def test_runtime_generated_directories_are_ignored_by_gitignore():
    gitignore_path = PROJECT_ROOT / ".gitignore"
    assert gitignore_path.exists(), ".gitignore is required"

    gitignore_text = gitignore_path.read_text(encoding="utf-8")

    missing = [
        pattern
        for pattern in REQUIRED_GITIGNORE_PATTERNS
        if pattern not in gitignore_text
    ]

    assert missing == []


def test_runtime_artifact_directories_are_not_tracked_in_git():
    tracked = _tracked_files()

    offenders = [
        path
        for path in tracked
        if path.startswith(FORBIDDEN_TRACKED_PATH_PREFIXES)
    ]

    assert offenders == []


def test_database_and_sqlite_sidecar_files_are_not_tracked_in_git():
    tracked = _tracked_files()

    offenders = [
        path
        for path in tracked
        if path.lower().endswith(FORBIDDEN_TRACKED_SUFFIXES)
    ]

    assert offenders == []


def test_no_runtime_logs_or_generated_audit_files_are_tracked_in_git():
    tracked = _tracked_files()

    forbidden_patterns = (
        "logs/**/*.log",
        "logs/**/*.json",
        "logs/**/*.jsonl",
        "logs/**/*.txt",
        "logs/**/*.csv",
        "artifacts/**/*",
        "support_bundles/**/*",
        "backups/**/*",
    )

    offenders: list[str] = []
    for path in tracked:
        for pattern in forbidden_patterns:
            if fnmatch.fnmatch(path, pattern):
                offenders.append(path)
                break

    assert offenders == []


def test_git_status_has_no_deleted_runtime_artifacts_waiting_to_be_committed():
    """
    Runtime artifacts should not appear as deleted tracked files after tests or local runs.
    This catches the common case where backup/log files were committed earlier and then
    local cleanup creates noisy deleted-file status entries.
    """
    status = _git("status", "--porcelain")

    offenders: list[str] = []
    for raw_line in status.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        path = line[3:].replace("\\", "/") if len(line) > 3 else line

        status_code = raw_line[:2]

        # Staged deletions are allowed here because this is exactly how we remove
        # previously tracked runtime artifacts from the repository.
        if status_code == "D ":
            continue

        if path.startswith(FORBIDDEN_TRACKED_PATH_PREFIXES):
            offenders.append(raw_line)
        elif path.lower().endswith(FORBIDDEN_TRACKED_SUFFIXES):
            offenders.append(raw_line)

    assert offenders == []
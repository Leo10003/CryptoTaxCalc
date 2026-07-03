from __future__ import annotations

import os
import sys
from pathlib import Path as FSPath


def _resolve_project_root() -> FSPath:
    if getattr(sys, "frozen", False):
        return FSPath(sys.executable).resolve().parent

    here = FSPath(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        if (p / "pyproject.toml").exists() or (p / "requirements.txt").exists():
            return p.resolve()

    return here.parents[2]


def _resolve_resource_root(project_root: FSPath) -> FSPath:
    """
    Resolve where runtime resources live (templates/static/logo).

    Priority:
      1) Frozen builds: sys._MEIPASS
      2) Explicit override: CTC_RESOURCE_ROOT
      3) Repo root if it contains templates/ and static/
      4) Current working directory if it contains templates/ and static/
      5) Fallback: project_root
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return FSPath(meipass).resolve()

    env_root = (os.getenv("CTC_RESOURCE_ROOT") or "").strip()
    if env_root:
        return FSPath(env_root).resolve()

    if (project_root / "templates").exists() and (project_root / "static").exists():
        return project_root

    cwd = FSPath.cwd()
    if (cwd / "templates").exists() and (cwd / "static").exists():
        return cwd.resolve()

    return project_root


PROJECT_ROOT = _resolve_project_root()
RESOURCE_ROOT = _resolve_resource_root(PROJECT_ROOT)

AUTOMATION = RESOURCE_ROOT / "automation"

# Ops paths (used by admin ops router)
GIT_SCRIPT = PROJECT_ROOT / "automation" / "git_auto_push.ps1"
LOG_DIR = PROJECT_ROOT / "automation" / "logs"
SUPPORT_BUNDLES_DIR = PROJECT_ROOT / "support_bundles"

from __future__ import annotations

import fnmatch
import os
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INCLUDE = [
    "src/**",
    "templates/**",
    "static/**",
    "docs/**",
    ".github/**",
    "automation/**",
    "tests/**",
    "README.md",
    "requirements.txt",
    "pyproject.toml",
    "LICENSE",
    ".gitignore",
    ".env.example",
    "RELEASE_CHECKLIST.md",
]

EXCLUDE_GLOBS = [
    ".env",
    ".env.*",
    ".git/**",
    ".venv/**",
    "**/__pycache__/**",
    "**/*.pyc",
    ".pytest_cache/**",
    ".mypy_cache/**",
    ".ruff_cache/**",
    "storage_raw/**",
    "support_bundles/**",
    "artifacts/**",
    "backups/**",
    "dist/**",
    "build/**",
    "*.db",
    "*.sqlite",
    "*-wal",
    "*-shm",
]


def _match_any(path: str, globs: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, g) for g in globs)


def make_snapshot(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for inc in INCLUDE:
            # Expand directories via globbing
            matches = list(PROJECT_ROOT.glob(inc))
            if not matches:
                continue

            for m in matches:
                if m.is_dir():
                    for p in m.rglob("*"):
                        if not p.is_file():
                            continue
                        rel = p.relative_to(PROJECT_ROOT).as_posix()
                        if _match_any(rel, EXCLUDE_GLOBS):
                            continue
                        z.write(p, rel)
                else:
                    rel = m.relative_to(PROJECT_ROOT).as_posix()
                    if _match_any(rel, EXCLUDE_GLOBS):
                        continue
                    z.write(m, rel)

    print(f"Wrote snapshot: {out_path}")


if __name__ == "__main__":
    name = os.getenv("CTC_SNAPSHOT_NAME", "CryptoTaxCalc_shareable_snapshot.zip")
    make_snapshot(PROJECT_ROOT / "artifacts" / name)

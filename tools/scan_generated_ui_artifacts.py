from __future__ import annotations

from pathlib import Path
import sys


SCAN_ROOTS = [
    Path("artifacts"),
    Path("dist"),
    Path("demo_usb"),
]

TEXT_EXTENSIONS = {
    ".html",
    ".js",
    ".css",
    ".py",
    ".json",
    ".txt",
    ".md",
    ".csv",
    ".log",
}

SKIP_PARTS = {
    ".git",
    ".venv",
    "env",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
}

FORBIDDEN_TOKENS = [
    "Ã",
    "Â",
    "â€™",
    "â€œ",
    "â€",
    "â€¦",
    "â€”",
    "â€“",
    "â€¢",
    "â‚¬",
    "Î”",
    "îˆ",
    "î‡",
    "”¦",
    "�",
    "Opening automatically”",
    "Generating PDF”",
    "Updating”",
]


def should_skip(path: Path) -> bool:
    return any(part in SKIP_PARTS for part in path.parts)


def iter_files():
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if should_skip(path):
                continue
            if not path.is_file():
                continue
            if path.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            yield path


def main() -> int:
    offenders: list[str] = []
    checked = 0

    for path in iter_files():
        checked += 1
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            offenders.append(f"{path}: could not read text: {exc}")
            continue

        for token in FORBIDDEN_TOKENS:
            if token not in text:
                continue
            for line_no, line in enumerate(text.splitlines(), start=1):
                if token in line:
                    offenders.append(f"{path}:{line_no}: contains {token!r}: {line.strip()[:220]}")

    print(f"Checked generated text files: {checked}")

    if offenders:
        print("Found UI/text artifacts:")
        for item in offenders[:120]:
            print(item)
        if len(offenders) > 120:
            print(f"... and {len(offenders) - 120} more")
        return 1

    print("No generated UI/text artifacts found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

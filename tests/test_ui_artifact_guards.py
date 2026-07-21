from __future__ import annotations

from pathlib import Path


SOURCE_ROOTS = [
    Path("templates"),
    Path("src/cryptotaxcalc"),
]

TEXT_EXTENSIONS = {
    ".html",
    ".js",
    ".css",
    ".py",
    ".json",
    ".txt",
    ".md",
}

FORBIDDEN_TOKENS = [
    "Ã",
    "Â",
    "â€™",
    "â€œ",
    "â€",
    "â€¦",
    "â€”",
    "â€“",
    "â€¢",
    "â‚¬",
    "Î”",
    "îˆ",
    "î‡",
    "”¦",
    "”...",
    "�",
    "Opening automatically”",
    "Generating PDF”",
    "Updating”",
]


def _iter_source_text_files():
    for root in SOURCE_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            yield path


def test_source_ui_text_has_no_common_encoding_artifacts():
    offenders: list[str] = []
    checked = 0

    for path in _iter_source_text_files():
        checked += 1
        text = path.read_text(encoding="utf-8", errors="replace")

        for token in FORBIDDEN_TOKENS:
            if token not in text:
                continue

            for line_no, line in enumerate(text.splitlines(), start=1):
                if token in line:
                    offenders.append(f"{path}:{line_no}: contains {token!r}: {line.strip()[:220]}")

    assert checked > 0, "No source UI files checked"
    assert not offenders, "\n".join(offenders[:80])

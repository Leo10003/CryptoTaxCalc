from __future__ import annotations

from pathlib import Path


def test_templates_do_not_contain_common_mojibake_or_ui_artifacts():
    forbidden = [
        "Opening automatically”...",
        "Opening automatically”",
        "Î”",
        "îˆ",
        "î‡",
        "â€¦",
        "â‚¬",
        "”¦",
        "�",
    ]

    checked = []
    offenders = []

    for path in Path("templates").glob("*.html"):
        checked.append(path)
        text = path.read_text(encoding="utf-8", errors="replace")
        for token in forbidden:
            if token in text:
                offenders.append(f"{path}: contains {token!r}")

    assert checked, "No templates checked"
    assert not offenders, "\n".join(offenders)

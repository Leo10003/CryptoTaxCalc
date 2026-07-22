from __future__ import annotations

from pathlib import Path


def test_python_test_files_do_not_start_with_utf8_bom():
    offenders = []

    for path in Path("tests").glob("test_*.py"):
        raw = path.read_bytes()
        if raw.startswith(b"\xef\xbb\xbf"):
            offenders.append(str(path))

    assert not offenders, "Python test files must not start with UTF-8 BOM:\n" + "\n".join(offenders)

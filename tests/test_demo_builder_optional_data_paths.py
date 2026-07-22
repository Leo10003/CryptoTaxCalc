from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke


def test_demo_builder_skips_missing_optional_demo_data_paths():
    text = Path("src/cryptotaxcalc/demo_builder.py").read_text(encoding="utf-8", errors="replace")

    assert "def add_data_if_exists(src_path: Path, dest_name: str) -> None:" in text
    assert "Skipping missing demo build data path" in text
    assert 'add_data_if_exists(demo_dir, "demo")' in text

    # The old unconditional PyInstaller add-data entry broke builds when ./demo was absent.
    assert 'f"{demo_dir};demo",' not in text

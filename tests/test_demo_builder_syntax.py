from __future__ import annotations

import py_compile
from pathlib import Path


def test_demo_builder_is_valid_python():
    py_compile.compile(
        str(Path("src/cryptotaxcalc/demo_builder.py")),
        doraise=True,
    )

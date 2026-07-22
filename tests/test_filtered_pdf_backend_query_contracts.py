from __future__ import annotations

from pathlib import Path

import pytest


pytestmark = pytest.mark.smoke

APP = Path("src/cryptotaxcalc/app.py")


def _section_from_func(text: str, func_name: str) -> str:
    start_marker = f"def {func_name}("
    async_start_marker = f"async def {func_name}("

    start = text.find(start_marker)
    if start == -1:
        start = text.find(async_start_marker)
    if start == -1:
        raise AssertionError(f"Could not find function: {func_name}")

    next_def = text.find("\ndef ", start + 1)
    next_async_def = text.find("\nasync def ", start + 1)

    candidates = [x for x in [next_def, next_async_def] if x != -1]
    end = min(candidates) if candidates else len(text)

    return text[start:end]


def test_subset_pdf_routes_accept_string_year_so_empty_year_reaches_route_logic():
    text = APP.read_text(encoding="utf-8", errors="replace")

    helper_section = _section_from_func(text, "_optional_year_query_to_int")
    subset_section = _section_from_func(text, "export_workspace_summary_subset")
    job_section = _section_from_func(text, "export_workspace_summary_subset_job")

    assert "if not s:" in helper_section
    assert "return None" in helper_section
    assert 're.fullmatch(r"\\d{4}", s)' in helper_section

    assert 'year: str | None = Query(None, description="Optional tax-year filter (YYYY)")' in subset_section
    assert "year_i = _optional_year_query_to_int(year)" in subset_section
    assert 'year: int | None = Query(None, description="Optional tax-year filter (YYYY)")' not in subset_section

    assert "year: str | None = Query(None)" in job_section
    assert "year_i = _optional_year_query_to_int(year)" in job_section
    assert "year: int | None = Query(None)" not in job_section


def test_subset_pdf_job_url_does_not_generate_empty_year_param():
    text = APP.read_text(encoding="utf-8", errors="replace")

    helper_section = _section_from_func(text, "_subset_pdf_query_string")
    job_section = _section_from_func(text, "export_workspace_summary_subset_job")

    assert 'params.append(("year", str(year)))' in helper_section
    assert 'qs = _subset_pdf_query_string(' in job_section
    assert 'pdf_url = f"/export/workspace_summary/{run_db_id}/subset.pdf{qs}"' in job_section

    assert 'f"?year={year or \'\'}' not in job_section
    assert "year={year or ''}" not in job_section

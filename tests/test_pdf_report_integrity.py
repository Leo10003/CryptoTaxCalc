from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from reportlab import rl_config

from cryptotaxcalc.report_pdf import build_summary_pdf

pytestmark = pytest.mark.smoke

LOG_PATH = Path("logs/pdf/last_run.json")


def _pdf_textish(pdf_bytes: bytes) -> str:
    return pdf_bytes.decode("latin-1", errors="ignore")


def _assert_valid_pdf_bytes(pdf_bytes: bytes) -> None:
    assert pdf_bytes.startswith(b"%PDF-"), pdf_bytes[:50]
    assert b"%%EOF" in pdf_bytes[-2048:]
    assert len(pdf_bytes) > 10_000


def test_build_summary_pdf_includes_report_scope_totals_and_audit_metadata_in_pdf_and_diagnostics(monkeypatch):
    monkeypatch.setattr(rl_config, "pageCompression", 0)
    run_id = f"pdf-direct-{uuid.uuid4()}"
    asset = f"PDFD{uuid.uuid4().hex[:8]}".upper()

    pdf_bytes = build_summary_pdf(
        {
            "title": "PDF Integrity Smoke Report",
            "run_id": run_id,
            "year": 2025,
            "scope_asset": asset,
            "scope_year": "2025",
            "period_start": "2025-06-01",
            "period_end": "2025-06-01",
            "generated_at": "2026-01-01T00:00:00Z",
            "jurisdiction": "HR",
            "rule_version": "2025.1",
            "summary_by_quote": {
                "EUR": {
                    "proceeds": "1234.56",
                    "cost_basis": "789.01",
                    "gain": "445.55",
                }
            },
            "summary_by_month": {
                "2025-06": {
                    "proceeds": "1234.56",
                    "cost_basis": "789.01",
                    "gain": "445.55",
                }
            },
            "summary_by_asset": {
                asset: {
                    "proceeds": "1234.56",
                    "cost_basis": "789.01",
                    "gain": "445.55",
                }
            },
            "eur_summary": {
                "totals_eur": {
                    "proceeds": "1234.56",
                    "cost_basis": "789.01",
                    "gain": "445.55",
                },
                "notes": [],
            },
            "top_events": [
                {
                    "timestamp": "2025-06-01T00:00:00+00:00",
                    "asset": asset,
                    "qty_sold": "1",
                    "proceeds": "1234.56",
                    "cost_basis": "789.01",
                    "gain": "445.55",
                    "quote_asset": "EUR",
                }
            ],
            "run_totals": {
                "taxable_gain_eur": "445.55",
                "exempt_gain_eur": "0.00",
                "tax_year_used": 2025,
            },
            "warnings": [],
            "show_portfolio_charts": False,
            "show_timeline_chart": False,
            "show_audit_appendix": True,
            "show_tax_helpers": True,
        }
    )

    _assert_valid_pdf_bytes(pdf_bytes)

    textish = _pdf_textish(pdf_bytes)
    assert "PDF Integrity Smoke Report" in textish
    assert run_id in textish
    assert asset in textish
    assert "HR" in textish
    assert "2025" in textish
    assert "1,234.56" in textish
    assert "445.55" in textish
    assert "Audit-Ready Appendix" in textish

    diag = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    assert diag["run_id"] == run_id
    assert diag["title"] == "PDF Integrity Smoke Report"
    assert diag["summary_sections"] == {
        "by_month": True,
        "by_quote": True,
        "by_asset": True,
        "eur_summary": True,
        "top_events": 1,
    }
    assert diag["pdf_size_bytes"] == len(pdf_bytes)
from __future__ import annotations

import os
import sys
import pathlib
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

# Ensure imports work when running from repo root without pip install (same pattern as smoke_test.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from cryptotaxcalc.app import app  # noqa: E402


client = TestClient(app)

_FIX = pathlib.Path(__file__).resolve().parent / "fixtures" / "determinism" / "minimal_normalized.csv"


def _D(v) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _import_fixture_reset() -> dict:
    assert _FIX.exists(), f"Missing determinism fixture CSV: {_FIX}"

    content = _FIX.read_bytes()

    # /import/multiple expects multipart with field name "files"
    files = [
        ("files", (_FIX.name, content, "text/csv")),
    ]
    r = client.post("/import/multiple?reset=1", files=files)
    assert r.status_code == 200, f"import failed: {r.status_code} {r.text}"
    data = r.json()
    assert isinstance(data, dict)
    assert "results" in data and isinstance(data["results"], list)
    assert data["results"], "import returned empty results"
    inserted = sum(int(x.get("inserted", 0) or 0) for x in data["results"] if isinstance(x, dict))
    assert inserted > 0, f"expected inserted > 0, got {inserted}"
    return data


def _run_calc(jurisdiction: str) -> tuple[int, dict, dict]:
    r = client.post("/calculate/v2", json={"jurisdiction": jurisdiction, "tax_year": 2025})
    assert r.status_code == 200, f"/calculate/v2 failed: {r.status_code} {r.text}"
    payload = r.json()
    assert isinstance(payload, dict)
    assert isinstance(payload.get("run_id"), int)
    assert isinstance(payload.get("summary"), dict)
    dig = payload.get("digests") or {}
    assert isinstance(dig, dict)
    return int(payload["run_id"]), payload["summary"], dig


@pytest.mark.parametrize("jurisdiction", ["HR", "IT"])
def test_determinism_e2e_output_hash_and_totals(jurisdiction: str):
    """
    Determinism contract:
    - For identical inputs and config, the *results* must be identical across runs.
    - We compare output_hash (realized events list digest) and EUR totals.
    - We do NOT compare manifest_hash across runs because it includes run metadata (timestamps/id).
    """
    _import_fixture_reset()

    run1, sum1, dig1 = _run_calc(jurisdiction)
    run2, sum2, dig2 = _run_calc(jurisdiction)

    assert run1 != run2, "each /calculate/v2 call should create a new run id"

    # Verify each run’s stored digests are internally consistent
    v1 = client.get(f"/audit/verify/{run1}")
    assert v1.status_code == 200, f"audit verify failed for run1: {v1.status_code} {v1.text}"
    assert v1.json().get("verified") is True

    v2 = client.get(f"/audit/verify/{run2}")
    assert v2.status_code == 200, f"audit verify failed for run2: {v2.status_code} {v2.text}"
    assert v2.json().get("verified") is True

    # Cross-run determinism: output_hash must match
    assert dig1.get("output_hash") == dig2.get("output_hash"), "output_hash must be deterministic across runs"

    # Cross-run determinism: totals must match (ignore timings_ms and other volatile fields)
    t1 = sum1.get("totals") or {}
    t2 = sum2.get("totals") or {}
    assert isinstance(t1, dict) and isinstance(t2, dict)

    keys = ["proceeds_eur", "cost_eur", "gain_eur", "taxable_gain_eur", "exempt_gain_eur"]
    for k in keys:
        assert _D(t1.get(k)) == _D(t2.get(k)), f"totals.{k} must be deterministic across runs"

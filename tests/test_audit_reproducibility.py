import pytest

from copy import deepcopy

from cryptotaxcalc.audit_digest import compute_digests

pytestmark = pytest.mark.smoke


def _manifest() -> dict:
    return {
        "timestamp_built": "2026-07-07T12:00:00Z",
        "run_id": 101,
        "run": {
            "id": 101,
            "started_at": "2026-07-07T11:59:59Z",
            "finished_at": "2026-07-07T12:00:01Z",
            "jurisdiction": "HR",
            "rule_version": "2025.1",
            "lot_method": "FIFO",
            "fx_set_id": None,
            "params": {
                "holding_exemption_days": 730,
                "jurisdiction": "HR",
                "tax_year": 2025,
            },
        },
        "fx_batch": {
            "id": None,
            "imported_at": None,
            "source": None,
            "rates_hash": None,
        },
        "inputs": {
            "mode": "snapshot",
            "cutoff_finished_at": None,
            "transactions_hashes_ordered": [
                "00a-buy-btc-eur-2025-01-01",
                "00b-sell-btc-eur-2025-06-01",
            ],
        },
        "outputs": [
            {
                "timestamp": "2025-06-01T12:00:00+00:00",
                "asset": "BTC",
                "qty_sold": "1",
                "proceeds": "25000",
                "cost_basis": "20000",
                "gain": "5000",
                "quote_asset": "EUR",
                "fee_applied": None,
                "matches": [
                    {
                        "from_qty": "1",
                        "lot_cost_per_unit": "20000",
                        "lot_cost_total": "20000",
                    }
                ],
            }
        ],
    }


def test_compute_digests_is_repeatable_for_identical_manifest_content():
    manifest = _manifest()

    first = compute_digests(deepcopy(manifest))
    second = compute_digests(deepcopy(manifest))

    assert first == second
    assert set(first) == {"input_hash", "output_hash", "manifest_hash"}
    assert all(len(value) == 64 for value in first.values())


def test_compute_digests_ignores_volatile_timestamp_built_for_manifest_hash():
    original = _manifest()
    rebuilt_later = deepcopy(original)
    rebuilt_later["timestamp_built"] = "2030-01-01T00:00:00Z"

    assert compute_digests(original) == compute_digests(rebuilt_later)


def test_input_hash_changes_when_input_transaction_set_changes():
    original = _manifest()
    changed = deepcopy(original)
    changed["inputs"]["transactions_hashes_ordered"].append("00c-extra-buy-eth-eur-2025-07-01")

    original_digests = compute_digests(original)
    changed_digests = compute_digests(changed)

    assert changed_digests["input_hash"] != original_digests["input_hash"]
    assert changed_digests["manifest_hash"] != original_digests["manifest_hash"]
    assert changed_digests["output_hash"] == original_digests["output_hash"]


def test_output_hash_changes_when_realized_event_changes():
    original = _manifest()
    changed = deepcopy(original)
    changed["outputs"][0]["gain"] = "4900"
    changed["outputs"][0]["cost_basis"] = "20100"
    changed["outputs"][0]["matches"][0]["lot_cost_total"] = "20100"

    original_digests = compute_digests(original)
    changed_digests = compute_digests(changed)

    assert changed_digests["input_hash"] == original_digests["input_hash"]
    assert changed_digests["output_hash"] != original_digests["output_hash"]
    assert changed_digests["manifest_hash"] != original_digests["manifest_hash"]


def test_input_hash_changes_when_tax_configuration_changes():
    original = _manifest()
    changed = deepcopy(original)
    changed["run"]["jurisdiction"] = "IT"
    changed["run"]["params"] = {
        "it_threshold_eur": None,
        "jurisdiction": "IT",
        "tax_year": 2025,
    }

    original_digests = compute_digests(original)
    changed_digests = compute_digests(changed)

    assert changed_digests["input_hash"] != original_digests["input_hash"]
    assert changed_digests["output_hash"] == original_digests["output_hash"]
    assert changed_digests["manifest_hash"] != original_digests["manifest_hash"]


def test_trust_metadata_does_not_change_stored_digest_contract():
    original = _manifest()
    with_trust_metadata = deepcopy(original)
    with_trust_metadata["trust"] = {
        "strict_fx_configured": False,
        "fx_fallback_used": True,
        "fx_fallback_days_count": 2,
        "fee_valuation": {"base_asset_fee_value_source": "spot_price"},
    }

    assert compute_digests(original) == compute_digests(with_trust_metadata)


def test_canonical_json_makes_dict_key_order_irrelevant():
    original = _manifest()
    reordered = deepcopy(original)
    reordered["run"]["params"] = {
        "tax_year": 2025,
        "jurisdiction": "HR",
        "holding_exemption_days": 730,
    }

    assert compute_digests(original) == compute_digests(reordered)
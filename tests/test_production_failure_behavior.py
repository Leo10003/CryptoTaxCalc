from __future__ import annotations

import json
from pathlib import Path

import pytest

from cryptotaxcalc.csv_normalizer import CSVFormatError, parse_csv_with_meta
import cryptotaxcalc.csv_source_registry as csv_source_registry

pytestmark = pytest.mark.smoke


def _is_format_error(exc: BaseException, expected_text: str) -> bool:
    return isinstance(exc, CSVFormatError) and expected_text.lower() in str(exc).lower()


def test_duplicate_headers_fail_before_any_rows_are_accepted():
    raw = b"timestamp,type,base_asset,base_amount,timestamp\n2025-01-01,buy,BTC,1,2025-01-02\n"

    with pytest.raises(CSVFormatError) as raised:
        parse_csv_with_meta(raw, filename="duplicate_headers.csv")

    assert _is_format_error(raised.value, "Duplicate CSV header")
    assert raised.value.meta["recognized_source_status"] == "supported"
    assert raised.value.meta["recognized_source_id"] == "cryptotaxcalc_generic"


def test_blank_headers_fail_before_any_rows_are_accepted():
    raw = b"timestamp,type,base_asset,base_amount,\n2025-01-01,buy,BTC,1,extra\n"

    with pytest.raises(CSVFormatError) as raised:
        parse_csv_with_meta(raw, filename="blank_header.csv")

    assert _is_format_error(raised.value, "Blank CSV header")
    assert raised.value.meta["recognized_source_status"] == "supported"
    assert raised.value.meta["recognized_source_id"] == "cryptotaxcalc_generic"


def test_unsupported_source_fails_loudly_and_records_structure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    registry_dir = tmp_path / "csv_sources"
    monkeypatch.setattr(csv_source_registry, "_REGISTRY_DIR", registry_dir)
    monkeypatch.setattr(csv_source_registry, "_SUPPORTED_PATH", registry_dir / "supported_sources.json")
    monkeypatch.setattr(csv_source_registry, "_UNSUPPORTED_PATH", registry_dir / "unsupported_structures.json")

    raw = b"Trade Time,Coin,Operation,Units,Total Value\n2025-01-01,BTC,Acquire,1,10000\n"

    with pytest.raises(CSVFormatError) as raised:
        parse_csv_with_meta(raw, filename="unknown_exchange_export.csv")

    assert _is_format_error(raised.value, "Unrecognized CSV format")
    meta = raised.value.meta
    assert meta["recognized_source_status"] == "unsupported"
    assert meta["recognized_source_id"] is None
    assert isinstance(meta["recognized_source_signature"], str)
    assert len(meta["recognized_source_signature"]) == 64

    unsupported_path = registry_dir / "unsupported_structures.json"
    payload = json.loads(unsupported_path.read_text(encoding="utf-8"))
    entry = payload["signatures"][meta["recognized_source_signature"]]
    assert entry["headers"] == ["trade time", "coin", "operation", "units", "total value"]
    assert entry["filenames"] == ["unknown_exchange_export.csv"]
    assert entry["count"] == 1
    assert entry["reason"] == "no match in supported registry"


def test_invalid_required_values_are_reported_as_row_errors_not_silent_transactions():
    raw = b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount\nnot-a-date,buy,BTC,1,EUR,10000,EUR,0\n2025-01-02,sell,BTC,0.5,EUR,7500,EUR,0\n"

    rows, errors, meta = parse_csv_with_meta(raw, filename="normalized_invalid_date.csv")

    assert meta["recognized_source_id"] == "cryptotaxcalc_generic"
    assert len(rows) == 1
    assert rows[0].type == "sell"
    assert rows[0].base_asset == "BTC"
    assert len(errors) == 1
    assert "row 2" in errors[0]
    assert "unrecognized timestamp" in errors[0]


def test_extra_columns_are_reported_as_row_errors_not_silent_transactions():
    raw = b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount\n2025-01-01,buy,BTC,1,EUR,10000,EUR,0\n2025-01-02,sell,BTC,0.5,EUR,8000,EUR,0,unexpected-extra-cell\n"

    rows, errors, meta = parse_csv_with_meta(raw, filename="normalized_extra_columns.csv")

    assert meta["recognized_source_id"] == "cryptotaxcalc_generic"
    assert len(rows) == 1
    assert rows[0].type == "buy"
    assert len(errors) == 1
    assert "row 3" in errors[0]
    assert "more columns than the CSV header" in errors[0]


def test_negative_amounts_and_fee_without_asset_are_reported_as_row_errors():
    raw = b"timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount\n2025-01-01,buy,BTC,-1,EUR,10000,EUR,0\n2025-01-02,sell,BTC,0.5,EUR,8000,,10\n"

    rows, errors, meta = parse_csv_with_meta(raw, filename="normalized_bad_amounts.csv")

    assert meta["recognized_source_id"] == "cryptotaxcalc_generic"
    assert rows == []
    assert len(errors) == 2
    assert "base_amount must be positive" in errors[0]
    assert "fee_asset is required when fee_amount is positive" in errors[1]
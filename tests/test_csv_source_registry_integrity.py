from __future__ import annotations

from pathlib import Path

import pytest

import cryptotaxcalc.csv_source_registry as csv_source_registry
from cryptotaxcalc.csv_normalizer import PARSER_BY_SOURCE_ID
from cryptotaxcalc.csv_source_registry import (
    detect_csv_source,
    headers_signature,
    list_supported_sources_catalog,
    list_unsupported_signatures,
    record_unsupported_structure,
)

pytestmark = pytest.mark.smoke

EXPECTED_SUPPORTED_SOURCE_IDS = {
    "ledger_live",
    "cryptotaxcalc_generic",
    "binance_spot_trades",
    "coinbase_transactions",
    "kraken_trades",
    "okx_trades",
    "bybit_executions",
    "kucoin_fills",
    "crypto_com_exchange_trades",
    "bitfinex_trades",
    "bitget_spot_trades",
    "gateio_trades",
}


def _registry_sandbox(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    registry_dir = tmp_path / "csv_sources"
    monkeypatch.setattr(csv_source_registry, "_REGISTRY_DIR", registry_dir)
    monkeypatch.setattr(csv_source_registry, "_SUPPORTED_PATH", registry_dir / "supported_sources.json")
    monkeypatch.setattr(csv_source_registry, "_UNSUPPORTED_PATH", registry_dir / "unsupported_structures.json")
    return registry_dir


def _source_headers(source: dict) -> list[str]:
    match = source["match"]
    return list(match.get("headers_all") or []) + list(match.get("headers_any") or [])


def _source_filename(source: dict) -> str:
    keywords = source["match"].get("filename_keywords") or []
    if keywords:
        return f"{keywords[0]}_{source['id']}.csv"
    return f"{source['id']}.csv"


def test_builtin_supported_source_catalog_has_stable_complete_ids_and_unique_parsers(monkeypatch, tmp_path):
    _registry_sandbox(monkeypatch, tmp_path)

    catalog = list_supported_sources_catalog()
    source_ids = [source["id"] for source in catalog]

    assert set(source_ids) == EXPECTED_SUPPORTED_SOURCE_IDS
    assert len(source_ids) == len(set(source_ids))

    parser_by_source_id = {
        source["id"]: source.get("parser")
        for source in catalog
    }
    assert parser_by_source_id["cryptotaxcalc_generic"] == "generic"

    implemented_parser_ids = set(PARSER_BY_SOURCE_ID)
    assert implemented_parser_ids == EXPECTED_SUPPORTED_SOURCE_IDS - {"cryptotaxcalc_generic"}

    non_generic_parsers = [parser for parser in parser_by_source_id.values() if parser and parser != "generic"]
    assert sorted(non_generic_parsers) == sorted(implemented_parser_ids)
    assert len(non_generic_parsers) == len(set(non_generic_parsers))


def test_supported_source_match_definitions_are_normalized_and_detectable(monkeypatch, tmp_path):
    _registry_sandbox(monkeypatch, tmp_path)

    for source in list_supported_sources_catalog():
        source_id = source["id"]
        match = source["match"]
        headers_all = match.get("headers_all") or []
        headers_any = match.get("headers_any") or []
        filename_keywords = match.get("filename_keywords") or []

        assert headers_all, f"{source_id} must declare required detection headers"
        assert len(headers_all) == len(set(headers_all)), f"{source_id} has duplicate headers_all entries"
        assert len(headers_any) == len(set(headers_any)), f"{source_id} has duplicate headers_any entries"
        assert len(filename_keywords) == len(set(filename_keywords)), f"{source_id} has duplicate filename keywords"

        for header in headers_all + headers_any:
            assert header == header.strip().lower(), f"{source_id} header is not normalized: {header!r}"
        for keyword in filename_keywords:
            assert keyword == keyword.strip().lower(), f"{source_id} filename keyword is not normalized: {keyword!r}"

        detection = detect_csv_source(headers=_source_headers(source), filename=_source_filename(source))
        assert detection.status == "supported"
        assert detection.source_id == source_id
        assert detection.confidence == 1.0
        assert isinstance(detection.signature, str) and len(detection.signature) == 64


def test_supported_source_signatures_do_not_collide(monkeypatch, tmp_path):
    _registry_sandbox(monkeypatch, tmp_path)

    signatures: dict[str, str] = {}
    for source in list_supported_sources_catalog():
        headers_norm = [str(header).strip().lower() for header in _source_headers(source)]
        signature = headers_signature(headers_norm)
        previous_source_id = signatures.setdefault(signature, source["id"])
        assert previous_source_id == source["id"], (
            f"CSV source signature collision: {source['id']} and {previous_source_id} both use {signature}"
        )


def test_supported_detection_removes_matching_previously_unsupported_signature(monkeypatch, tmp_path):
    _registry_sandbox(monkeypatch, tmp_path)
    source = next(src for src in list_supported_sources_catalog() if src["id"] == "binance_spot_trades")
    headers_norm = [str(header).strip().lower() for header in _source_headers(source)]
    signature = headers_signature(headers_norm)

    record_unsupported_structure(
        signature=signature,
        headers_norm=headers_norm,
        filename="old_unknown_binance_export.csv",
        delimiter=",",
        quotechar='"',
        reason="previously unsupported during implementation gap",
    )
    assert any(item["signature"] == signature for item in list_unsupported_signatures())

    detection = detect_csv_source(headers=_source_headers(source), filename="binance_spot_trades.csv")

    assert detection.status == "supported"
    assert detection.source_id == "binance_spot_trades"
    assert all(item["signature"] != signature for item in list_unsupported_signatures())
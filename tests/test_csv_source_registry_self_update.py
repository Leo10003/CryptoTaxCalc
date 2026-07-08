from __future__ import annotations

import json
from pathlib import Path

import pytest

from cryptotaxcalc import csv_source_registry as registry

pytestmark = pytest.mark.smoke


def test_registry_updates_existing_builtin_source_definitions(tmp_path, monkeypatch):
    registry_dir = tmp_path / "csv_sources"
    supported_path = registry_dir / "supported_sources.json"
    unsupported_path = registry_dir / "unsupported_structures.json"

    monkeypatch.setattr(registry, "_REGISTRY_DIR", registry_dir)
    monkeypatch.setattr(registry, "_SUPPORTED_PATH", supported_path)
    monkeypatch.setattr(registry, "_UNSUPPORTED_PATH", unsupported_path)

    stale_payload = {
        "version": 1,
        "sources": [
            {
                "id": "okx_trades",
                "name": "OKX Trades",
                "status": "supported",
                "parser": "okx_trades",
                "match": {
                    "headers_all": [
                        "time",
                        "instrument",
                        "side",
                        "type",
                        "price",
                        "size",
                        "trade value",
                        "fee",
                        "fee currency",
                        "trade id",
                    ],
                    "headers_any": [],
                    "filename_keywords": ["okx", "okex"],
                },
            },
            {
                "id": "custom_wallet_export",
                "name": "Custom Wallet Export",
                "status": "supported",
                "parser": "generic",
                "match": {
                    "headers_all": ["timestamp", "type", "base_asset", "base_amount"],
                    "headers_any": ["memo"],
                    "filename_keywords": ["custom_wallet"],
                },
            },
        ],
    }

    supported_path.parent.mkdir(parents=True, exist_ok=True)
    supported_path.write_text(json.dumps(stale_payload), encoding="utf-8")

    registry.ensure_csv_source_registry_files()

    updated = json.loads(supported_path.read_text(encoding="utf-8"))
    sources = {s["id"]: s for s in updated["sources"]}

    assert "okx_trades" in sources
    assert "custom_wallet_export" in sources

    okx_match = sources["okx_trades"]["match"]

    assert okx_match["headers_all"] == [
        "time",
        "instrument",
        "side",
        "size",
        "trade value",
    ]
    assert "type" in okx_match["headers_any"]
    assert "price" in okx_match["headers_any"]
    assert "fee" in okx_match["headers_any"]
    assert "fee currency" in okx_match["headers_any"]
    assert "trade id" in okx_match["headers_any"]

    # Custom/user-added source definitions must not be removed.
    assert sources["custom_wallet_export"]["name"] == "Custom Wallet Export"


def test_registry_relaxed_builtin_rules_detect_existing_okx_file_without_manual_delete(tmp_path, monkeypatch):
    registry_dir = tmp_path / "csv_sources"
    supported_path = registry_dir / "supported_sources.json"
    unsupported_path = registry_dir / "unsupported_structures.json"

    monkeypatch.setattr(registry, "_REGISTRY_DIR", registry_dir)
    monkeypatch.setattr(registry, "_SUPPORTED_PATH", supported_path)
    monkeypatch.setattr(registry, "_UNSUPPORTED_PATH", unsupported_path)

    stale_payload = {
        "version": 1,
        "sources": [
            {
                "id": "okx_trades",
                "name": "OKX Trades",
                "status": "supported",
                "parser": "okx_trades",
                "match": {
                    "headers_all": [
                        "time",
                        "instrument",
                        "side",
                        "type",
                        "price",
                        "size",
                        "trade value",
                        "fee",
                        "fee currency",
                        "trade id",
                    ],
                    "headers_any": [],
                    "filename_keywords": ["okx", "okex"],
                },
            },
        ],
    }

    supported_path.parent.mkdir(parents=True, exist_ok=True)
    supported_path.write_text(json.dumps(stale_payload), encoding="utf-8")

    meta = registry.detect_csv_source(
        headers=[
            "Time",
            "Instrument",
            "Side",
            "Size",
            "Trade Value",
            "Fee",
            "Fee Currency",
            "Trade ID",
        ],
        filename="okx_trades.csv",
    )

    assert meta.source_id == "okx_trades"
    assert meta.status == "supported"
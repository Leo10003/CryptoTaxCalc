from __future__ import annotations

import csv
import io

import pytest


def _csv_bytes(headers: list[str], row: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    w.writerow(row)
    return buf.getvalue().encode("utf-8")


@pytest.fixture()
def _registry_sandbox(monkeypatch, tmp_path):
    # csv_source_registry writes to storage_raw/ relative to CWD
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_supported_sources_detectable_and_parsers_exist(_registry_sandbox):
    from cryptotaxcalc.csv_source_registry import list_supported_sources_catalog, detect_csv_source
    from cryptotaxcalc.csv_normalizer import PARSER_BY_SOURCE_ID, parse_csv_with_meta

    catalog = list_supported_sources_catalog()
    assert catalog, "No supported CSV sources found in catalog."

    # 1) Each catalog source has match headers and is detectable.
    for src in catalog:
        sid = src.get("id")
        assert sid, f"Catalog entry missing id: {src!r}"

        match = src.get("match") or {}
        headers_all = match.get("headers_all") or []
        assert isinstance(headers_all, list) and headers_all, f"{sid} missing match.headers_all"

        det = detect_csv_source(headers=headers_all, filename=f"{sid}.csv")
        assert det.status == "supported", f"{sid} was not detected as supported (got {det.status})"
        assert det.source_id == sid, f"{sid} detected as {det.source_id}"

        parser_name = src.get("parser")
        if parser_name and parser_name != "generic":
            assert sid in PARSER_BY_SOURCE_ID, f"{sid} is in registry but has no parser mapping in csv_normalizer"

    # 2) Minimal parse smoke tests (ensures routing doesn't crash)
    samples = {
        "cryptotaxcalc_generic": _csv_bytes(
            ["timestamp", "type", "base_asset", "base_amount", "quote_asset", "quote_amount"],
            ["2024-01-01 00:00:00", "buy", "BTC", "0.01", "EUR", "500"],
        ),
        "ledger_live": _csv_bytes(
            ["Operation Date", "Currency Ticker", "Operation Type", "Operation Amount"],
            ["2024-01-01 00:00:00", "BTC", "IN", "0.01"],
        ),
        "binance_spot_trades": _csv_bytes(
            ["Date(UTC)", "Symbol", "Side", "Price", "Quantity", "Amount", "Fee", "Fee Coin", "Quote Asset"],
            ["2024-01-01 00:00:00", "BTCUSDT", "BUY", "50000", "0.01", "500", "0.0001", "BNB", "USDT"],
        ),

        "coinbase_transactions": _csv_bytes(
            ["Timestamp", "Transaction Type", "Asset", "Quantity Transacted", "Spot Price Currency", "Spot Price at Transaction", "Subtotal", "Total (inclusive of fees)", "Fees", "Notes"],
            ["2024-01-01T00:00:00Z", "Buy", "BTC", "0.01", "USD", "50000", "500", "501", "1", "Example"],
        ),
        "kraken_trades": _csv_bytes(
            ["txid", "ordertxid", "pair", "time", "type", "ordertype", "price", "cost", "fee", "vol", "margin", "misc", "ledgers"],
            ["T1", "O1", "XBTUSD", "2024-01-01T00:00:00Z", "buy", "market", "50000", "500", "0.5", "0.01", "0", "", ""],
        ),
        "okx_trades": _csv_bytes(
            ["Time", "Instrument", "Side", "Type", "Price", "Size", "Trade Value", "Fee", "Fee Currency", "Trade ID"],
            ["2024-01-01 00:00:00", "BTC-USD", "BUY", "spot", "50000", "0.01", "500", "0.5", "USD", "okx_1"],
        ),
        "bybit_executions": _csv_bytes(
            ["Exec Time", "Order ID", "Symbol", "Side", "Order Type", "Order Price", "Order Qty", "Exec Price", "Exec Qty", "Exec Value", "Exec Fee", "Fee Currency"],
            ["2024-01-01 00:00:00", "by_1", "BTCUSD", "BUY", "Market", "50000", "0.01", "50000", "0.01", "500", "0.5", "USD"],
        ),
        "kucoin_fills": _csv_bytes(
            ["Time", "Symbol", "Side", "Price", "Size", "Funds", "Fee", "Fee Currency", "Order ID", "Trade ID"],
            ["2024-01-01 00:00:00", "BTC-USD", "buy", "50000", "0.01", "500", "0.5", "USD", "O1", "T1"],
        ),
        "crypto_com_exchange_trades": _csv_bytes(
            ["Timestamp (UTC)", "Instrument", "Side", "Price", "Quantity", "Total", "Fee", "Fee Currency", "Transaction ID"],
            ["2024-01-01 00:00:00", "BTC_USD", "BUY", "50000", "0.01", "500", "0.5", "USD", "cdc_1"],
        ),
        "bitfinex_trades": _csv_bytes(
            ["ID", "PAIR", "AMOUNT", "PRICE", "FEE", "FEE CURRENCY", "TIME"],
            ["1", "BTCUSD", "-0.01", "50000", "0.5", "USD", "2024-01-01T00:00:00Z"],
        ),
        "bitget_spot_trades": _csv_bytes(
            ["Date", "Symbol", "Side", "Price", "Quantity", "Amount", "Fee", "Fee Coin", "Order ID"],
            ["2024-01-01 00:00:00", "BTCUSDT", "BUY", "50000", "0.01", "500", "0.0001", "BGB", "bgO_1"],
        ),
        "gateio_trades": _csv_bytes(
            ["Time", "Currency Pair", "Side", "Price", "Amount", "Total", "Fee", "Fee Currency", "Order ID"],
            ["2024-01-01 00:00:00", "BTC_USDT", "buy", "50000", "0.01", "500", "0.5", "USDT", "gtO_1"],
        ),
    }

    for sid, raw in samples.items():
        rows, errors, meta = parse_csv_with_meta(raw, filename=f"{sid}.csv")
        assert rows, f"{sid} produced no parsed rows; sample errors={errors[:3]}"
        assert meta.get("recognized_source_status") == "supported", f"{sid} not reported as supported"

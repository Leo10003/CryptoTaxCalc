from __future__ import annotations

import io
from decimal import Decimal

import pytest

from cryptotaxcalc.csv_normalizer import parse_csv_stream_with_meta

pytestmark = pytest.mark.smoke


def _parse(csv_text: str, *, filename: str):
    rows, errors, meta = parse_csv_stream_with_meta(
        io.StringIO(csv_text),
        filename=filename,
    )
    return rows, errors, meta


def _dec(value) -> Decimal:
    return Decimal(str(value))


def test_binance_spot_trades_golden_buy_and_sell_with_fee_coin():
    csv_text = "\n".join(
        [
            "Date(UTC),Symbol,Side,Price,Quantity,Amount,Fee,Fee Coin,Quote Asset",
            "2024-01-02 03:04:05,BTCUSDT,BUY,42000.00,0.01000000,420.00,0.00001000,BTC,USDT",
            "2024-02-03 04:05:06,ETHUSDT,SELL,2500.00,0.50000000,1250.00,1.25,USDT,USDT",
        ]
    )

    rows, errors, meta = _parse(csv_text, filename="binance_spot_trades.csv")

    assert errors == []
    assert meta["recognized_source_id"] == "binance_spot_trades"
    assert meta["recognized_source_status"] == "supported"

    assert len(rows) == 2

    buy = rows[0]
    assert buy.type == "BUY"
    assert buy.exchange == "BINANCE"
    assert buy.base_asset == "BTC"
    assert buy.base_amount == _dec("0.01000000")
    assert buy.quote_asset == "USDT"
    assert buy.quote_amount == _dec("420.00")
    assert buy.fee_asset == "BTC"
    assert buy.fee_amount == _dec("0.00001000")
    assert buy.memo == "symbol=BTCUSDT; side=BUY"

    sell = rows[1]
    assert sell.type == "SELL"
    assert sell.exchange == "BINANCE"
    assert sell.base_asset == "ETH"
    assert sell.base_amount == _dec("0.50000000")
    assert sell.quote_asset == "USDT"
    assert sell.quote_amount == _dec("1250.00")
    assert sell.fee_asset == "USDT"
    assert sell.fee_amount == _dec("1.25")
    assert sell.memo == "symbol=ETHUSDT; side=SELL"


def test_coinbase_transactions_golden_buy_and_sell_fee_in_quote_currency():
    csv_text = "\n".join(
        [
            "Timestamp,Transaction Type,Asset,Quantity Transacted,Spot Price Currency,Subtotal,Fees,Notes",
            "2024-01-02T03:04:05Z,Buy,BTC,0.01000000,EUR,400.00,2.50,Coinbase buy order",
            "2024-02-03T04:05:06Z,Sell,ETH,-0.50000000,EUR,1200.00,4.00,Coinbase sell order",
        ]
    )

    rows, errors, meta = _parse(csv_text, filename="coinbase_transactions.csv")

    assert errors == []
    assert meta["recognized_source_id"] == "coinbase_transactions"
    assert meta["recognized_source_status"] == "supported"

    assert len(rows) == 2

    buy = rows[0]
    assert buy.type == "BUY"
    assert buy.exchange == "COINBASE"
    assert buy.base_asset == "BTC"
    assert buy.base_amount == _dec("0.01000000")
    assert buy.quote_asset == "EUR"
    assert buy.quote_amount == _dec("400.00")
    assert buy.fee_asset == "EUR"
    assert buy.fee_amount == _dec("2.50")
    assert buy.memo == "Coinbase buy order"

    sell = rows[1]
    assert sell.type == "SELL"
    assert sell.exchange == "COINBASE"
    assert sell.base_asset == "ETH"
    assert sell.base_amount == _dec("0.50000000")
    assert sell.quote_asset == "EUR"
    assert sell.quote_amount == _dec("1200.00")
    assert sell.fee_asset == "EUR"
    assert sell.fee_amount == _dec("4.00")
    assert sell.memo == "Coinbase sell order"


def test_kraken_trades_golden_pair_split_xbt_to_btc_and_memo_ids():
    csv_text = "\n".join(
        [
            "txid,ordertxid,pair,time,type,ordertype,price,cost,fee,vol,margin,misc,ledgers",
            "TX123,ORDER123,XBTEUR,2024-01-02 03:04:05,buy,limit,40000.0,400.00,0.80,0.01000000,0,,",
            "TX456,ORDER456,ETHEUR,2024-02-03 04:05:06,sell,limit,2400.0,1200.00,2.40,0.50000000,0,,",
        ]
    )

    rows, errors, meta = _parse(csv_text, filename="kraken_trades.csv")

    assert errors == []
    assert meta["recognized_source_id"] == "kraken_trades"
    assert meta["recognized_source_status"] == "supported"

    assert len(rows) == 2

    buy = rows[0]
    assert buy.type == "BUY"
    assert buy.exchange == "KRAKEN"
    assert buy.base_asset == "BTC"
    assert buy.base_amount == _dec("0.01000000")
    assert buy.quote_asset == "EUR"
    assert buy.quote_amount == _dec("400.00")
    assert buy.fee_asset == "EUR"
    assert buy.fee_amount == _dec("0.80")
    assert buy.memo == "txid=TX123 | order=ORDER123"

    sell = rows[1]
    assert sell.type == "SELL"
    assert sell.exchange == "KRAKEN"
    assert sell.base_asset == "ETH"
    assert sell.base_amount == _dec("0.50000000")
    assert sell.quote_asset == "EUR"
    assert sell.quote_amount == _dec("1200.00")
    assert sell.fee_asset == "EUR"
    assert sell.fee_amount == _dec("2.40")
    assert sell.memo == "txid=TX456 | order=ORDER456"
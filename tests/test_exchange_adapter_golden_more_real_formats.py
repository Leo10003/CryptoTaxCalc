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


@pytest.mark.parametrize(
    "filename,expected_source,csv_text,expected",
    [
        (
            "okx_trades.csv",
            "okx_trades",
            "\n".join(
                [
                    "Time,Instrument,Side,Size,Trade Value,Fee,Fee Currency,Trade ID",
                    "2024-01-02 03:04:05,BTC-USDT,BUY,0.01000000,420.00,0.10,USDT,OKX-1",
                    "2024-02-03 04:05:06,ETH-USDT,SELL,0.50000000,1250.00,1.25,USDT,OKX-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "OKX",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "trade_id=OKX-1",
                },
                {
                    "type": "SELL",
                    "exchange": "OKX",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "trade_id=OKX-2",
                },
            ],
        ),
        (
            "bybit_executions.csv",
            "bybit_executions",
            "\n".join(
                [
                    "Exec Time,Order ID,Symbol,Side,Exec Qty,Exec Value,Exec Fee,Fee Currency",
                    "2024-01-02 03:04:05,BYBIT-1,BTCUSDT,BUY,0.01000000,420.00,0.10,USDT",
                    "2024-02-03 04:05:06,BYBIT-2,ETHUSDT,SELL,0.50000000,1250.00,1.25,USDT",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "BYBIT",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "order_id=BYBIT-1",
                },
                {
                    "type": "SELL",
                    "exchange": "BYBIT",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "order_id=BYBIT-2",
                },
            ],
        ),
        (
            "kucoin_fills.csv",
            "kucoin_fills",
            "\n".join(
                [
                    "Time,Symbol,Side,Size,Funds,Fee,Fee Currency,Order ID,Trade ID",
                    "2024-01-02 03:04:05,BTC-USDT,buy,0.01000000,420.00,0.10,USDT,KU-O-1,KU-T-1",
                    "2024-02-03 04:05:06,ETH-USDT,sell,0.50000000,1250.00,1.25,USDT,KU-O-2,KU-T-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "KUCOIN",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "order_id=KU-O-1 | trade_id=KU-T-1",
                },
                {
                    "type": "SELL",
                    "exchange": "KUCOIN",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "order_id=KU-O-2 | trade_id=KU-T-2",
                },
            ],
        ),
        (
            "crypto_com_exchange_trades.csv",
            "crypto_com_exchange_trades",
            "\n".join(
                [
                    "Timestamp (UTC),Instrument,Side,Quantity,Total,Fee,Fee Currency,Transaction ID",
                    "2024-01-02 03:04:05,BTC_USDT,BUY,0.01000000,420.00,0.10,USDT,CDC-1",
                    "2024-02-03 04:05:06,ETH_USDT,SELL,0.50000000,1250.00,1.25,USDT,CDC-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "CRYPTO_COM",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "tx=CDC-1",
                },
                {
                    "type": "SELL",
                    "exchange": "CRYPTO_COM",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "tx=CDC-2",
                },
            ],
        ),
        (
            "bitfinex_trades.csv",
            "bitfinex_trades",
            "\n".join(
                [
                    "Time,Pair,Amount,Price,Fee,Fee Currency,ID",
                    "2024-01-02 03:04:05,BTCUSD,0.01000000,42000.00,0.10,USD,BFX-1",
                    "2024-02-03 04:05:06,ETHUSD,-0.50000000,2500.00,1.25,USD,BFX-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "BITFINEX",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USD",
                    "quote_amount": "420.0000000000",
                    "fee_asset": "USD",
                    "fee_amount": "0.10",
                    "memo": "id=BFX-1",
                },
                {
                    "type": "SELL",
                    "exchange": "BITFINEX",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USD",
                    "quote_amount": "1250.0000000000",
                    "fee_asset": "USD",
                    "fee_amount": "1.25",
                    "memo": "id=BFX-2",
                },
            ],
        ),
        (
            "bitget_spot_trades.csv",
            "bitget_spot_trades",
            "\n".join(
                [
                    "Date,Symbol,Side,Quantity,Amount,Fee,Fee Coin,Order ID",
                    "2024-01-02 03:04:05,BTCUSDT,BUY,0.01000000,420.00,0.10,USDT,BG-1",
                    "2024-02-03 04:05:06,ETHUSDT,SELL,0.50000000,1250.00,1.25,USDT,BG-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "BITGET",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "order_id=BG-1",
                },
                {
                    "type": "SELL",
                    "exchange": "BITGET",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "order_id=BG-2",
                },
            ],
        ),
        (
            "gateio_trades.csv",
            "gateio_trades",
            "\n".join(
                [
                    "Time,Currency Pair,Side,Amount,Total,Fee,Fee Currency,Order ID",
                    "2024-01-02 03:04:05,BTC_USDT,buy,0.01000000,420.00,0.10,USDT,GATE-1",
                    "2024-02-03 04:05:06,ETH_USDT,sell,0.50000000,1250.00,1.25,USDT,GATE-2",
                ]
            ),
            [
                {
                    "type": "BUY",
                    "exchange": "GATEIO",
                    "base_asset": "BTC",
                    "base_amount": "0.01000000",
                    "quote_asset": "USDT",
                    "quote_amount": "420.00",
                    "fee_asset": "USDT",
                    "fee_amount": "0.10",
                    "memo": "order_id=GATE-1",
                },
                {
                    "type": "SELL",
                    "exchange": "GATEIO",
                    "base_asset": "ETH",
                    "base_amount": "0.50000000",
                    "quote_asset": "USDT",
                    "quote_amount": "1250.00",
                    "fee_asset": "USDT",
                    "fee_amount": "1.25",
                    "memo": "order_id=GATE-2",
                },
            ],
        ),
    ],
)
def test_more_exchange_trade_adapters_golden_outputs(
    filename,
    expected_source,
    csv_text,
    expected,
):
    rows, errors, meta = _parse(csv_text, filename=filename)

    assert errors == []
    assert meta["recognized_source_id"] == expected_source
    assert meta["recognized_source_status"] == "supported"

    assert len(rows) == len(expected)

    for row, exp in zip(rows, expected):
        assert row.type == exp["type"]
        assert row.exchange == exp["exchange"]
        assert row.base_asset == exp["base_asset"]
        assert row.base_amount == _dec(exp["base_amount"])
        assert row.quote_asset == exp["quote_asset"]
        assert row.quote_amount == _dec(exp["quote_amount"])
        assert row.fee_asset == exp["fee_asset"]
        assert row.fee_amount == _dec(exp["fee_amount"])
        assert row.memo == exp["memo"]
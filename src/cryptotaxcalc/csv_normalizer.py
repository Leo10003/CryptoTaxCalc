from __future__ import annotations

import csv
import io
import importlib
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import datetime as _dt

# Use a *unique* name for the stdlib module so nothing can shadow it
_dtmod = importlib.import_module("datetime")  # guarantees we get the module, not the class

# Import your pydantic Transaction model so the rest of the app stays the same
from .schemas import Transaction
from .csv_source_registry import CSVSourceMeta, detect_csv_source

# Accepted timestamp formats (extend if you need more)
_TS_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d",  # fallback: date-only
)

def _parse_ts(value: str) -> _dt.datetime:
    s = (value or "").strip()
    if not s:
        raise ValueError("empty timestamp")

    # tolerate trailing Z
    if s.endswith("Z"):
        s = s[:-1]

    # try ISO first
    try:
        return _dtmod.datetime.fromisoformat(s)
    except Exception:
        pass

    # try known formats
    for fmt in _TS_FORMATS:
        try:
            return _dtmod.datetime.strptime(s, fmt)
        except Exception:
            continue

    # last resort
    raise ValueError(f"unrecognized timestamp: {value!r}")

def parse_iso(ts: str) -> _dt.datetime:
    ts = ts.strip()
    # Handle 'Z' suffix:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return _dtmod.datetime.fromisoformat(ts)

def _dec(s: str | None) -> Decimal | None:
    if s is None:
        return None
    text = str(s).strip()
    if text == "" or text.lower() in {"null", "none"}:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        raise ValueError(f"invalid decimal: {s!r}")

def _upper_or_none(s: str | None) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    return t.upper() if t else None


class CSVFormatError(ValueError):
    """
    Raised when we cannot parse a CSV due to an unsupported/unknown structure.

    The `meta` payload is safe to return to the client (contains only structural info).
    """
    def __init__(self, message: str, meta: Dict[str, Any] | None = None):
        super().__init__(message)
        self.meta: Dict[str, Any] = meta or {}


def _sniff_dialect(text: str) -> csv.Dialect:
    """
    Try to sniff CSV dialect (delimiter/quotechar). Fall back to Excel dialect.

    This improves robustness for EU exports that commonly use ';' delimiters.
    """
    sample = (text or "")[:8192]
    try:
        return csv.Sniffer().sniff(sample)
    except Exception:
        return csv.excel


def _source_meta_to_dict(meta: CSVSourceMeta | None) -> Dict[str, Any]:
    if meta is None:
        return {
            "recognized_source_id": None,
            "recognized_source_name": None,
            "recognized_source_status": "unsupported",
            "recognized_source_confidence": 0.0,
            "recognized_source_signature": None,
        }
    return {
        "recognized_source_id": meta.source_id,
        "recognized_source_name": meta.source_name,
        "recognized_source_status": meta.status,
        "recognized_source_confidence": meta.confidence,
        "recognized_source_signature": meta.signature,
    }


def _parse_ledger_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    """
    Parse a Ledger Live / Ledger Wallet CSV export.

    Expected headers (case-insensitive):
      - Operation Date
      - Status
      - Currency Ticker
      - Operation Type (IN / OUT)
      - Operation Amount
      - Operation Fees
      - Countervalue Ticker (e.g. EUR)
      - Countervalue at Operation Date

    We map:
      Operation Date                 -> timestamp
      IN                             -> BUY
      OUT                            -> SELL
      Currency Ticker                -> base_asset
      Operation Amount               -> base_amount
      Countervalue Ticker            -> quote_asset
      Countervalue at Operation Date -> quote_amount
      Operation Fees (in asset)      -> fee_asset / fee_amount
    """
    out: List[Transaction] = []
    errors: List[str] = []

    for i, row in enumerate(reader, start=2):  # header is row 1
        try:
            ts_raw = row.get(header_map["operation date"], "") or ""
            ts = _parse_ts(ts_raw)

            status_raw = (row.get(header_map.get("status", ""), "") or "").strip().lower()
            # Skip non-confirmed operations (pending, failed, etc.)
            if status_raw and status_raw not in {"confirmed"}:
                continue

            base_asset = _upper_or_none(row.get(header_map.get("currency ticker", "")))
            if not base_asset:
                raise ValueError("missing Currency Ticker")

            op_type_raw = (row.get(header_map.get("operation type", ""), "") or "").strip().upper()
            
            # Operation Amount (base_amount) is required for all ledger rows
            # Use mapped header if present; fallback to the literal CSV header.
            amt_raw = row.get(header_map.get("operation amount", "Operation Amount"), "") or ""
            base_amount = _dec(amt_raw)
            if base_amount is None:
                raise ValueError("Operation Amount is required")
            
            # Drop zero-amount rows (they create phantom transfers and cannot be resolved meaningfully)
            try:
                if Decimal(str(base_amount)) == 0:
                    continue
            except Exception:
                pass

            # Ledger Operations:
            # - IN/OUT are wallet movements => TRANSFER
            # - BUY/SELL are resolved ledger rows => actual BUY/SELL trades
            if op_type_raw in {"IN", "OUT"}:
                tx_type = "TRANSFER"
            elif op_type_raw in {"BUY", "SELL"}:
                tx_type = op_type_raw  # keep as BUY/SELL
            else:
                # Skip fees / staking actions / unknown
                continue

            # Drop dust rows only for BUY/SELL (taxable rows), not for TRANSFER history rows
            try:
                if tx_type in {"BUY", "SELL"} and abs(Decimal(str(base_amount))) < Decimal("0.00000001"):
                    continue
            except Exception:
                pass
            
            # Normalize sign for wallet movements:
            # IN increases inventory (positive), OUT decreases inventory (negative).
            try:
                if op_type_raw == "OUT" and base_amount > 0:
                    base_amount = -base_amount
                elif op_type_raw == "IN" and base_amount < 0:
                    base_amount = -base_amount
            except Exception:
                pass

            fee_amount = _dec(row.get(header_map.get("operation fees", "")))
            fee_asset = base_asset if (fee_amount is not None and fee_amount > 0) else None
            
            # Resolved ledger rows (BUY/SELL) should not treat wallet network fees as trade fees.
            if tx_type in {"BUY", "SELL"}:
                fee_amount = None
                fee_asset = None
            
            # Persist Ledger countervalue (for later auto-fill of taxable proceeds)
            cv_ticker = _upper_or_none(row.get(header_map.get("countervalue ticker", "")))
            cv_amount = _dec(row.get(header_map.get("countervalue at operation date", "")))
            fair_value = (None if cv_amount is None else Decimal(str(cv_amount)))

            # Countervalue handling:
            # - For TRANSFER rows: keep quote empty (informational only)
            # - For BUY/SELL rows: treat countervalue as proceeds/cost in its native ticker (EUR or USD)
            if tx_type in {"BUY", "SELL"}:
                quote_asset = cv_ticker
                quote_amount = cv_amount
            else:
                quote_asset = None
                quote_amount = None

            # Optional memo: include hash + account name for audit
            memo_parts: List[str] = []
            op_hash = row.get(header_map.get("operation hash", ""), "")
            if op_hash:
                memo_parts.append(f"hash={op_hash}")
            acc_name = row.get(header_map.get("account name", ""), "")
            if acc_name:
                memo_parts.append(f"account={acc_name}")
            if cv_ticker:
                memo_parts.append(f"cv_ticker={cv_ticker}")
            memo = " | ".join(memo_parts) if memo_parts else ""

            tx = Transaction(
                timestamp=ts,
                type=tx_type,
                base_asset=(base_asset or ""),
                base_amount=Decimal(str(base_amount)),
                quote_asset=quote_asset,
                quote_amount=(None if quote_amount is None else Decimal(str(quote_amount))),
                fee_asset=fee_asset,
                fee_amount=(None if fee_amount is None else Decimal(str(fee_amount))),
                exchange="LEDGER_LIVE",
                memo=memo or None,
                fair_value=fair_value,
            )
            out.append(tx)

        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors

def _split_binance_symbol(symbol: str, quote_asset: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    s = (symbol or "").strip().upper()
    if not s:
        return None, None

    # Common Binance formats:
    # - BTCUSDT
    # - BTC/USDT
    # - BTC-USDT
    if "/" in s:
        parts = [p for p in s.split("/") if p]
        if len(parts) == 2:
            return parts[0], parts[1]
    if "-" in s:
        parts = [p for p in s.split("-") if p]
        if len(parts) == 2:
            return parts[0], parts[1]

    q = (quote_asset or "").strip().upper()
    if q and s.endswith(q) and len(s) > len(q):
        return s[:-len(q)], q

    # Fallback: infer quote by common suffixes.
    # NOTE: Some symbols are ambiguous (e.g., "BNBUSD" could be "BNB"+"USD" or "BN"+"BUSD").
    # We skip matches that would produce an implausibly short base code (<3 chars).
    common_quotes = ["FDUSD", "USDT", "USDC", "BUSD", "USD", "EUR", "GBP", "BTC", "ETH", "BNB", "TRY", "BRL"]
    for cq in common_quotes:
        if not (s.endswith(cq) and len(s) > len(cq)):
            continue
        base = s[:-len(cq)]
        if len(base) < 3:
            continue
        return base, cq

    return None, None


def _parse_binance_spot_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    """
    Parse a Binance Spot Trades CSV export.

    Supported headers (case-insensitive):
      - Date(UTC)
      - Symbol
      - Side (BUY/SELL)
      - Price
      - Quantity (base quantity)
      - Amount (quote amount)
      - Fee (optional)
      - Fee Coin (optional)
      - Quote Asset (optional)
    """
    out: List[Transaction] = []
    errors: List[str] = []

    fee_non_quote_count = 0
    fee_non_quote_examples = 0

    k_date = header_map.get("date(utc)")
    k_symbol = header_map.get("symbol")
    k_side = header_map.get("side")
    k_price = header_map.get("price")
    k_qty = header_map.get("quantity")
    k_amount = header_map.get("amount") or header_map.get("total")
    k_fee = header_map.get("fee")
    k_fee_coin = header_map.get("fee coin")
    k_quote_asset = header_map.get("quote asset")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_date or not k_symbol or not k_side or not k_price or not k_qty or not k_amount:
                raise ValueError("missing required Binance columns")

            ts = _parse_ts(row.get(k_date, "") or "")
            symbol = (row.get(k_symbol, "") or "").strip().upper()
            side = (row.get(k_side, "") or "").strip().upper()

            price = _dec(row.get(k_price, ""))
            qty = _dec(row.get(k_qty, ""))
            amount = _dec(row.get(k_amount, ""))

            if price is None or qty is None or amount is None:
                raise ValueError("missing numeric fields (price/quantity/amount)")

            quote_asset = _upper_or_none(row.get(k_quote_asset, "")) if k_quote_asset else None
            base_asset, quote_inferred = _split_binance_symbol(symbol, quote_asset)
            if not quote_asset:
                quote_asset = quote_inferred

            if not base_asset or not quote_asset:
                raise ValueError(f"cannot infer base/quote from symbol {symbol!r}")

            if side == "BUY":
                tx_type = "BUY"
            elif side == "SELL":
                tx_type = "SELL"
            else:
                raise ValueError(f"unsupported Side {side!r}")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _upper_or_none(row.get(k_fee_coin, "")) if k_fee_coin else None

            # Non-quote fee handling is surfaced by the FIFO engine as a single aggregated warning,
            # so we do not treat this as a parse error here (avoids false "skipped_errors").
            if fee_amount is not None and fee_asset and fee_asset != quote_asset:
                fee_non_quote_count += 1

            tx = Transaction(
                timestamp=ts,
                type=tx_type,
                base_asset=base_asset,
                base_amount=Decimal(str(qty)),
                quote_asset=quote_asset,
                quote_amount=Decimal(str(amount)),
                fee_asset=fee_asset,
                fee_amount=(None if fee_amount is None else Decimal(str(fee_amount))),
                exchange="BINANCE",
                memo=f"symbol={symbol}; side={side}",
                fair_value=None,
            )
            out.append(tx)

        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _norm_asset_code(asset: str | None) -> Optional[str]:
    a = _upper_or_none(asset)
    if not a:
        return None
    if a == "XBT":
        return "BTC"
    return a


def _split_pair_guess(pair: str, quote_hint: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    s = (pair or "").strip().upper()
    if not s:
        return None, None

    for sep in ("/", "-", "_"):
        if sep in s:
            parts = [p for p in s.split(sep) if p]
            if len(parts) == 2:
                return parts[0], parts[1]

    qh = (quote_hint or "").strip().upper()
    if qh and s.endswith(qh) and len(s) > len(qh):
        return s[:-len(qh)], qh

    common_quotes = ["USDT", "USDC", "FDUSD", "BUSD", "USD", "EUR", "GBP", "TRY", "BRL", "BTC", "ETH", "BNB"]
    for q in sorted(common_quotes, key=len, reverse=True):
        if not (s.endswith(q) and len(s) > len(q)):
            continue
        base = s[:-len(q)]
        if len(base) < 3:
            continue
        return base, q

    return None, None


def _parse_coinbase_transactions_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_ts = header_map.get("timestamp")
    k_type = header_map.get("transaction type")
    k_asset = header_map.get("asset")
    k_qty = header_map.get("quantity transacted")
    k_quote = header_map.get("spot price currency")
    k_subtotal = header_map.get("subtotal")
    k_fees = header_map.get("fees")
    k_notes = header_map.get("notes")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_ts or not k_type or not k_asset or not k_qty or not k_quote or not k_subtotal:
                raise ValueError("missing required Coinbase columns")

            ts = _parse_ts(row.get(k_ts, "") or "")
            raw_tt = (row.get(k_type, "") or "").strip().lower()

            if raw_tt.startswith("buy"):
                tx_type = "BUY"
            elif raw_tt.startswith("sell"):
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported Transaction Type {raw_tt!r}")
                continue

            base_asset = _norm_asset_code(row.get(k_asset, ""))
            qty = _dec(row.get(k_qty, ""))
            if base_asset is None or qty is None:
                raise ValueError("asset/quantity missing")

            quote_asset = _upper_or_none(row.get(k_quote, "")) or "USD"
            subtotal = _dec(row.get(k_subtotal, ""))
            if subtotal is None:
                raise ValueError("subtotal missing")

            fee_amount = _dec(row.get(k_fees, "")) if k_fees else None
            fee_asset = quote_asset if fee_amount is not None else None

            memo = (row.get(k_notes, "") or "").strip() if k_notes else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base_asset,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote_asset,
                    quote_amount=Decimal(str(abs(subtotal))),
                    fee_asset=fee_asset,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="COINBASE",
                    memo=memo or None,
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_kraken_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("time")
    k_type = header_map.get("type")
    k_pair = header_map.get("pair")
    k_cost = header_map.get("cost")
    k_fee = header_map.get("fee")
    k_vol = header_map.get("vol")
    k_txid = header_map.get("txid")
    k_order = header_map.get("ordertxid")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_type or not k_pair or not k_cost or not k_fee or not k_vol:
                raise ValueError("missing required Kraken columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_type, "") or "").strip().lower()

            if side == "buy":
                tx_type = "BUY"
            elif side == "sell":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported type {side!r}")
                continue

            pair = (row.get(k_pair, "") or "").strip().upper()
            base, quote = _split_pair_guess(pair)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base from pair {pair!r}")

            qty = _dec(row.get(k_vol, ""))
            cost = _dec(row.get(k_cost, ""))
            fee = _dec(row.get(k_fee, ""))

            if qty is None or cost is None:
                raise ValueError("missing vol/cost")

            txid = (row.get(k_txid, "") or "").strip() if k_txid else ""
            order = (row.get(k_order, "") or "").strip() if k_order else ""
            memo_parts = []
            if txid:
                memo_parts.append(f"txid={txid}")
            if order:
                memo_parts.append(f"order={order}")
            memo = " | ".join(memo_parts)

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(cost))),
                    fee_asset=quote if fee is not None else None,
                    fee_amount=(None if fee is None else Decimal(str(abs(fee)))),
                    exchange="KRAKEN",
                    memo=memo or None,
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_okx_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("time")
    k_inst = header_map.get("instrument")
    k_side = header_map.get("side")
    k_size = header_map.get("size")
    k_value = header_map.get("trade value")
    k_fee = header_map.get("fee")
    k_fee_ccy = header_map.get("fee currency")
    k_trade_id = header_map.get("trade id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_inst or not k_side or not k_size or not k_value:
                raise ValueError("missing required OKX columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_side, "") or "").strip().upper()

            if side == "BUY":
                tx_type = "BUY"
            elif side == "SELL":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            inst = (row.get(k_inst, "") or "").strip().upper()
            base, quote = _split_pair_guess(inst)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base/quote from instrument {inst!r}")

            qty = _dec(row.get(k_size, ""))
            value = _dec(row.get(k_value, ""))
            if qty is None or value is None:
                raise ValueError("missing size/value")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            if fee_amount is not None and fee_asset is None:
                fee_asset = quote

            tid = (row.get(k_trade_id, "") or "").strip() if k_trade_id else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(value))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="OKX",
                    memo=(f"trade_id={tid}" if tid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_bybit_executions_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("exec time")
    k_order_id = header_map.get("order id")
    k_symbol = header_map.get("symbol")
    k_side = header_map.get("side")
    k_qty = header_map.get("exec qty")
    k_value = header_map.get("exec value")
    k_fee = header_map.get("exec fee")
    k_fee_ccy = header_map.get("fee currency")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_symbol or not k_side or not k_qty or not k_value:
                raise ValueError("missing required Bybit columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_side, "") or "").strip().upper()

            if side == "BUY":
                tx_type = "BUY"
            elif side == "SELL":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            sym = (row.get(k_symbol, "") or "").strip().upper()
            base, quote = _split_pair_guess(sym)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base/quote from symbol {sym!r}")

            qty = _dec(row.get(k_qty, ""))
            value = _dec(row.get(k_value, ""))
            if qty is None or value is None:
                raise ValueError("missing exec qty/value")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            if fee_amount is not None and fee_asset is None:
                fee_asset = quote

            oid = (row.get(k_order_id, "") or "").strip() if k_order_id else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(value))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="BYBIT",
                    memo=(f"order_id={oid}" if oid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_kucoin_fills_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("time")
    k_symbol = header_map.get("symbol")
    k_side = header_map.get("side")
    k_size = header_map.get("size")
    k_funds = header_map.get("funds")
    k_fee = header_map.get("fee")
    k_fee_ccy = header_map.get("fee currency")
    k_oid = header_map.get("order id")
    k_tid = header_map.get("trade id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_symbol or not k_side or not k_size or not k_funds:
                raise ValueError("missing required KuCoin columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_side, "") or "").strip().lower()

            if side == "buy":
                tx_type = "BUY"
            elif side == "sell":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            sym = (row.get(k_symbol, "") or "").strip().upper()
            base, quote = _split_pair_guess(sym)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base/quote from symbol {sym!r}")

            qty = _dec(row.get(k_size, ""))
            funds = _dec(row.get(k_funds, ""))
            if qty is None or funds is None:
                raise ValueError("missing size/funds")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            if fee_amount is not None and fee_asset is None:
                fee_asset = quote

            oid = (row.get(k_oid, "") or "").strip() if k_oid else ""
            tid = (row.get(k_tid, "") or "").strip() if k_tid else ""
            memo_parts = []
            if oid:
                memo_parts.append(f"order_id={oid}")
            if tid:
                memo_parts.append(f"trade_id={tid}")
            memo = " | ".join(memo_parts)

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(funds))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="KUCOIN",
                    memo=memo or None,
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_crypto_com_exchange_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("timestamp (utc)")
    k_inst = header_map.get("instrument")
    k_side = header_map.get("side")
    k_qty = header_map.get("quantity")
    k_total = header_map.get("total")
    k_fee = header_map.get("fee")
    k_fee_ccy = header_map.get("fee currency")
    k_tid = header_map.get("transaction id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_inst or not k_side or not k_qty or not k_total:
                raise ValueError("missing required Crypto.com columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_side, "") or "").strip().upper()

            if side == "BUY":
                tx_type = "BUY"
            elif side == "SELL":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            inst = (row.get(k_inst, "") or "").strip().upper()
            base, quote = _split_pair_guess(inst)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base/quote from instrument {inst!r}")

            qty = _dec(row.get(k_qty, ""))
            total = _dec(row.get(k_total, ""))
            if qty is None or total is None:
                raise ValueError("missing quantity/total")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            if fee_amount is not None and fee_asset is None:
                fee_asset = quote

            tid = (row.get(k_tid, "") or "").strip() if k_tid else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(total))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="CRYPTO_COM",
                    memo=(f"tx={tid}" if tid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_bitfinex_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("time")
    k_pair = header_map.get("pair")
    k_amount = header_map.get("amount")
    k_price = header_map.get("price")
    k_fee = header_map.get("fee")
    k_fee_ccy = header_map.get("fee currency")
    k_id = header_map.get("id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_pair or not k_amount or not k_price:
                raise ValueError("missing required Bitfinex columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            pair = (row.get(k_pair, "") or "").strip().upper()
            base, quote = _split_pair_guess(pair)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USD"
            if not base:
                raise ValueError(f"cannot infer base/quote from pair {pair!r}")

            amt = _dec(row.get(k_amount, ""))
            price = _dec(row.get(k_price, ""))
            if amt is None or price is None:
                raise ValueError("missing amount/price")

            tx_type = "BUY" if amt >= 0 else "SELL"
            qty = abs(amt)
            quote_amount = qty * price

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            bid = (row.get(k_id, "") or "").strip() if k_id else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(qty)),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(quote_amount))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="BITFINEX",
                    memo=(f"id={bid}" if bid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_bitget_spot_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_date = header_map.get("date")
    k_symbol = header_map.get("symbol")
    k_side = header_map.get("side")
    k_qty = header_map.get("quantity")
    k_amount = header_map.get("amount")
    k_fee = header_map.get("fee")
    k_fee_coin = header_map.get("fee coin")
    k_oid = header_map.get("order id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_date or not k_symbol or not k_side or not k_qty or not k_amount:
                raise ValueError("missing required Bitget columns")

            ts = _parse_ts(row.get(k_date, "") or "")
            side = (row.get(k_side, "") or "").strip().upper()

            if side == "BUY":
                tx_type = "BUY"
            elif side == "SELL":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            sym = (row.get(k_symbol, "") or "").strip().upper()
            base, quote = _split_pair_guess(sym)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USDT"
            if not base:
                raise ValueError(f"cannot infer base/quote from symbol {sym!r}")

            qty = _dec(row.get(k_qty, ""))
            amount = _dec(row.get(k_amount, ""))
            if qty is None or amount is None:
                raise ValueError("missing quantity/amount")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_coin, "")) if k_fee_coin else None
            oid = (row.get(k_oid, "") or "").strip() if k_oid else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(amount))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="BITGET",
                    memo=(f"order_id={oid}" if oid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


def _parse_gateio_trades_csv(reader, header_map) -> Tuple[List[Transaction], List[str]]:
    out: List[Transaction] = []
    errors: List[str] = []

    k_time = header_map.get("time")
    k_pair = header_map.get("currency pair")
    k_side = header_map.get("side")
    k_amount = header_map.get("amount")
    k_total = header_map.get("total")
    k_fee = header_map.get("fee")
    k_fee_ccy = header_map.get("fee currency")
    k_oid = header_map.get("order id")

    for i, row in enumerate(reader, start=2):
        try:
            if not k_time or not k_pair or not k_side or not k_amount or not k_total:
                raise ValueError("missing required Gate.io columns")

            ts = _parse_ts(row.get(k_time, "") or "")
            side = (row.get(k_side, "") or "").strip().lower()

            if side == "buy":
                tx_type = "BUY"
            elif side == "sell":
                tx_type = "SELL"
            else:
                errors.append(f"row {i}: unsupported side {side!r}")
                continue

            pair = (row.get(k_pair, "") or "").strip().upper()
            base, quote = _split_pair_guess(pair)
            base = _norm_asset_code(base)
            quote = _norm_asset_code(quote) or "USDT"
            if not base:
                raise ValueError(f"cannot infer base/quote from pair {pair!r}")

            qty = _dec(row.get(k_amount, ""))
            total = _dec(row.get(k_total, ""))
            if qty is None or total is None:
                raise ValueError("missing amount/total")

            fee_amount = _dec(row.get(k_fee, "")) if k_fee else None
            fee_asset = _norm_asset_code(row.get(k_fee_ccy, "")) if k_fee_ccy else None
            if fee_amount is not None and fee_asset is None:
                fee_asset = quote

            oid = (row.get(k_oid, "") or "").strip() if k_oid else ""

            out.append(
                Transaction(
                    timestamp=ts,
                    type=tx_type,
                    base_asset=base,
                    base_amount=Decimal(str(abs(qty))),
                    quote_asset=quote,
                    quote_amount=Decimal(str(abs(total))),
                    fee_asset=fee_asset if fee_amount is not None else None,
                    fee_amount=(None if fee_amount is None else Decimal(str(abs(fee_amount)))),
                    exchange="GATEIO",
                    memo=(f"order_id={oid}" if oid else None),
                    fair_value=None,
                )
            )
        except Exception as e:
            errors.append(f"row {i}: {e}")

    return out, errors


# =========================================================
# Parser registry (source_id -> parser function)
# This is the single source of truth for "supported source has a parser".
# =========================================================
PARSER_BY_SOURCE_ID = {
    "ledger_live": _parse_ledger_csv,
    "binance_spot_trades": _parse_binance_spot_trades_csv,
    "coinbase_transactions": _parse_coinbase_transactions_csv,
    "kraken_trades": _parse_kraken_trades_csv,
    "okx_trades": _parse_okx_trades_csv,
    "bybit_executions": _parse_bybit_executions_csv,
    "kucoin_fills": _parse_kucoin_fills_csv,
    "crypto_com_exchange_trades": _parse_crypto_com_exchange_trades_csv,
    "bitfinex_trades": _parse_bitfinex_trades_csv,
    "bitget_spot_trades": _parse_bitget_spot_trades_csv,
    "gateio_trades": _parse_gateio_trades_csv,
}


def parse_csv_stream_with_meta(
    text_io: io.TextIOBase,
    filename: str | None = None,
) -> Tuple[List[Transaction], List[str], Dict[str, Any]]:
    """
    Streaming variant of parse_csv_with_meta:

    - Accepts a decoded text stream (TextIO) instead of raw bytes.
    - Avoids allocating one huge in-memory string for large CSV files.
    - Keeps identical output shape: (rows, errors, meta).
    """
    # Dialect sniff: read a small sample then rewind if possible
    pos = None
    try:
        pos = text_io.tell()
    except Exception:
        pos = None

    sample = text_io.read(8192) or ""
    dialect = _sniff_dialect(sample)

    # Rewind if we can; otherwise fall back to an in-memory combined buffer
    if pos is not None:
        try:
            text_io.seek(pos)
        except Exception:
            text_io = io.StringIO(sample + (text_io.read() or ""))
    else:
        text_io = io.StringIO(sample + (text_io.read() or ""))

    reader = csv.DictReader(text_io, dialect=dialect)

    # Map headers (case-insensitive)
    header_map: dict[str, str] = {}
    headers_raw: List[str] = []
    normalized_headers: List[str] = []

    if reader.fieldnames:
        headers_raw = [str(h) for h in reader.fieldnames if h is not None]
        for h in reader.fieldnames:
            if h is None:
                continue
            normalized = str(h).lower().strip()
            if normalized:
                normalized_headers.append(normalized)
            header_map[normalized] = str(h)

    blank_headers = [
        str(h)
        for h in (reader.fieldnames or [])
        if h is None or not str(h).strip()
    ]
    if blank_headers:
        meta_obj = detect_csv_source(
            headers=headers_raw,
            filename=filename,
            delimiter=getattr(dialect, "delimiter", None),
            quotechar=getattr(dialect, "quotechar", None),
        )
        raise CSVFormatError(
            "Blank CSV header(s) are not allowed",
            meta=_source_meta_to_dict(meta_obj),
        )

    duplicate_headers = sorted(
        {h for h in normalized_headers if normalized_headers.count(h) > 1}
    )
    if duplicate_headers:
        meta_obj = detect_csv_source(
            headers=headers_raw,
            filename=filename,
            delimiter=getattr(dialect, "delimiter", None),
            quotechar=getattr(dialect, "quotechar", None),
        )
        raise CSVFormatError(
            f"Duplicate CSV header(s): {', '.join(duplicate_headers)}",
            meta=_source_meta_to_dict(meta_obj),
        )

    meta_obj = detect_csv_source(
        headers=headers_raw,
        filename=filename,
        delimiter=getattr(dialect, "delimiter", None),
        quotechar=getattr(dialect, "quotechar", None),
    )
    meta = _source_meta_to_dict(meta_obj)

    if not header_map:
        raise CSVFormatError(
            "Invalid CSV header row (no columns detected).",
            meta=meta,
        )

    # Source-driven routing (single registry)
    parser = PARSER_BY_SOURCE_ID.get(meta_obj.source_id) if meta_obj else None
    if parser:
        rows, errors = parser(reader, header_map)
        return rows, errors, meta

    # Default (generic) CSV format: require normalized headers
    required = ["timestamp", "type", "base_asset", "base_amount"]
    missing = [h for h in required if h not in header_map]
    if missing:
        raise CSVFormatError(
            "Unrecognized CSV format. The file structure has been saved for implementation. "
            "For now, export using a supported format (e.g., Ledger Live / Binance) or use the CryptoTaxCalc normalized template.",
            meta=meta,
        )

    out: List[Transaction] = []
    errors: List[str] = []

    for i, row in enumerate(reader, start=2):  # header is row 1
        try:
            ts = _parse_ts(row[header_map["timestamp"]])

            ttype = (row.get(header_map.get("type", ""), "") or "").strip().lower()
            if not ttype:
                raise ValueError("type is required")

            if ttype in {"transfer_in", "transfer_out", "transfer-in", "transfer-out"}:
                ttype = "transfer"

            if ttype not in {"buy", "sell", "transfer"}:
                raise ValueError(f"unsupported type: {ttype!r}")

            base_asset = _upper_or_none(row.get(header_map.get("base_asset", "")))
            if not base_asset:
                raise ValueError("base_asset is required")

            base_amount = _dec(row.get(header_map.get("base_amount", "")))
            if base_amount is None:
                raise ValueError("base_amount is required")
            if ttype in {"buy", "sell"} and base_amount <= 0:
                raise ValueError("base_amount must be positive for BUY/SELL rows")

            quote_asset = _upper_or_none(row.get(header_map.get("quote_asset", "")))
            quote_amount = _dec(row.get(header_map.get("quote_amount", "")))

            if ttype in {"buy", "sell"}:
                if not quote_asset:
                    raise ValueError("quote_asset is required for BUY/SELL rows")
                if quote_amount is None:
                    raise ValueError("quote_amount is required for BUY/SELL rows")
                if quote_amount <= 0:
                    raise ValueError("quote_amount must be positive for BUY/SELL rows")

            fee_asset = _upper_or_none(row.get(header_map.get("fee_asset", "")))
            fee_amount = _dec(row.get(header_map.get("fee_amount", "")))

            if fee_amount is not None and fee_amount < 0:
                raise ValueError("fee_amount must be zero or positive")
            if fee_amount is not None and fee_amount > 0 and not fee_asset:
                raise ValueError("fee_asset is required when fee_amount is positive")
            if fee_asset and fee_amount is None:
                raise ValueError("fee_amount is required when fee_asset is provided")

            exchange = (row.get(header_map.get("exchange", ""), "") or "").strip()
            memo = (row.get(header_map.get("memo", ""), "") or "").strip()
            fair_value = _dec(row.get(header_map.get("fair_value", "")))

            tx = Transaction(
                timestamp=ts,
                type=ttype,
                base_asset=base_asset,
                base_amount=Decimal(str(base_amount)),
                quote_asset=quote_asset,
                quote_amount=(None if quote_amount is None else Decimal(str(quote_amount))),
                fee_asset=fee_asset,
                fee_amount=(None if fee_amount is None else Decimal(str(fee_amount))),
                exchange=exchange or None,
                memo=memo or None,
                fair_value=(None if fair_value is None else Decimal(str(fair_value))),
            )
            out.append(tx)

        except Exception as e:
            snippet = {
                k: row.get(v)
                for k, v in header_map.items()
                if k in {"timestamp", "type", "base_asset", "base_amount"}
            }
            errors.append(f"row {i}: {e} | snippet={snippet}")

    return out, errors, meta


def parse_csv_with_meta(raw_bytes: bytes, filename: str | None = None) -> Tuple[List[Transaction], List[str], Dict[str, Any]]:
    """
    Parse a CSV payload into normalized Transaction objects and return structural metadata.

    Supports:
      - CryptoTaxCalc normalized format: timestamp, type, base_asset, base_amount, ...
      - Ledger Live / Ledger Wallet exports: Operation Date, Currency Ticker, Operation Type, ...

    Metadata (`meta`) includes recognized source info and a stable header signature.
    """
    text = raw_bytes.decode("utf-8-sig", errors="replace")
    dialect = _sniff_dialect(text)
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)

    # Map headers (case-insensitive)
    header_map: dict[str, str] = {}
    headers_raw: List[str] = []
    normalized_headers: List[str] = []

    if reader.fieldnames:
        headers_raw = [str(h) for h in reader.fieldnames if h is not None]
        for h in reader.fieldnames:
            if h is None:
                continue
            normalized = str(h).lower().strip()
            if normalized:
                normalized_headers.append(normalized)
            header_map[normalized] = str(h)

    blank_headers = [
        str(h)
        for h in (reader.fieldnames or [])
        if h is None or not str(h).strip()
    ]
    if blank_headers:
        meta_obj = detect_csv_source(
            headers=headers_raw,
            filename=filename,
            delimiter=getattr(dialect, "delimiter", None),
            quotechar=getattr(dialect, "quotechar", None),
        )
        raise CSVFormatError(
            "Blank CSV header(s) are not allowed",
            meta=_source_meta_to_dict(meta_obj),
        )

    duplicate_headers = sorted(
        {h for h in normalized_headers if normalized_headers.count(h) > 1}
    )
    if duplicate_headers:
        meta_obj = detect_csv_source(
            headers=headers_raw,
            filename=filename,
            delimiter=getattr(dialect, "delimiter", None),
            quotechar=getattr(dialect, "quotechar", None),
        )
        raise CSVFormatError(
            f"Duplicate CSV header(s): {', '.join(duplicate_headers)}",
            meta=_source_meta_to_dict(meta_obj),
        )

    meta_obj = detect_csv_source(
        headers=headers_raw,
        filename=filename,
        delimiter=getattr(dialect, "delimiter", None),
        quotechar=getattr(dialect, "quotechar", None),
    )
    meta = _source_meta_to_dict(meta_obj)

    # If we cannot even see headers, treat as unsupported
    if not header_map:
        raise CSVFormatError(
            "Invalid CSV header row (no columns detected).",
            meta=meta,
        )

    # Source-driven routing (single registry)
    parser = PARSER_BY_SOURCE_ID.get(meta_obj.source_id) if meta_obj else None
    if parser:
        rows, errors = parser(reader, header_map)
        return rows, errors, meta

    # Default (generic) CSV format: require normalized headers
    required = ["timestamp", "type", "base_asset", "base_amount"]
    missing = [h for h in required if h not in header_map]
    if missing:
        # We already recorded this structure as unsupported in the registry layer.
        raise CSVFormatError(
            "Unrecognized CSV format. The file structure has been saved for implementation. "
            "For now, export using a supported format (e.g., Ledger Live) or use the CryptoTaxCalc normalized template.",
            meta=meta,
        )

    out: List[Transaction] = []
    errors: List[str] = []

    for i, row in enumerate(reader, start=2):  # header is row 1
        try:
            ts = _parse_ts(row[header_map["timestamp"]])

            ttype = (row.get(header_map.get("type", ""), "") or "").strip().lower()
            if not ttype:
                raise ValueError("type is required")

            # Normalize transfer variants to a single canonical type.
            if ttype in {"transfer_in", "transfer_out", "transfer-in", "transfer-out"}:
                ttype = "transfer"

            if ttype not in {"buy", "sell", "transfer"}:
                raise ValueError(f"unsupported type: {ttype!r}")

            base_asset = _upper_or_none(row.get(header_map.get("base_asset", "")))
            if not base_asset:
                raise ValueError("base_asset is required")

            base_amount = _dec(row.get(header_map.get("base_amount", "")))
            if base_amount is None:
                raise ValueError("base_amount is required")
            if ttype in {"buy", "sell"} and base_amount <= 0:
                raise ValueError("base_amount must be positive for BUY/SELL rows")

            quote_asset = _upper_or_none(row.get(header_map.get("quote_asset", "")))
            quote_amount = _dec(row.get(header_map.get("quote_amount", "")))

            if ttype in {"buy", "sell"}:
                if not quote_asset:
                    raise ValueError("quote_asset is required for BUY/SELL rows")
                if quote_amount is None:
                    raise ValueError("quote_amount is required for BUY/SELL rows")
                if quote_amount <= 0:
                    raise ValueError("quote_amount must be positive for BUY/SELL rows")

            fee_asset = _upper_or_none(row.get(header_map.get("fee_asset", "")))
            fee_amount = _dec(row.get(header_map.get("fee_amount", "")))

            if fee_amount is not None and fee_amount < 0:
                raise ValueError("fee_amount must be zero or positive")
            if fee_amount is not None and fee_amount > 0 and not fee_asset:
                raise ValueError("fee_asset is required when fee_amount is positive")
            if fee_asset and fee_amount is None:
                raise ValueError("fee_amount is required when fee_asset is provided")

            exchange = (row.get(header_map.get("exchange", ""), "") or "").strip()
            memo = (row.get(header_map.get("memo", ""), "") or "").strip()
            fair_value = _dec(row.get(header_map.get("fair_value", "")))

            tx = Transaction(
                timestamp=ts,
                type=ttype,
                base_asset=base_asset,
                base_amount=Decimal(str(base_amount)),
                quote_asset=quote_asset,
                quote_amount=(None if quote_amount is None else Decimal(str(quote_amount))),
                fee_asset=fee_asset,
                fee_amount=(None if fee_amount is None else Decimal(str(fee_amount))),
                exchange=exchange or None,
                memo=memo or None,
                fair_value=(None if fair_value is None else Decimal(str(fair_value))),
            )
            out.append(tx)

        except Exception as e:
            # Attach the original row to the error to speed up debugging
            snippet = {k: row.get(v) for k, v in header_map.items() if k in {"timestamp","type","base_asset","base_amount"}}
            errors.append(f"row {i}: {e} | snippet={snippet}")

    return out, errors, meta


def parse_csv(raw_bytes: bytes) -> Tuple[List[Transaction], List[str]]:
    """
    Backward-compatible wrapper used by legacy call sites.

    Prefer `parse_csv_with_meta(...)` for source detection + better UX.
    """
    out, errors, _meta = parse_csv_with_meta(raw_bytes, filename=None)
    return out, errors

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REGISTRY_DIR = Path("storage_raw") / "csv_sources"
_SUPPORTED_PATH = _REGISTRY_DIR / "supported_sources.json"
_UNSUPPORTED_PATH = _REGISTRY_DIR / "unsupported_structures.json"

_DEFAULT_SUPPORTED: Dict[str, Any] = {
    "version": 1,
    "sources": [
        {
            "id": "ledger_live",
            "name": "Ledger Live",
            "status": "supported",
            "parser": "ledger_live",
            "match": {
                "headers_all": [
                    "operation date",
                    "currency ticker",
                    "operation type",
                    "operation amount",
                ],
                "headers_any": [
                    "status",
                    "operation fees",
                    "countervalue ticker",
                    "countervalue at operation date",
                    "operation hash",
                    "account name",
                ],
                "filename_keywords": ["ledger", "ledgerlive", "ledger_live"],
            },
        },
        {
            "id": "cryptotaxcalc_generic",
            "name": "CryptoTaxCalc (Normalized)",
            "status": "supported",
            "parser": "generic",
            "match": {
                "headers_all": ["timestamp", "type", "base_asset", "base_amount"],
                "headers_any": [
                    "quote_asset",
                    "quote_amount",
                    "fee_asset",
                    "fee_amount",
                    "exchange",
                    "memo",
                    "fair_value",
                ],
                "filename_keywords": ["cryptotaxcalc", "normalized"],
            },
        },
        {
            "id": "binance_spot_trades",
            "name": "Binance Spot Trades",
            "status": "supported",
            "parser": "binance_spot_trades",
            "match": {
                "headers_all": [
                    "date(utc)",
                    "symbol",
                    "side",
                    "price",
                    "quantity",
                    "amount",
                ],
                "headers_any": [
                    "fee",
                    "fee coin",
                    "quote asset",
                ],
                "filename_keywords": ["binance", "spot", "trade", "trades"],
            },
        },
        {
            "id": "coinbase_transactions",
            "name": "Coinbase Transactions",
            "status": "supported",
            "parser": "coinbase_transactions",
            "match": {
                "headers_all": [
                    "timestamp",
                    "transaction type",
                    "asset",
                    "quantity transacted",
                    "spot price currency",
                    "subtotal",
                    "fees",
                ],
                "headers_any": [
                    "spot price at transaction",
                    "total (inclusive of fees)",
                    "notes",
                ],
                "filename_keywords": ["coinbase"],
            },
        },
        {
            "id": "kraken_trades",
            "name": "Kraken Trades",
            "status": "supported",
            "parser": "kraken_trades",
            "match": {
                "headers_all": [
                    "txid",
                    "ordertxid",
                    "pair",
                    "time",
                    "type",
                    "price",
                    "cost",
                    "fee",
                    "vol",
                ],
                "headers_any": [
                    "ordertype",
                    "margin",
                    "misc",
                    "ledgers",
                ],
                "filename_keywords": ["kraken"],
            },
        },
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
                    "size",
                    "trade value",
                ],
                "headers_any": [
                    "type",
                    "price",
                    "fee",
                    "fee currency",
                    "trade id",
                ],
                "filename_keywords": ["okx", "okex"],
            },
        },
        {
            "id": "bybit_executions",
            "name": "Bybit Executions",
            "status": "supported",
            "parser": "bybit_executions",
            "match": {
                "headers_all": [
                    "exec time",
                    "symbol",
                    "side",
                    "exec qty",
                    "exec value",
                ],
                "headers_any": [
                    "order id",
                    "exec price",
                    "exec fee",
                    "fee currency",
                    "order type",
                    "order price",
                    "order qty",
                ],
                "filename_keywords": ["bybit"],
            },
        },
        {
            "id": "kucoin_fills",
            "name": "KuCoin Fills",
            "status": "supported",
            "parser": "kucoin_fills",
            "match": {
                "headers_all": [
                    "time",
                    "symbol",
                    "side",
                    "size",
                    "funds",
                ],
                "headers_any": [
                    "price",
                    "fee",
                    "fee currency",
                    "order id",
                    "trade id",
                ],
                "filename_keywords": ["kucoin"],
            },
        },
        {
            "id": "crypto_com_exchange_trades",
            "name": "Crypto.com Exchange Trades",
            "status": "supported",
            "parser": "crypto_com_exchange_trades",
            "match": {
                "headers_all": [
                    "timestamp (utc)",
                    "instrument",
                    "side",
                    "quantity",
                    "total",
                ],
                "headers_any": [
                    "price",
                    "fee",
                    "fee currency",
                    "transaction id",
                ],
                "filename_keywords": ["crypto", "cryptocom", "crypto_com"],
            },
        },
        {
            "id": "bitfinex_trades",
            "name": "Bitfinex Trades",
            "status": "supported",
            "parser": "bitfinex_trades",
            "match": {
                "headers_all": [
                    "id",
                    "pair",
                    "amount",
                    "price",
                    "fee",
                    "fee currency",
                    "time",
                ],
                "headers_any": [],
                "filename_keywords": ["bitfinex"],
            },
        },
        {
            "id": "bitget_spot_trades",
            "name": "Bitget Spot Trades",
            "status": "supported",
            "parser": "bitget_spot_trades",
            "match": {
                "headers_all": [
                    "date",
                    "symbol",
                    "side",
                    "quantity",
                    "amount",
                ],
                "headers_any": [
                    "price",
                    "fee",
                    "fee coin",
                    "order id",
                ],
                "filename_keywords": ["bitget"],
            },
        },
        {
            "id": "gateio_trades",
            "name": "Gate.io Trades",
            "status": "supported",
            "parser": "gateio_trades",
            "match": {
                "headers_all": [
                    "time",
                    "currency pair",
                    "side",
                    "amount",
                    "total",
                ],
                "headers_any": [
                    "price",
                    "fee",
                    "fee currency",
                    "order id",
                ],
                "filename_keywords": ["gate", "gateio", "gate.io"],
            },
        },
    ],
}

_DEFAULT_UNSUPPORTED: Dict[str, Any] = {
    "version": 1,
    "signatures": {}
}


@dataclass(frozen=True)
class CSVSourceMeta:
    """
    Normalized metadata about a CSV file source detection.

    status:
      - "supported": matched a known/implemented format
      - "unsupported": unknown format; structure recorded for later implementation
    """
    source_id: Optional[str]
    source_name: Optional[str]
    status: str
    confidence: float
    signature: str
    delimiter: Optional[str] = None
    quotechar: Optional[str] = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(tmp, path)


def _load_or_init_json(path: Path, default_payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            _atomic_write_json(path, default_payload)
            return dict(default_payload)
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("registry root is not an object")
        return data
    except Exception:
        # Self-heal: rewrite a clean default to keep the app operational.
        _atomic_write_json(path, default_payload)
        return dict(default_payload)


def ensure_csv_source_registry_files() -> Tuple[Path, Path]:
    _REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    supported = _load_or_init_json(_SUPPORTED_PATH, _DEFAULT_SUPPORTED)
    _load_or_init_json(_UNSUPPORTED_PATH, _DEFAULT_UNSUPPORTED)

    # Merge built-in supported sources into the on-disk registry (by stable "id").
    # This ensures new formats (e.g., Binance) become available without requiring users
    # to delete their existing supported_sources.json.
    try:
        sources = supported.get("sources")
        if not isinstance(sources, list):
            sources = []
            supported["sources"] = sources

        existing_by_id = {
            str(s.get("id")): s
            for s in sources
            if isinstance(s, dict) and s.get("id")
        }
        changed = False

        for built in (_DEFAULT_SUPPORTED.get("sources") or []):
            if not isinstance(built, dict):
                continue

            bid = str(built.get("id") or "").strip()
            if not bid:
                continue

            existing = existing_by_id.get(bid)

            if existing is None:
                sources.append(built)
                existing_by_id[bid] = built
                changed = True
                continue

            # Built-in supported source definitions are part of the application
            # contract. Keep existing user installations current when parser
            # header requirements are relaxed or otherwise corrected.
            for key in ("name", "status", "parser", "match"):
                if existing.get(key) != built.get(key):
                    existing[key] = built.get(key)
                    changed = True

        if supported.get("version") != _DEFAULT_SUPPORTED.get("version"):
            supported["version"] = _DEFAULT_SUPPORTED.get("version", 1)
            changed = True

        if changed:
            _atomic_write_json(_SUPPORTED_PATH, supported)

    except Exception:
        # Self-heal if the file was corrupted in an unexpected way.
        _atomic_write_json(_SUPPORTED_PATH, _DEFAULT_SUPPORTED)

    return _SUPPORTED_PATH, _UNSUPPORTED_PATH


def _normalize_headers(headers: List[str]) -> List[str]:
    out: List[str] = []
    for h in headers:
        t = str(h or "").strip().lower()
        if t:
            out.append(t)
    return out


def headers_signature(headers_norm: List[str]) -> str:
    joined = "\n".join(headers_norm)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def record_unsupported_structure(
    *,
    signature: str,
    headers_norm: List[str],
    filename: Optional[str],
    delimiter: Optional[str],
    quotechar: Optional[str],
    reason: str,
) -> None:
    ensure_csv_source_registry_files()
    payload = _load_or_init_json(_UNSUPPORTED_PATH, _DEFAULT_UNSUPPORTED)

    sigs = payload.setdefault("signatures", {})
    if not isinstance(sigs, dict):
        sigs = {}
        payload["signatures"] = sigs

    now = _now_iso()
    entry = sigs.get(signature)
    if isinstance(entry, dict):
        entry["last_seen"] = now
        entry["count"] = int(entry.get("count", 0)) + 1
        if filename:
            filenames = entry.setdefault("filenames", [])
            if isinstance(filenames, list) and filename not in filenames:
                filenames.append(filename)
                if len(filenames) > 5:
                    del filenames[:-5]
    else:
        sigs[signature] = {
            "first_seen": now,
            "last_seen": now,
            "count": 1,
            "filenames": [filename] if filename else [],
            "headers": headers_norm,
            "delimiter": delimiter,
            "quotechar": quotechar,
            "reason": reason,
        }

    _atomic_write_json(_UNSUPPORTED_PATH, payload)
    

def remove_unsupported_signature(signature: str) -> bool:
    """
    Remove a signature from unsupported_structures.json once the format becomes supported.
    Returns True if an entry was removed.
    """
    ensure_csv_source_registry_files()
    payload = _load_or_init_json(_UNSUPPORTED_PATH, _DEFAULT_UNSUPPORTED)

    sigs = payload.get("signatures")
    if not isinstance(sigs, dict):
        return False

    if signature not in sigs:
        return False

    del sigs[signature]
    _atomic_write_json(_UNSUPPORTED_PATH, payload)
    return True


def _score_candidate(source: Dict[str, Any], headers_set: set[str], filename_lower: str) -> Tuple[int, int]:
    match = source.get("match", {}) if isinstance(source.get("match", {}), dict) else {}

    headers_all = {str(h).strip().lower() for h in (match.get("headers_all") or [])}
    headers_any = {str(h).strip().lower() for h in (match.get("headers_any") or [])}
    keywords = [str(k).strip().lower() for k in (match.get("filename_keywords") or []) if str(k).strip()]

    if headers_all and not headers_all.issubset(headers_set):
        return -1, 1

    score = 10 * len(headers_all)
    score += 2 * len(headers_any.intersection(headers_set))
    if filename_lower and keywords and any(k in filename_lower for k in keywords):
        score += 5

    max_score = 10 * len(headers_all) + 2 * len(headers_any) + (5 if keywords else 0)
    return score, max_score if max_score > 0 else 1


def detect_csv_source(
    *,
    headers: List[str],
    filename: Optional[str] = None,
    delimiter: Optional[str] = None,
    quotechar: Optional[str] = None,
) -> CSVSourceMeta:
    """
    Detect the source format of a CSV based on header structure + (optional) filename hints.

    If no supported source matches, the header structure is recorded in:
      storage_raw/csv_sources/unsupported_structures.json
    """
    ensure_csv_source_registry_files()

    headers_norm = _normalize_headers(headers)
    sig = headers_signature(headers_norm)
    headers_set = set(headers_norm)
    filename_lower = (filename or "").strip().lower()

    supported = _load_or_init_json(_SUPPORTED_PATH, _DEFAULT_SUPPORTED)
    sources = supported.get("sources", [])
    if not isinstance(sources, list):
        sources = []

    best: Optional[Dict[str, Any]] = None
    best_score = -1
    best_max = 1

    for src in sources:
        if not isinstance(src, dict):
            continue
        score, max_score = _score_candidate(src, headers_set, filename_lower)
        if score > best_score:
            best = src
            best_score = score
            best_max = max_score

    if best is None or best_score < 0:
        record_unsupported_structure(
            signature=sig,
            headers_norm=headers_norm,
            filename=filename,
            delimiter=delimiter,
            quotechar=quotechar,
            reason="no match in supported registry",
        )
        return CSVSourceMeta(
            source_id=None,
            source_name=None,
            status="unsupported",
            confidence=0.0,
            signature=sig,
            delimiter=delimiter,
            quotechar=quotechar,
        )
    
    # If this structure was previously logged as unsupported, clean it up now.
    remove_unsupported_signature(sig)

    confidence = round(min(1.0, float(best_score) / float(best_max)), 4)
    return CSVSourceMeta(
        source_id=str(best.get("id") or ""),
        source_name=str(best.get("name") or ""),
        status="supported",
        confidence=confidence,
        signature=sig,
        delimiter=delimiter,
        quotechar=quotechar,
    )
    
def list_supported_sources() -> List[Dict[str, str]]:
    """
    Return a clean, UI-safe list of supported CSV sources.

    Output shape:
      [{"id": "binance_spot_trades", "name": "Binance Spot Trades"}, ...]
    """
    ensure_csv_source_registry_files()
    supported = _load_or_init_json(_SUPPORTED_PATH, _DEFAULT_SUPPORTED)

    sources = supported.get("sources", [])
    if not isinstance(sources, list):
        sources = []

    out: List[Dict[str, str]] = []
    for s in sources:
        if not isinstance(s, dict):
            continue
        if str(s.get("status") or "supported") != "supported":
            continue

        sid = str(s.get("id") or "").strip()
        name = str(s.get("name") or sid).strip()
        if not sid:
            continue

        out.append({"id": sid, "name": name})

    out.sort(key=lambda d: d.get("name", "").lower())
    return out

def list_supported_sources_catalog() -> List[Dict[str, Any]]:
    """
    Return a UI-safe catalog of supported CSV formats, including match criteria.

    Output shape:
      [
        {
          "id": "binance_spot_trades",
          "name": "Binance Spot Trades",
          "parser": "binance_spot_trades",
          "match": {
            "headers_all": [...],
            "headers_any": [...],
            "filename_keywords": [...]
          }
        },
        ...
      ]
    """
    ensure_csv_source_registry_files()
    supported = _load_or_init_json(_SUPPORTED_PATH, _DEFAULT_SUPPORTED)

    sources = supported.get("sources", [])
    if not isinstance(sources, list):
        sources = []

    out: List[Dict[str, Any]] = []
    for s in sources:
        if not isinstance(s, dict):
            continue
        if str(s.get("status") or "supported") != "supported":
            continue

        sid = str(s.get("id") or "").strip()
        if not sid:
            continue

        name = str(s.get("name") or sid).strip()
        parser = str(s.get("parser") or "").strip() or None

        m = s.get("match") if isinstance(s.get("match"), dict) else {}
        headers_all = [str(x) for x in (m.get("headers_all") or []) if str(x).strip()]
        headers_any = [str(x) for x in (m.get("headers_any") or []) if str(x).strip()]
        filename_keywords = [str(x) for x in (m.get("filename_keywords") or []) if str(x).strip()]

        out.append(
            {
                "id": sid,
                "name": name,
                "parser": parser,
                "match": {
                    "headers_all": headers_all,
                    "headers_any": headers_any,
                    "filename_keywords": filename_keywords,
                },
            }
        )

    out.sort(key=lambda d: (d.get("name") or "").lower())
    return out

def list_unsupported_signatures(limit: int = 200) -> List[Dict[str, Any]]:
    """
    Return unsupported CSV structures for implementation triage.
    Sorted by highest count, then most recent last_seen.
    """
    ensure_csv_source_registry_files()
    payload = _load_or_init_json(_UNSUPPORTED_PATH, _DEFAULT_UNSUPPORTED)

    sigs = payload.get("signatures")
    if not isinstance(sigs, dict):
        return []

    items: List[Dict[str, Any]] = []
    for sig, entry in sigs.items():
        if not isinstance(entry, dict):
            continue

        try:
            cnt = int(entry.get("count", 0) or 0)
        except Exception:
            cnt = 0

        items.append(
            {
                "signature": str(sig),
                "count": cnt,
                "first_seen": entry.get("first_seen"),
                "last_seen": entry.get("last_seen"),
                "filenames": entry.get("filenames") if isinstance(entry.get("filenames"), list) else [],
                "headers": entry.get("headers") if isinstance(entry.get("headers"), list) else [],
                "delimiter": entry.get("delimiter"),
                "quotechar": entry.get("quotechar"),
                "reason": entry.get("reason"),
            }
        )

    items.sort(key=lambda d: (-(d.get("count") or 0), str(d.get("last_seen") or "")), reverse=False)
    return items[: max(1, min(int(limit), 2000))]

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any, Dict, Iterable, Optional

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from .db import SessionLocal
from .fx_utils import ensure_fx_rates_schema, ensure_rate_or_default_lookup
from .security import IS_PROD


logger = logging.getLogger("cryptotaxcalc.price.autosync")

USD_LIKE_QUOTES = {"USD", "USDT", "USDC", "BUSD"}


def _env_truthy(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or not str(v).strip():
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not str(v).strip():
        return default
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def price_autosync_enabled() -> bool:
    # Default OFF (external HTTP). Enable explicitly.
    enabled = _env_truthy("PRICE_AUTOSYNC_ENABLED", default=True)

    # Safety: allow in production by default, but support explicit opt-out.
    if IS_PROD and _env_truthy("PRICE_AUTOSYNC_DISABLE_IN_PROD", default=False):
        return False

    return enabled


def price_autosync_interval_seconds() -> int:
    minutes = _env_int("PRICE_AUTOSYNC_INTERVAL_MINUTES", 360)
    if minutes < 1:
        minutes = 1
    return minutes * 60


def _price_autosync_quote() -> str:
    return _env_str("PRICE_AUTOSYNC_QUOTE", "USDT").upper()


def _price_autosync_provider() -> str:
    # Default: auto (Binance first, Bitget fallback — needed for BGB)
    return _env_str("PRICE_AUTOSYNC_PROVIDER", "auto").lower()


def _price_autosync_http_timeout() -> int:
    return _env_int("PRICE_AUTOSYNC_HTTP_TIMEOUT_SECONDS", 25)


def _price_autosync_backfill_days() -> int:
    # How far back to fetch candles (covers historical fee dates)
    d = _env_int("PRICE_AUTOSYNC_BACKFILL_DAYS", 800)
    if d < 30:
        d = 30
    if d > 5000:
        d = 5000
    return d


def _price_autosync_max_assets() -> int:
    n = _env_int("PRICE_AUTOSYNC_MAX_ASSETS", 12)
    if n < 1:
        n = 1
    if n > 50:
        n = 50
    return n


def _price_autosync_assets_override() -> list[str]:
    raw = _env_str("PRICE_AUTOSYNC_ASSETS", "").strip()
    if not raw:
        return []
    parts = [p.strip().upper() for p in raw.split(",")]
    return [p for p in parts if p]


def _bitget_base_url() -> str:
    return _env_str("PRICE_AUTOSYNC_BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")


def _bitget_granularity() -> str:
    # Bitget supports day granularity: 1day / 1Dutc. We prefer UTC day boundaries.
    return _env_str("PRICE_AUTOSYNC_BITGET_GRANULARITY", "1Dutc")


def _bitget_fetch_daily_candles(
    symbol: str,
    *,
    start: date,
    end: date,
    timeout_s: int,
) -> dict[str, Decimal]:
    """
    Bitget spot candles:
      GET /api/v2/spot/market/candles?symbol=BTCUSDT&granularity=1Dutc&startTime=...&endTime=...&limit=1000

    Response data rows:
      [openTimeMs, open, high, low, close, ...]
    We return {YYYY-MM-DD: close_in_quote}.
    """
    base_url = _bitget_base_url()
    url = f"{base_url}/api/v2/spot/market/candles"

    # Segment if range exceeds 1000 days (Bitget limit can be 1000 rows)
    out: dict[str, Decimal] = {}
    max_days = 999

    cur = start
    while cur <= end:
        seg_end = min(end, cur + timedelta(days=max_days))

        start_ms = _date_to_ms(cur)
        end_ms = _date_to_ms(seg_end + timedelta(days=1)) - 1

        params = {
            "symbol": symbol,
            "granularity": _bitget_granularity(),
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": "1000",
        }

        r = requests.get(
            url,
            params=params,
            timeout=timeout_s,
            headers={"User-Agent": "CryptoTaxCalc/1.0"},
        )

        if r.status_code != 200:
            raise RuntimeError(f"Bitget HTTP {r.status_code}: {r.text[:200]}")

        j = r.json() or {}
        code = str(j.get("code") or "")
        if code != "00000":
            raise RuntimeError(f"Bitget API code {code}: {str(j.get('msg') or '')[:200]}")

        data = j.get("data") or []
        for row in data:
            try:
                # row[0]=ms, row[4]=close
                ms = int(row[0])
                close = Decimal(str(row[4]))
                d_iso = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
                out[d_iso] = close
            except Exception:
                continue

        cur = seg_end + timedelta(days=1)

    return out


def _is_binance_invalid_symbol_error(e: Exception) -> bool:
    msg = str(e)
    # Binance invalid symbol is often -1121
    return ("Invalid symbol" in msg) or ("-1121" in msg)


def _binance_symbol(base: str, quote: str) -> str:
    # Binance symbols are concatenated like BGBUSDT
    return f"{base.upper()}{quote.upper()}"


def _date_to_ms(d: date) -> int:
    # Binance expects milliseconds since epoch (UTC)
    dt0 = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt0.timestamp() * 1000)


def _ms_to_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


# -----------------------------
# CoinGecko fallback (EUR prices)
# -----------------------------
_DEFAULT_COINGECKO_IDS = {
    "BGB": "bitget-token",
    "BNB": "binancecoin",
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
}

def _coingecko_base_url() -> str:
    return _env_str("PRICE_AUTOSYNC_COINGECKO_BASE_URL", "https://api.coingecko.com").rstrip("/")

def _coingecko_vs_currency() -> str:
    # Prefer EUR so we don't need USD->EUR FX conversion for fee valuation.
    return _env_str("PRICE_AUTOSYNC_COINGECKO_VS_CURRENCY", "eur").lower()

def _coingecko_ids_override() -> dict[str, str]:
    """
    Parse env: PRICE_AUTOSYNC_COINGECKO_IDS=BGB=bitget-token,BNB=binancecoin
    """
    raw = _env_str("PRICE_AUTOSYNC_COINGECKO_IDS", "").strip()
    if not raw:
        return {}
    out: dict[str, str] = {}
    for part in raw.split(","):
        s = part.strip()
        if not s or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip().upper()
        v = v.strip()
        if k and v:
            out[k] = v
    return out

def _coingecko_coin_id(asset: str) -> str | None:
    a = (asset or "").upper().strip()
    if not a:
        return None
    overrides = _coingecko_ids_override()
    if a in overrides:
        return overrides[a]
    return _DEFAULT_COINGECKO_IDS.get(a)

def _coingecko_fetch_daily_prices(
    coin_id: str,
    *,
    vs_currency: str,
    start: date,
    end: date,
    timeout_s: int,
) -> dict[str, Decimal]:
    """
    Return {YYYY-MM-DD: price_in_vs_currency} using market_chart/range.
    We take the last observed price per UTC day (deterministic given response).
    """
    base = _coingecko_base_url()
    url = f"{base}/api/v3/coins/{coin_id}/market_chart/range"

    # CoinGecko expects seconds
    from_s = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp())
    to_s = int(datetime((end + timedelta(days=1)).year, (end + timedelta(days=1)).month, (end + timedelta(days=1)).day, tzinfo=timezone.utc).timestamp()) - 1

    r = requests.get(
        url,
        params={"vs_currency": vs_currency, "from": from_s, "to": to_s},
        timeout=timeout_s,
        headers={"User-Agent": "CryptoTaxCalc/1.0"},
    )

    if r.status_code != 200:
        raise RuntimeError(f"CoinGecko HTTP {r.status_code}: {r.text[:200]}")

    data = r.json() or {}
    prices = data.get("prices") or []
    out: dict[str, Decimal] = {}

    for pt in prices:
        try:
            ts_ms = int(pt[0])
            px = Decimal(str(pt[1]))
            d_iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()
            out[d_iso] = px  # last write wins -> last price per day
        except Exception:
            continue

    return out

def _is_binance_invalid_symbol_error(e: Exception) -> bool:
    msg = str(e)
    return ("Invalid symbol" in msg) or ("-1121" in msg) or ("HTTP 400" in msg and "symbol" in msg.lower())


def _bitget_base_url() -> str:
    return _env_str("PRICE_AUTOSYNC_BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")


def _bitget_granularity() -> str:
    # Prefer UTC day boundaries
    return _env_str("PRICE_AUTOSYNC_BITGET_GRANULARITY", "1Dutc")


def _bitget_fetch_daily_candles(
    symbol: str,
    *,
    start: date,
    end: date,
    timeout_s: int,
) -> dict[str, Decimal]:
    """
    Bitget spot candles:
      GET /api/v2/spot/market/candles?symbol=BTCUSDT&granularity=1Dutc&startTime=...&endTime=...&limit=1000
    Returns {YYYY-MM-DD: close_in_quote}.
    """
    base_url = _bitget_base_url()
    url = f"{base_url}/api/v2/spot/market/candles"

    # Segment range to respect typical limits (1000 rows)
    out: dict[str, Decimal] = {}
    max_days = 999

    cur = start
    while cur <= end:
        seg_end = min(end, cur + timedelta(days=max_days))

        start_ms = _date_to_ms(cur)
        end_ms = _date_to_ms(seg_end + timedelta(days=1)) - 1

        params = {
            "symbol": symbol,
            "granularity": _bitget_granularity(),
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": "1000",
        }

        r = requests.get(
            url,
            params=params,
            timeout=timeout_s,
            headers={"User-Agent": "CryptoTaxCalc/1.0"},
        )

        if r.status_code != 200:
            raise RuntimeError(f"Bitget HTTP {r.status_code}: {r.text[:200]}")

        j = r.json() or {}
        code = str(j.get("code") or "")
        if code != "00000":
            raise RuntimeError(f"Bitget API code {code}: {str(j.get('msg') or '')[:200]}")

        data = j.get("data") or []
        for row in data:
            try:
                ms = int(row[0])
                close = Decimal(str(row[4]))
                d_iso = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
                out[d_iso] = close
            except Exception:
                continue

        cur = seg_end + timedelta(days=1)

    return out


def _binance_fetch_daily_klines(
    symbol: str,
    *,
    start: date,
    end: date,
    timeout_s: int,
) -> dict[str, Decimal]:
    """
    Return {YYYY-MM-DD: close_price_in_quote} for 1d candles.
    """
    url = _env_str("PRICE_AUTOSYNC_BINANCE_BASE_URL", "https://api.binance.com")
    endpoint = f"{url}/api/v3/klines"

    start_ms = _date_to_ms(start)
    # include end day
    end_ms = _date_to_ms(end + timedelta(days=1)) - 1

    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }

    r = requests.get(
        endpoint,
        params=params,
        timeout=timeout_s,
        headers={"User-Agent": "CryptoTaxCalc/1.0"},
    )

    if r.status_code != 200:
        raise RuntimeError(f"Binance HTTP {r.status_code}: {r.text[:200]}")

    data = r.json()
    out: dict[str, Decimal] = {}

    for k in data:
        try:
            open_ms = int(k[0])
            close_s = str(k[4])
            d = _ms_to_date(open_ms).isoformat()
            out[d] = Decimal(close_s)
        except Exception:
            continue

    return out


def _detect_third_asset_fee_coins(db: Session) -> dict[str, tuple[date, int]]:
    """
    Return {ASSET: earliest_fee_day} for fee coins that appear as a third asset
    (fee_asset not in {base_asset, quote_asset} and not EUR/USD-like).
    """
    rows = db.execute(
        text(
            """
            SELECT
              UPPER(fee_asset) AS fa,
              MIN(timestamp)  AS min_ts,
              SUM(
                CASE
                  WHEN fee_asset IS NOT NULL
                   AND fee_amount IS NOT NULL
                   AND fee_amount != 0
                   AND UPPER(fee_asset) NOT IN ('EUR','USD','USDT','USDC','BUSD')
                   AND UPPER(fee_asset) != UPPER(base_asset)
                   AND UPPER(fee_asset) != UPPER(quote_asset)
                  THEN 1 ELSE 0
                END
              ) AS third_cnt
            FROM transactions
            WHERE fee_asset IS NOT NULL AND fee_amount IS NOT NULL AND fee_amount != 0
            GROUP BY UPPER(fee_asset)
            """
        )
    ).fetchall()

    out: dict[str, date] = {}

    for fa, min_ts, third_cnt in rows:
        try:
            if not fa or int(third_cnt or 0) <= 0:
                continue
            # timestamps are stored as ISO text in this project; take YYYY-MM-DD prefix
            s = str(min_ts) if min_ts is not None else ""
            if len(s) >= 10:
                d = datetime.strptime(s[:10], "%Y-%m-%d").date()
            else:
                d = datetime.now(timezone.utc).date() - timedelta(days=_price_autosync_backfill_days())
            out[str(fa)] = (d, int(third_cnt or 0))
        except Exception:
            continue

    return out


def _existing_price_days(db: Session, asset: str, start: date, end: date) -> set[str]:
    rows = db.execute(
        text(
            """
            SELECT date FROM fx_rates
            WHERE base = :b AND quote = 'EUR'
              AND date >= :s AND date <= :e
            """
        ),
        {"b": asset.upper(), "s": start.isoformat(), "e": end.isoformat()},
    ).fetchall()
    return {str(r[0]) for r in rows}


def _insert_prices_eur(
    db: Session,
    *,
    asset: str,
    eur_prices: dict[str, Decimal],
    bid: int | None,
) -> tuple[int, int, int]:
    inserted = 0
    skipped = 0
    errors = 0

    for d_iso, eur_per_asset in eur_prices.items():
        try:
            if eur_per_asset <= 0:
                errors += 1
                continue

            exists = db.execute(
                text("SELECT 1 FROM fx_rates WHERE date = :d AND base = :b AND quote = 'EUR'"),
                {"d": d_iso, "b": asset},
            ).scalar()

            if exists:
                skipped += 1
                continue

            db.execute(
                text(
                    """
                    INSERT INTO fx_rates (date, base, quote, rate, batch_id)
                    VALUES (:d, :b, 'EUR', :r, :bid)
                    """
                ),
                {"d": d_iso, "b": asset, "r": str(eur_per_asset), "bid": bid},
            )
            inserted += 1
        except Exception:
            errors += 1

    return inserted, skipped, errors


def price_autosync_tick(reason: str = "tick", *, db: Session | None = None) -> dict[str, Any]:
    """
    One price autosync iteration:
      - detect fee coins (third-asset fees) unless PRICE_AUTOSYNC_ASSETS is set
      - fetch daily candles from provider (default Binance) for ASSET/QUOTE
      - convert QUOTE->EUR via fx_rates USD->EUR
      - store ASSET/EUR into fx_rates for missing days
    """
    if not price_autosync_enabled():
        return {"ok": True, "enabled": False, "reason": reason}

    provider = _price_autosync_provider()
    quote = _price_autosync_quote()
    cg_vs = _coingecko_vs_currency()
    timeout_s = _price_autosync_http_timeout()

    if provider not in {"auto", "binance", "bitget", "coingecko"}:
        return {"ok": False, "enabled": True, "reason": reason, "error": f"Unsupported provider: {provider}"}

    today = datetime.now(timezone.utc).date()
    start_floor = today - timedelta(days=_price_autosync_backfill_days())

    inserted_total = 0
    skipped_total = 0
    errors_total = 0

    assets_override = _price_autosync_assets_override()

    session = db if db is not None else SessionLocal()
    created_here = db is None
    try:
        try:
            ensure_fx_rates_schema(session)
        except Exception:
            pass

        # Determine assets and their earliest-needed day
        # Normalize mapping to: asset -> (min_day, count)
        if assets_override:
            assets = {a: (start_floor, 0) for a in assets_override}
        else:
            assets = _detect_third_asset_fee_coins(session)

        if not assets:
            return {"ok": True, "enabled": True, "reason": reason, "assets": 0, "inserted": 0}

        # Choose assets to sync:
        # 1) Always prioritize BNB if present (common Binance fee coin)
        # 2) Then sort remaining by count desc, then asset asc
        items_all = list(assets.items())

        def _cnt(v) -> int:
            try:
                return int(v[1] or 0)
            except Exception:
                return 0

        items_all.sort(key=lambda kv: (-_cnt(kv[1]), kv[0]))

        # Move BNB to front if it exists
        for i, (k, v) in enumerate(items_all):
            if str(k).upper() == "BNB":
                items_all.insert(0, items_all.pop(i))
                break

        items = items_all[: _price_autosync_max_assets()]

        # Create a batch record
        now_iso_z = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        source = f"PRICE AUTOSYNC {provider.upper()} (binance_quote={quote}, coingecko_vs={cg_vs})"
        bid = session.execute(
            text("INSERT INTO fx_batches (imported_at, source, rates_hash) VALUES (:t,:s,:h)"),
            {"t": now_iso_z, "s": source, "h": None},
        ).lastrowid

        batch_hash = hashlib.sha256()

        details: dict[str, Any] = {"assets": []}

        for asset, (min_day, _cnt) in items:
            a = str(asset).upper().strip()
            fetch_error: str | None = None
            if not a or a in USD_LIKE_QUOTES or a == "EUR":
                continue

            start = max(min_day, start_floor)
            end = today

            # Skip if we already have full coverage for requested range (cheap check on latest 30 days)
            existing = _existing_price_days(session, a, start, end)

            provider_used = None
            symbol = _binance_symbol(a, quote)
            q_prices: dict[str, Decimal] = {}
            cg_prices: dict[str, Decimal] = {}

            # 1) Binance (preferred) unless provider is coingecko-only
            if provider in {"auto", "binance"}:
                try:
                    q_prices = _binance_fetch_daily_klines(symbol, start=start, end=end, timeout_s=timeout_s)
                    provider_used = "binance"
                except Exception as e:
                    if provider == "auto" and _is_binance_invalid_symbol_error(e):
                        provider_used = None  # fallback to CoinGecko
                    else:
                        raise

            # 2) Bitget fallback (covers non-Binance assets like BGB)
            if provider_used is None and provider in {"auto", "bitget"}:
                q_prices = _bitget_fetch_daily_candles(symbol, start=start, end=end, timeout_s=timeout_s)
                provider_used = "bitget"

            # Convert to EUR-per-asset
            eur_prices: dict[str, Decimal] = {}
            fx_fallback_days: set[str] = set()

            if provider_used in {"binance", "bitget"}:
                # Convert quote->EUR day-by-day (USDT treated as USD)
                for d_iso, q_per_asset in q_prices.items():
                    if d_iso in existing:
                        continue
                    day = datetime.strptime(d_iso, "%Y-%m-%d").date()

                    fx = ensure_rate_or_default_lookup(
                        session,
                        day,
                        base="USD",
                        quote="EUR",
                        default_rate=Decimal("1.0"),
                        max_lookback_days=7,
                    )
                    eur_per_usd = fx.rate if isinstance(fx.rate, Decimal) else Decimal(str(fx.rate))
                    if fx.used_fallback:
                        fx_fallback_days.add(d_iso)

                    eur_per_asset = (Decimal(str(q_per_asset)) * eur_per_usd).quantize(Decimal("0.00000001"))
                    eur_prices[d_iso] = eur_per_asset

            elif provider_used == "coingecko":
                # CoinGecko can provide EUR directly (preferred)
                for d_iso, px in cg_prices.items():
                    if d_iso in existing:
                        continue

                    if cg_vs == "eur":
                        eur_prices[d_iso] = Decimal(str(px)).quantize(Decimal("0.00000001"))
                    elif cg_vs in {"usd"}:
                        day = datetime.strptime(d_iso, "%Y-%m-%d").date()
                        fx = ensure_rate_or_default_lookup(
                            session,
                            day,
                            base="USD",
                            quote="EUR",
                            default_rate=Decimal("1.0"),
                            max_lookback_days=7,
                        )
                        eur_per_usd = fx.rate if isinstance(fx.rate, Decimal) else Decimal(str(fx.rate))
                        if fx.used_fallback:
                            fx_fallback_days.add(d_iso)

                        eur_prices[d_iso] = (Decimal(str(px)) * eur_per_usd).quantize(Decimal("0.00000001"))
                    else:
                        raise RuntimeError(f"Unsupported CoinGecko vs_currency: {cg_vs}")

            else:
                raise RuntimeError("No provider produced prices")

            ins, skp, err = _insert_prices_eur(session, asset=a, eur_prices=eur_prices, bid=bid)
            inserted_total += ins
            skipped_total += skp
            errors_total += err

            # Hash inserted rows for audit
            for d_iso, eur_per_asset in sorted(eur_prices.items()):
                if d_iso in existing:
                    continue
                line = f"{d_iso}|{a}|EUR|{eur_per_asset}\n"
                batch_hash.update(line.encode("utf-8"))

            details["assets"].append(
                {
                    "asset": a,
                    "symbol": symbol,
                    "provider_used": provider_used,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "fetched_days": len(q_prices) if provider_used in {"binance", "bitget"} else len(cg_prices),
                    "inserted": ins,
                    "skipped_existing": skp,
                    "errors": err,
                    "fx_fallback_days_count": len(fx_fallback_days),
                }
            )

        # Update batch hash
        try:
            session.execute(
                text("UPDATE fx_batches SET rates_hash = :rh WHERE id = :bid"),
                {"rh": batch_hash.hexdigest(), "bid": bid},
            )
        except Exception:
            pass

        session.commit()
    
    finally:
        if created_here:
            session.close()

    if inserted_total > 0:
        logger.info(
            "Price autosync (%s): inserted=%s skipped=%s errors=%s",
            reason,
            inserted_total,
            skipped_total,
            errors_total,
        )
    else:
        logger.info("Price autosync (%s): no new rows (errors=%s)", reason, errors_total)

    return {
        "ok": True,
        "enabled": True,
        "reason": reason,
        "provider": provider,
        "quote": quote,
        "inserted": inserted_total,
        "skipped": skipped_total,
        "errors": errors_total,
        "details": details,
    }


async def price_autosync_loop(stop_event: asyncio.Event | None) -> None:
    interval = price_autosync_interval_seconds()

    logger.info(
        "Price autosync loop started (interval=%ss provider=%s quote=%s)",
        interval,
        _price_autosync_provider(),
        _price_autosync_quote(),
    )

    # Run once immediately
    try:
        await asyncio.to_thread(price_autosync_tick, "startup")
    except Exception as e:
        logger.warning("Price autosync startup tick failed: %s", e)

    while True:
        try:
            if stop_event is not None and stop_event.is_set():
                break

            await asyncio.to_thread(price_autosync_tick, "periodic")

            if stop_event is None:
                await asyncio.sleep(interval)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("Price autosync loop error: %s", e)
            await asyncio.sleep(min(interval, 60))

    logger.info("Price autosync loop stopped")

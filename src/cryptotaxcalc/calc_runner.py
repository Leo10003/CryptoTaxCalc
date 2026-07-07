# calc_runner.py – EUR-canonical, auditable calculation runner
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, List

import os
import json
import time
import traceback

def _debug_warnings_enabled() -> bool:
    v = (os.getenv("DEBUG_WARNINGS") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

from sqlalchemy.orm import Session

from cryptotaxcalc.schemas import CalcConfig, RunSummary, RunTotals, Transaction
from cryptotaxcalc.rules.registry import get_rule, split_taxable_exempt_gain
from cryptotaxcalc.rules.base import TaxRule, RunContext
from cryptotaxcalc.models import TransactionRow, RealizedEvent, RunInput, WalletOutOverride
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.fx_utils import ensure_rate_or_default_lookup, get_or_create_current_fx_batch_id
from cryptotaxcalc.logging_setup import get_logger, _atomic_write_json, _now_iso_z
from cryptotaxcalc.runtime_paths import PROJECT_ROOT

logger = get_logger("calc")
WORKSPACE_LOG_DIR = PROJECT_ROOT / "logs" / "workspace"
WORKSPACE_ERRORS_TXT = WORKSPACE_LOG_DIR / "errors.txt"
WORKSPACE_ERROR_PATH_POINTER = PROJECT_ROOT / "logs" / "workspace_error_log_path.txt"


def log_workspace_error(
    *,
    stage: str,
    cfg: CalcConfig,
    run_id: Any,
    error: Exception,
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Log relevant workspace calculation errors to disk for support/debugging.

    Writes:
      - logs/workspace/last_error.json (latest)
      - logs/workspace/errors.jsonl (append-only history)
    """
    try:
        WORKSPACE_LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        # If we can't create the dir, we still want normal logger output.
        pass

    payload: dict[str, Any] = {
        "timestamp": _now_iso_z(),
        "stage": stage,
        "jurisdiction": getattr(cfg, "jurisdiction", None),
        "tax_year": getattr(cfg, "tax_year", None),
        "run_id": run_id,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "error_text_log_path": str(WORKSPACE_ERRORS_TXT.resolve()),
        "latest_error_json_path": str((WORKSPACE_LOG_DIR / "last_error.json").resolve()),
        "error_log_pointer_path": str(WORKSPACE_ERROR_PATH_POINTER.resolve()),
    }

    if extra:
        payload["extra"] = extra

    try:
        exc_info = (
            (type(error), error, error.__traceback__)
            if getattr(error, "__traceback__", None)
            else None
        )
        logger.error(
            "Workspace error stage=%s run_id=%s error=%s error_log=%s",
            stage,
            run_id,
            error,
            WORKSPACE_ERRORS_TXT.resolve(),
            exc_info=exc_info,
            extra={
                "ctc_stage": stage,
                "ctc_run_id": run_id,
                "ctc_error_log_path": str(WORKSPACE_ERRORS_TXT.resolve()),
            },
        )
    except Exception:
        pass

    # Capture stack trace only when we're inside an exception path.
    try:
        payload["traceback"] = traceback.format_exc()
    except Exception:
        pass
    
    # Write a pointer file in logs/ that contains the location of the text error log.
    # This makes support/debugging easier (one stable file to open).
    try:
        WORKSPACE_ERROR_PATH_POINTER.write_text(str(WORKSPACE_ERRORS_TXT.resolve()), encoding="utf-8")
    except Exception:
        pass

    # Append a human-readable line to the workspace TXT error log.
    try:
        line = (
            f"{payload.get('timestamp')} | stage={stage} | jurisdiction={payload.get('jurisdiction')} | "
            f"tax_year={payload.get('tax_year')} | run_id={run_id} | "
            f"{payload.get('error_type')}: {payload.get('error_message')}\n"
        )
        with open(WORKSPACE_ERRORS_TXT, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

    try:
        _atomic_write_json(WORKSPACE_LOG_DIR / "last_error.json", payload)
    except Exception:
        pass

    try:
        with open(WORKSPACE_LOG_DIR / "errors.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass

# ==========================
# Internal utilities
# ==========================

def _rule_for(cfg: CalcConfig) -> TaxRule:
    # Single source of truth for jurisdiction routing (pluggable registry).
    return get_rule(cfg.jurisdiction)


def _D(x) -> Decimal:
    """Safe Decimal coercion."""
    if isinstance(x, Decimal):
        return x
    if x is None:
        return Decimal("0")
    return Decimal(str(x))


# ==========================
# Phase 1: EUR-canonical quote legs (pre-FIFO FX normalization)
# ==========================

USD_LIKE_QUOTES = {"USD", "USDT", "USDC", "BUSD", "FDUSD", "TUSD"}

# Runtime posture: in production, prevent silent FX fallbacks unless explicitly allowed.
# Psychology: users trust tax outputs only when FX assumptions are explicit and controllable.
CTC_ENV = (os.getenv("CTC_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()
IS_PROD = CTC_ENV in {"prod", "production"}
ALLOW_FX_FALLBACK_IN_PROD = (os.getenv("ALLOW_FX_FALLBACK_IN_PROD") or "").strip().lower() in {"1", "true", "yes", "on"}
STRICT_FEE_VALUATION = (os.getenv("STRICT_FEE_VALUATION") or "").strip().lower() in {"1", "true", "yes", "on"}

# Fee valuation helpers:
# - DB daily prices: fx_rates base=<ASSET>, quote=EUR
# - Internal fallback: derive <ASSET>/EUR from user trades (e.g., BNBUSDT) + FX conversion
FEE_INTERNAL_PRICE_FROM_TRADES = (os.getenv("FEE_INTERNAL_PRICE_FROM_TRADES") or "").strip().lower() in {"1", "true", "yes", "on"}
try:
    FEE_INTERNAL_PRICE_LOOKBACK_DAYS = int(os.getenv("FEE_INTERNAL_PRICE_LOOKBACK_DAYS") or "7")
except Exception:
    FEE_INTERNAL_PRICE_LOOKBACK_DAYS = 7
if FEE_INTERNAL_PRICE_LOOKBACK_DAYS < 0:
    FEE_INTERNAL_PRICE_LOOKBACK_DAYS = 0
if FEE_INTERNAL_PRICE_LOOKBACK_DAYS > 30:
    FEE_INTERNAL_PRICE_LOOKBACK_DAYS = 30

try:
    FEE_DB_PRICE_LOOKBACK_DAYS = int(os.getenv("FEE_DB_PRICE_LOOKBACK_DAYS") or "0")
except Exception:
    FEE_DB_PRICE_LOOKBACK_DAYS = 0
if FEE_DB_PRICE_LOOKBACK_DAYS < 0:
    FEE_DB_PRICE_LOOKBACK_DAYS = 0
if FEE_DB_PRICE_LOOKBACK_DAYS > 30:
    FEE_DB_PRICE_LOOKBACK_DAYS = 30


def _warn_once(warnings: list[str], seen: set[str], msg: str) -> None:
    if msg in seen:
        return
    warnings.append(msg)
    seen.add(msg)


def _quote_amount_to_eur(
    amount: Decimal,
    *,
    quote_asset: str,
    day,
    db: Session,
    strict_fx: bool,
    warnings: list[str],
    seen: set[str],
    fx_meta: dict[str, Any] | None = None,
) -> Decimal:
    q = (quote_asset or "").upper().strip()
    if q in ("", "EUR"):
        return amount

    if q in USD_LIKE_QUOTES:
        # Transparency: treat some USD-pegged stablecoins as USD for FX conversion.
        # (We keep USDT/USDC silent to avoid noise; we warn for the others.)
        if q not in {"USD", "USDT", "USDC"}:
            _warn_once(warnings, seen, f"Stablecoin assumption: {q} treated as USD for FX conversion (audit note).")
        
        # EUR per 1 USD
        lookup = ensure_rate_or_default_lookup(db, day, base="USD", quote="EUR", default_rate=Decimal("1.0"))
        rate = lookup.rate

        if strict_fx and lookup.used_fallback:
            raise ValueError(
                f"Strict FX: missing USD->EUR rate for {day.isoformat()} "
                f"(lookback {int(getattr(lookup, 'looked_back_days', 0))} days). "
                "Import FX rates (HNB/ECB CSV) and re-run."
            )

        if lookup.used_fallback:
            _warn_once(
                warnings,
                seen,
                (
                    f"FX integrity warning: missing USD->EUR rate for {day.isoformat()}; "
                    "conversion assumed 1.0. Results may be materially inaccurate. "
                    "Import FX rates (HNB/ECB CSV) and re-run, or enable strict_fx."
                ),
            )
            if fx_meta is not None:
                fx_meta.setdefault("fallback_days", set()).add(day.isoformat())
                fx_meta.setdefault("fallback_pairs", set()).add("USD/EUR")

        return (amount * rate).quantize(Decimal("0.00000001"))

    raise ValueError(
        f"Unsupported quote asset '{q}' for EUR conversion. "
        "Provide fair_value (EUR) for this transaction or normalize the data."
    )


def _build_internal_eur_price_map_from_trades(
    txs: List[Transaction],
    *,
    db: Session,
    strict_fx: bool,
    warnings: list[str],
    seen: set[str],
    fx_meta: dict[str, Any],
    fee_assets_needed: set[str],
) -> dict[str, dict[str, Decimal]]:
    """
    Build a daily EUR-per-asset price map from the user's own trades.

    Uses any transaction where:
      - base_asset is in fee_assets_needed
      - quote_asset is USD-like (USD/USDT/USDC/BUSD/...)
      - quote_amount/base_amount yields quote per 1 base
      - quote is converted to EUR via _quote_amount_to_eur
    """
    prices: dict[str, dict[str, Decimal]] = {}

    for t in txs:
        try:
            base = (t.base_asset or "").upper().strip()
            if base not in fee_assets_needed:
                continue

            if not t.quote_asset or t.quote_amount is None:
                continue

            quote = (t.quote_asset or "").upper().strip()
            if quote not in USD_LIKE_QUOTES:
                continue

            base_amt = _D(getattr(t, "base_amount", None))
            if base_amt == 0:
                continue
            if base_amt < 0:
                base_amt = abs(base_amt)

            day = t.timestamp.date()
            day_iso = day.isoformat()

            qa_eur = _quote_amount_to_eur(
                _D(t.quote_amount),
                quote_asset=quote,
                day=day,
                db=db,
                strict_fx=strict_fx,
                warnings=warnings,
                seen=seen,
                fx_meta=fx_meta,
            )

            # EUR per 1 base asset
            rate_eur = (qa_eur / base_amt).quantize(Decimal("0.00000001"))
            prices.setdefault(base, {})[day_iso] = rate_eur
        except Exception:
            continue

    return prices


def _internal_price_lookup(
    prices: dict[str, dict[str, Decimal]],
    *,
    asset: str,
    day,
    lookback_days: int,
) -> tuple[Decimal | None, str | None, int]:
    """
    Find EUR-per-asset for `asset` at `day` with lookback.
    Returns (rate, matched_day_iso, looked_back_days).
    """
    a = (asset or "").upper().strip()
    per_day = prices.get(a) or {}
    for i in range(lookback_days + 1):
        probe = day - timedelta(days=i)
        k = probe.isoformat()
        if k in per_day:
            return per_day[k], k, i
    return None, None, lookback_days


def _normalize_transactions_to_eur(
    txs: List[Transaction],
    *,
    db: Session,
    strict_fx: bool,
    warnings: list[str],
    fx_meta: dict[str, Any] | None = None,
    fee_val_meta: dict[str, Any] | None = None,
) -> List[Transaction]:
    """
    Convert all quote legs used by FIFO into EUR *before* FIFO runs.

    Why this matters:
    - FIFO must compute proceeds vs cost_basis in the same currency.
    - Filtered summaries (summary_filtered + subset PDF) sum realized_events directly.
    """
    seen: set[str] = set()
    out: List[Transaction] = []
    
    if fx_meta is None:
        fx_meta = {}
    fx_meta.setdefault("fallback_days", set())
    fx_meta.setdefault("fallback_pairs", set())
    if fee_val_meta is None:
        fee_val_meta = {}
    fee_val_meta.setdefault("third_fee_detected", 0)
    fee_val_meta.setdefault("third_fee_valued", 0)
    fee_val_meta.setdefault("missing_price_days", set())
    fee_val_meta.setdefault("missing_price_pairs", set())
    fee_val_meta.setdefault("internal_price_used", 0)
    fee_val_meta.setdefault("internal_price_assets", set())
    fee_val_meta.setdefault("internal_price_fallback_days", set())

    # Determine which fee assets actually need third-asset valuation
    fee_assets_needed: set[str] = set()
    for t in txs:
        try:
            if getattr(t, "fee_amount", None) is None or _D(getattr(t, "fee_amount", None)) == 0:
                continue
            fa = (t.fee_asset or "").upper().strip()
            if not fa or fa in ("EUR",) or fa in USD_LIKE_QUOTES:
                continue
            base = (t.base_asset or "").upper().strip()
            quote = (t.quote_asset or "").upper().strip()
            if fa not in {base, quote}:
                fee_assets_needed.add(fa)
        except Exception:
            continue

    internal_prices: dict[str, dict[str, Decimal]] = {}
    if FEE_INTERNAL_PRICE_FROM_TRADES and fee_assets_needed:
        internal_prices = _build_internal_eur_price_map_from_trades(
            txs,
            db=db,
            strict_fx=strict_fx,
            warnings=warnings,
            seen=seen,
            fx_meta=fx_meta,
            fee_assets_needed=fee_assets_needed,
        )


    for t in txs:
        ttype = (t.type or "").strip().lower()
        if ttype == "transfer":
            out.append(t)
            continue

        # If there is no quote_amount, keep as-is (FIFO will treat missing quotes as warnings).
        if getattr(t, "quote_amount", None) is None:
            out.append(t)
            continue

        day = t.timestamp.date()

        q = (t.quote_asset or "").upper().strip()
        qa = _D(t.quote_amount)

        updates: dict[str, Any] = {}

        # Convert quote_amount if needed
        if q not in ("", "EUR"):
            if q in USD_LIKE_QUOTES:
                qa_eur = _quote_amount_to_eur(
                    qa,
                    quote_asset=q,
                    day=day,
                    db=db,
                    strict_fx=strict_fx,
                    warnings=warnings,
                    seen=seen,
                    fx_meta=fx_meta,
                )
                updates["quote_asset"] = "EUR"
                updates["quote_amount"] = qa_eur
            else:
                # Non-USD non-EUR quote: require fair_value as EUR total
                fv = getattr(t, "fair_value", None)
                if fv is None:
                    raise ValueError(
                        f"Unsupported quote asset '{q}' at {t.timestamp.isoformat()}. "
                        "Provide fair_value (EUR) for this transaction."
                    )
                _warn_once(
                    warnings,
                    seen,
                    f"Used fair_value as EUR quote amount for unsupported quote asset '{q}' on {day.isoformat()} (audit note).",
                )
                updates["quote_asset"] = "EUR"
                updates["quote_amount"] = _D(fv)

        # Convert fee if it's USD-like (keeps FIFO fee-in-quote logic consistent)
        fee_asset = (t.fee_asset or "").upper().strip()
        fee_amount = getattr(t, "fee_amount", None)

        if fee_amount is not None and fee_asset in USD_LIKE_QUOTES:
            fee_eur = _quote_amount_to_eur(
                _D(fee_amount),
                quote_asset=fee_asset,
                day=day,
                db=db,
                strict_fx=strict_fx,
                warnings=warnings,
                seen=seen,
                fx_meta=fx_meta,
            )
            updates["fee_asset"] = "EUR"
            updates["fee_amount"] = fee_eur

        # --- Third-asset fee valuation (e.g., BNB fees) ---
        # If fee is neither base nor quote (after normalization), value it into EUR (if price exists)
        # and add a synthetic disposal so FIFO can realize gain/loss on the fee asset itself.
        synthetic_fee_tx: Transaction | None = None

        base_asset = (t.base_asset or "").upper().strip()
        effective_quote_asset = (updates.get("quote_asset") or t.quote_asset or "").upper().strip()

        original_fee_asset = (t.fee_asset or "").upper().strip()
        original_fee_amount = getattr(t, "fee_amount", None)

        if original_fee_amount is not None and original_fee_amount != 0:
            fee_amt_dec = _D(original_fee_amount)
            if fee_amt_dec < 0:
                _warn_once(warnings, seen, f"Fee amount negative at {t.timestamp}; using absolute value (audit note).")
                fee_amt_dec = abs(fee_amt_dec)

            # Fee asset/amount after any USD-like conversion above (updates may have changed them).
            current_fee_asset = (updates.get("fee_asset") or original_fee_asset or "").upper().strip()
            current_fee_amt = _D(updates.get("fee_amount")) if "fee_amount" in updates else fee_amt_dec

            # Only act if the fee remains in a third asset (not base, not quote, not EUR).
            if current_fee_asset not in ("", "EUR") and current_fee_asset not in {base_asset, effective_quote_asset}:
                fee_val_meta["third_fee_detected"] = int(fee_val_meta.get("third_fee_detected", 0)) + 1

                # Use fx_rates as a generic daily rate store: base=<ASSET>, quote=EUR, rate=<EUR per 1 ASSET>
                lookup = ensure_rate_or_default_lookup(
                    db,
                    day,
                    base=current_fee_asset,
                    quote="EUR",
                    default_rate=Decimal("0"),
                    max_lookback_days=FEE_DB_PRICE_LOOKBACK_DAYS,
                )
                rate = lookup.rate if isinstance(lookup.rate, Decimal) else Decimal(str(lookup.rate))
                can_value = (not lookup.used_fallback) and (rate > 0)

                # Internal fallback: derive price from user's own trades (e.g., BNBUSDT) + FX conversion
                if (not can_value) and FEE_INTERNAL_PRICE_FROM_TRADES and internal_prices:
                    hit_rate, matched_day, looked_back = _internal_price_lookup(
                        internal_prices,
                        asset=current_fee_asset,
                        day=day,
                        lookback_days=FEE_INTERNAL_PRICE_LOOKBACK_DAYS,
                    )
                    if hit_rate is not None and hit_rate > 0:
                        rate = hit_rate
                        can_value = True
                        fee_val_meta["internal_price_used"] = int(fee_val_meta.get("internal_price_used", 0)) + 1
                        fee_val_meta.setdefault("internal_price_assets", set()).add(f"{current_fee_asset}/EUR")
                        if looked_back > 0:
                            fee_val_meta.setdefault("internal_price_fallback_days", set()).add(day.isoformat())

                if not can_value:
                    _warn_once(
                        warnings,
                        seen,
                        f"Fee FX lookup debug: asset={current_fee_asset} day={day.isoformat()} "
                        f"rate={rate} used_fallback={getattr(lookup, 'used_fallback', None)} "
                        f"lookback_days={getattr(lookup, 'lookback_days', None)} "
                        f"matched_date={getattr(lookup, 'matched_date', None)}"
                    )
                    
                    fee_val_meta.setdefault("missing_price_days", set()).add(day.isoformat())
                    fee_val_meta.setdefault("missing_price_pairs", set()).add(f"{current_fee_asset}/EUR")

                    if STRICT_FEE_VALUATION:
                        raise ValueError(
                            f"Strict fee valuation: missing {current_fee_asset}->EUR price for {day.isoformat()}. "
                            "Load daily prices into fx_rates (base=<ASSET>, quote=EUR) and re-run."
                        )

                    _warn_once(
                        warnings,
                        seen,
                        (
                            f"Fee valuation incomplete: missing {current_fee_asset}/EUR price for {day.isoformat()}. "
                            "This fee is recorded but not applied; results may be materially inaccurate. "
                            "Load daily prices (base=<ASSET>, quote=EUR) to enable valuation."
                        ),
                    )
                else:
                    fee_eur = (current_fee_amt * rate).quantize(Decimal("0.00000001"))
                    updates["fee_asset"] = "EUR"
                    updates["fee_amount"] = fee_eur

                    fee_val_meta["third_fee_valued"] = int(fee_val_meta.get("third_fee_valued", 0)) + 1

                    synthetic_fee_tx = Transaction(
                        timestamp=t.timestamp,
                        type="trade",
                        base_asset=current_fee_asset,
                        base_amount=current_fee_amt,
                        quote_asset="EUR",
                        quote_amount=fee_eur,
                        fee_asset=None,
                        fee_amount=None,
                        exchange=getattr(t, "exchange", None),
                        memo=((getattr(t, "memo", None) or "") + f" | synthetic fee disposal ({current_fee_asset} fee)"),
                        fair_value=None,
                    )

        t_out = t.model_copy(update=updates) if updates else t
        out.append(t_out)
        if synthetic_fee_tx is not None:
            out.append(synthetic_fee_tx)

    return out


# ==========================
# Core Calculation Runner
# ==========================

def run_calculation(db: Session, run, cfg: CalcConfig) -> RunSummary:
    rows: List[TransactionRow] = (
        db.query(TransactionRow)
        .order_by(TransactionRow.timestamp.asc(), TransactionRow.id.asc())
        .all()
    )
    return _run_core(db=db, run=run, cfg=cfg, rows=rows, persist_events=True)


def _run_core(
    *,
    db: Session,
    run,
    cfg: CalcConfig,
    rows: list[TransactionRow],
    persist_events: bool = True,
) -> RunSummary:
    start_time = time.time()
    start_ts = _now_iso_z()
    perf0 = time.perf_counter()
    timings_ms: dict[str, int] = {}

    def _mark(name: str, t0: float, t1: float) -> None:
        timings_ms[name] = int(round((t1 - t0) * 1000))
    logger.info(f"Run {getattr(run, 'id', None)} started for {cfg.jurisdiction} at {start_ts}")

    # Build Pydantic transactions from DB rows (apply wallet OUT overrides before FIFO)
    t_load0 = time.perf_counter()

    preload_warnings: list[str] = []

    # Load overrides once (keyed by transaction_id)
    overrides_by_txid = {}
    try:
        for o in db.query(WalletOutOverride).all():
            try:
                overrides_by_txid[int(o.transaction_id)] = o
            except Exception:
                continue
    except Exception:
        # If table is missing/migration not applied yet, proceed without overrides.
        overrides_by_txid = {}

    tx_models: List[Transaction] = []
    for r in rows:
        r_type = r.type
        r_quote_asset = r.quote_asset
        r_quote_amount = r.quote_amount
        r_fee_asset = r.fee_asset
        r_fee_amount = r.fee_amount

        ov = overrides_by_txid.get(int(r.id))
        if ov is not None:
            cls = (ov.classification or "").strip().lower()

            # Backward-compat: treat old "taxable" as "sell"
            if cls == "taxable":
                cls = "sell"

            if cls in {"sell", "buy"}:
                if ov.proceeds_eur is None:
                    preload_warnings.append(
                        f"Wallet transfer override missing proceeds_eur for tx_id={r.id}; treated as TRANSFER."
                    )
                else:
                    # SELL/BUY use explicit EUR proceeds/cost.
                    r_type = "SELL" if cls == "sell" else "BUY"
                    r_quote_asset = "EUR"
                    r_quote_amount = ov.proceeds_eur
                    
                    # Wallet-derived synthetic trades: do NOT treat network fees as trade fees.
                    r_fee_asset = None
                    r_fee_amount = None

        tx_models.append(
            Transaction(
                timestamp=r.timestamp,
                type=r_type,
                base_asset=r.base_asset,
                base_amount=(abs(_D(r.base_amount)) if r_type in {"SELL", "BUY"} else _D(r.base_amount)),
                quote_asset=r_quote_asset,
                quote_amount=_D(r_quote_amount) if r_quote_amount is not None else None,
                fee_asset=r_fee_asset,
                fee_amount=_D(r_fee_amount) if r_fee_amount is not None else None,
                exchange=r.exchange,
                memo=r.memo,
                fair_value=_D(getattr(r, "fair_value", None)) if getattr(r, "fair_value", None) is not None else None,
            )
        )

    _mark("load_tx_models", t_load0, time.perf_counter())

    logger.info(f"Loaded {len(tx_models)} transactions from DB")
    
    inputs_to_persist: list[dict[str, Any]] = []
    if persist_events and getattr(run, "id", None) is not None:
        seen_hashes: set[str] = set()
        for r in rows:
            h = getattr(r, "hash", None)
            if not h:
                continue
            if h in seen_hashes:
                continue
            seen_hashes.add(h)
            inputs_to_persist.append({"run_id": int(run.id), "tx_hash": str(h)})

    strict_fx_configured = bool(getattr(cfg, "strict_fx", False))
    strict_fx_enforced_by_env = bool(IS_PROD and not ALLOW_FX_FALLBACK_IN_PROD)
    strict_fx = bool(strict_fx_configured or strict_fx_enforced_by_env)
    strict_fx_source = "cfg" if strict_fx_configured else ("prod_enforced" if strict_fx_enforced_by_env else "disabled")
    warnings: list[str] = []
    
    # Carry forward any warnings from override application during tx model build
    try:
        warnings.extend(preload_warnings)
    except Exception:
        pass

    fx_meta: dict[str, Any] = {"fallback_days": set(), "fallback_pairs": set()}
    fee_val_meta: dict[str, Any] = {
        "third_fee_detected": 0,
        "third_fee_valued": 0,
        "missing_price_days": set(),
        "missing_price_pairs": set(),
    }
        
    # Ensure third-asset fee prices exist before fee valuation / FIFO (same-request, deterministic)
    try:
        from cryptotaxcalc.price_autosync import price_autosync_tick
        res = price_autosync_tick(reason="pre-calc", db=db)
        
        # DB sanity check: confirm BNB/EUR exists in the same DB this run is using
        try:
            from sqlalchemy import text
            from datetime import timezone

            # Use the run's date span (min/max tx date) to avoid guessing a range
            min_day = min(t.timestamp.date() for t in tx_models) if tx_models else None
            max_day = max(t.timestamp.date() for t in tx_models) if tx_models else None

            if min_day and max_day:
                n = db.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM fx_rates
                        WHERE base = 'BNB' AND quote = 'EUR'
                        AND date >= :s AND date <= :e
                        """
                    ),
                    {"s": min_day.isoformat(), "e": max_day.isoformat()},
                ).scalar() or 0

                if _debug_warnings_enabled():
                    warnings.append(f"FX check: BNB/EUR rows in fx_rates for {min_day}..{max_day} = {int(n)}")
        except Exception as e:
            warnings.append(f"FX check failed: {e}")

        if isinstance(res, dict):
            enabled = bool(res.get("enabled", True))
            ok = bool(res.get("ok", True))
            provider = str(res.get("provider") or "")
            quote = str(res.get("quote") or "")
            inserted = int(res.get("inserted") or 0)
            skipped = int(res.get("skipped") or 0)
            errors = int(res.get("errors") or 0)

            details = res.get("details") or {}
            assets = details.get("assets") if isinstance(details, dict) else None

            if not enabled:
                warnings.append("Price autosync is disabled; third-asset fee valuation may be incomplete.")
            elif not ok:
                warnings.append(f"Price autosync returned error: {res.get('error')}")
            else:
                diag = f"Price autosync diag: provider={provider} quote={quote} inserted={inserted} skipped={skipped} errors={errors}"
                if isinstance(assets, list) and assets:
                    # show first 3 assets for quick diagnosis
                    parts = []
                    for a in assets[:3]:
                        try:
                            parts.append(
                                f"{a.get('asset')}:{a.get('provider_used')} fetched={a.get('fetched_days')} "
                                f"ins={a.get('inserted')} err={a.get('errors')}"
                                + (f" msg={a.get('error')}" if a.get("error") else "")
                            )
                        except Exception:
                            continue
                    if parts:
                        diag += " | " + " ; ".join(parts)
                if _debug_warnings_enabled():
                    warnings.append(diag)
    except Exception as e:
        warnings.append(f"Price autosync failed (third-asset fee valuation may be incomplete): {e}")

    # Phase 1: EUR-canonical quote legs BEFORE FIFO
    t_norm0 = time.perf_counter()
    try:
        warn_len_before_norm = len(warnings)
        tx_models_eur = _normalize_transactions_to_eur(
            tx_models,
            db=db,
            strict_fx=strict_fx,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_val_meta,
        )

        # If fee valuation reported missing daily prices, run one autosync retry and normalize once more.
        missing_days = fee_val_meta.get("missing_price_days") if isinstance(fee_val_meta, dict) else None
        if missing_days:
            try:
                from cryptotaxcalc.price_autosync import price_autosync_tick
                price_autosync_tick(reason="fee-missing-days", db=db)
            except Exception as e2:
                warnings.append(f"Price autosync retry failed (missing fee days may remain): {e2}")
            else:
                # Clear stale missing sets and retry normalization once
                fee_val_meta["missing_price_days"] = set()
                fee_val_meta["missing_price_pairs"] = set()
                tx_models_eur = _normalize_transactions_to_eur(
                    tx_models,
                    db=db,
                    strict_fx=strict_fx,
                    warnings=warnings,
                    fx_meta=fx_meta,
                    fee_val_meta=fee_val_meta,
                )

                # If retry succeeded (no missing days left), remove stale "incomplete" warnings from the first pass
                if not fee_val_meta.get("missing_price_days"):
                    tail = warnings[warn_len_before_norm:]
                    tail = [
                        w for w in tail
                        if not (isinstance(w, str) and (
                            w.startswith("Fee valuation incomplete:")
                            or w.startswith("Fee FX lookup debug:")
                        ))
                    ]
                    warnings[:] = warnings[:warn_len_before_norm] + tail

    except Exception as e:
        log_workspace_error(
            stage="normalize_to_eur",
            cfg=cfg,
            run_id=getattr(run, "id", None),
            error=e,
        )
        raise
    _mark("normalize_to_eur", t_norm0, time.perf_counter())

    fx_fallback_days = sorted(list(fx_meta.get("fallback_days", set())))
    fx_fallback_used = bool(fx_fallback_days)
    fx_fallback_days_count = len(fx_fallback_days)
    fx_fallback_days_sample = fx_fallback_days[:10]
    fx_fallback_pairs = sorted(list(fx_meta.get("fallback_pairs", set())))
    third_fee_detected = int(fee_val_meta.get("third_fee_detected", 0))
    third_fee_valued = int(fee_val_meta.get("third_fee_valued", 0))
    fee_price_missing_days = sorted(list(fee_val_meta.get("missing_price_days", set())))
    fee_price_missing_pairs = sorted(list(fee_val_meta.get("missing_price_pairs", set())))
    fee_price_missing_days_sample = fee_price_missing_days[:10]
    fee_price_missing_pairs_sample = fee_price_missing_pairs[:10]

    if third_fee_detected > 0:
        if third_fee_valued > 0:
            warnings.append(
                f"Fee valuation: valued {third_fee_valued}/{third_fee_detected} third-asset fees into EUR and created synthetic fee disposals."
            )
        # If valuation fully succeeded, clear any stale missing-day state and stale warnings from earlier passes
        if third_fee_valued >= third_fee_detected:
            try:
                fee_val_meta["missing_price_days"] = set()
                fee_val_meta["missing_price_pairs"] = set()
            except Exception:
                pass

            # Remove stale per-tx warnings emitted during the first pass
            warnings[:] = [
                w for w in warnings
                if not (isinstance(w, str) and w.startswith("Fee valuation incomplete: missing "))
            ]
        if third_fee_valued < third_fee_detected:
            pairs_s = ", ".join(fee_price_missing_pairs_sample[:5]) or "–"
            days_s = ", ".join(fee_price_missing_days_sample[:5]) or "–"
            warnings.append(
                "Fee valuation incomplete: some third-asset fee prices are missing "
                f"(pairs: {pairs_s}; days sample: {days_s}). "
                "Those fees are recorded but not applied. Load daily prices into fx_rates (base=<ASSET>, quote=EUR) and re-run."
            )
    
    internal_used = int(fee_val_meta.get("internal_price_used", 0))
    internal_assets = sorted(list(fee_val_meta.get("internal_price_assets", set())))
    if internal_used > 0:
        assets_s = ", ".join(internal_assets[:5]) or "–"
        warnings.append(
            f"Fee valuation: used internally derived EUR prices for {internal_used} third-asset fee events "
            f"(assets: {assets_s}; lookback up to {FEE_INTERNAL_PRICE_LOOKBACK_DAYS} day(s))."
        )

    # Run FIFO engine on EUR-canonical transactions
    events: list[Any] = []
    summary: dict[str, Any] = {}
    fifo_warnings: list[str] = []
    try:
        t_fifo0 = time.perf_counter()
        events, summary, fifo_warnings = compute_fifo(tx_models_eur, enable_diagnostics=True)
        _mark("fifo_compute", t_fifo0, time.perf_counter())
        warnings.extend(fifo_warnings)
    except Exception as e:
        logger.exception(f"FIFO engine failed: {e}")
        warnings.append(f"FIFO engine error: {e}")

        log_workspace_error(
            stage="fifo_compute",
            cfg=cfg,
            run_id=getattr(run, "id", None),
            error=e,
        )

        summary = {"totals": {"proceeds": "0", "cost_basis": "0", "gain": "0"}}

    # Attach / create FX batch for this run (used in diagnostics/summary_json)
    fx_batch_id = get_or_create_current_fx_batch_id(db)

    # Trust metadata (UI + audit): make available even for subset runs (persist_events=False).
    fx_context = {
        "fx_batch_id": fx_batch_id,
        "jurisdiction": cfg.jurisdiction,
        "fx_rate_used": "USD->EUR via fx_rates table (lookback up to 7 days)",
        "strict_fx": strict_fx,
        "strict_fx_source": strict_fx_source,
        "fx_fallback_used": fx_fallback_used,
        "fx_fallback_days_count": fx_fallback_days_count,
        "fx_fallback_days_sample": fx_fallback_days_sample,
        "fx_fallback_pairs": fx_fallback_pairs,
    }

    fee_valuation = {
        "strict_fee_valuation": bool(STRICT_FEE_VALUATION),
        "third_asset_fee_detected": third_fee_detected,
        "third_asset_fee_valued": third_fee_valued,
        "missing_price_days_count": len(fee_price_missing_days),
        "missing_price_days_sample": fee_price_missing_days_sample,
        "missing_price_pairs_sample": fee_price_missing_pairs_sample,
        "internal_price_used": int(fee_val_meta.get("internal_price_used", 0)),
        "internal_price_assets_sample": sorted(list(fee_val_meta.get("internal_price_assets", set())))[:10],
        "internal_price_lookback_days": int(FEE_INTERNAL_PRICE_LOOKBACK_DAYS),
        "db_price_lookback_days": int(FEE_DB_PRICE_LOOKBACK_DAYS),
    }

    # Safety: after Phase 1 normalization, events must be EUR-only.
    non_eur = sorted({
        (getattr(ev, "quote_asset", "") or "").upper()
        for ev in events
        if (getattr(ev, "quote_asset", "") or "").upper() not in ("EUR", "")
    })
    if non_eur:
        err = ValueError(
            "Non-EUR realized events detected after pre-FIFO normalization: "
            f"{', '.join(non_eur)}. "
            "This indicates input normalization did not canonicalize quote legs to EUR."
        )
        log_workspace_error(
            stage="post_normalization_non_eur",
            cfg=cfg,
            run_id=getattr(run, "id", None),
            error=err,
            extra={"non_eur": non_eur},
        )
        raise err

    # EUR totals are now the FIFO totals
    totals_dict = summary.get("totals", {}) if isinstance(summary, dict) else {}
    proceeds_eur = _D(totals_dict.get("proceeds"))
    cost_eur = _D(totals_dict.get("cost_basis"))
    gain_eur = _D(totals_dict.get("gain"))

    # Apply jurisdiction-specific taxable vs exempt logic
    rule = _rule_for(cfg)
    ctx = RunContext(cfg=cfg, tax_year=getattr(run, "tax_year", datetime.now(timezone.utc).year))

    taxable_gain_eur = Decimal("0")
    exempt_gain_eur = Decimal("0")

    # Some jurisdictions (e.g., HR) need per-lot match context to compute exemptions.
    needs_match_level_split = callable(getattr(rule, "split_taxable_exempt_gain", None))

    matches_eur: List[dict[str, Any]] = []
    matches_eur_by_event: dict[int, list[dict[str, Any]]] = {}

    if needs_match_level_split:
        # Build per-lot EUR matches (and per-event mapping for matches_json persistence).
        for ev in events:
            try:
                cost_event = _D(getattr(ev, "cost_basis", "0"))
                proceeds_event = _D(getattr(ev, "proceeds", "0"))
                qty_sold_event = _D(getattr(ev, "qty_sold", "0"))
                if qty_sold_event <= 0:
                    continue

                ev_ts = getattr(ev, "timestamp", None)
                disposed_str = str(ev_ts) if ev_ts is not None else None

                ev_key = id(ev)

                for m in getattr(ev, "matches", []) or []:
                    from_qty = _D(getattr(m, "from_qty", "0"))
                    lot_cost_total = _D(getattr(m, "lot_cost_total", "0"))

                    if from_qty <= 0:
                        continue
                    if lot_cost_total < 0:
                        continue

                    # Cost allocation: proportional to lot cost within the event
                    if cost_event > 0:
                        cost_ratio = lot_cost_total / cost_event
                        match_cost_eur = cost_event * cost_ratio
                    else:
                        match_cost_eur = Decimal("0")

                    # Proceeds allocation: proportional to quantity sold
                    qty_ratio = from_qty / qty_sold_event
                    match_proceeds_eur = proceeds_event * qty_ratio

                    acquired_at = getattr(m, "acquired_at", None)
                    acquired_str = (
                        acquired_at.isoformat()
                        if hasattr(acquired_at, "isoformat")
                        else (str(acquired_at) if acquired_at is not None else None)
                    )

                    match_dict = {
                        "proceeds_eur": str(match_proceeds_eur),
                        "cost_eur": str(match_cost_eur),
                        "acquired_at": acquired_str,
                        "disposed_at": disposed_str,
                    }
                    matches_eur.append(match_dict)
                    matches_eur_by_event.setdefault(ev_key, []).append(match_dict)
            except Exception as e:
                logger.warning(f"Match-level aggregation error: {e}")
                continue

        # Sanity check: match-derived total gain should reconcile with FIFO summary (within 1 cent).
        try:
            match_total_gain = sum(
                (_D(m.get("proceeds_eur")) - _D(m.get("cost_eur"))) for m in matches_eur
            )
            delta = match_total_gain - gain_eur
            if delta.copy_abs() > Decimal("0.01"):
                warnings.append(f"Match-level gain mismatch vs FIFO total: {delta} EUR.")
        except Exception:
            pass
    
    t_tax0 = time.perf_counter()

    taxable_gain_eur, exempt_gain_eur = split_taxable_exempt_gain(
        rule=rule,
        gain_eur=gain_eur,
        matches_raw=matches_eur,
        ctx=ctx,
    )
    
    _mark("tax_split", t_tax0, time.perf_counter())
    
    totals = RunTotals(
        proceeds_eur=proceeds_eur,
        cost_eur=cost_eur,
        gain_eur=gain_eur,
        taxable_gain_eur=taxable_gain_eur,
        exempt_gain_eur=exempt_gain_eur,
    )

    lots_processed = len(events)

    # Persist realized events for history
    if persist_events:
        t_persist0 = time.perf_counter()
        # Freeze input transaction hash set for stable audit manifests
        if getattr(run, "id", None) is not None:
            try:
                db.query(RunInput).filter(RunInput.run_id == int(run.id)).delete(synchronize_session=False)
                if inputs_to_persist:
                    db.bulk_insert_mappings(RunInput, inputs_to_persist)
            except Exception as e:
                logger.warning(f"Failed to persist run_inputs snapshot: {e}")

        _mark("persist_inputs", t_persist0, time.perf_counter())
        t_events0 = time.perf_counter()
        to_insert: list[dict[str, Any]] = []

        for ev in events:
            try:
                matches_payload: list[dict[str, Any]] = []
                if needs_match_level_split:
                    matches_payload = matches_eur_by_event.get(id(ev), [])
                else:
                    for m in getattr(ev, "matches", []) or []:
                        acquired_at = getattr(m, "acquired_at", None)
                        acquired_str = (
                            acquired_at.isoformat()
                            if hasattr(acquired_at, "isoformat")
                            else (str(acquired_at) if acquired_at is not None else None)
                        )
                        matches_payload.append(
                            {
                                "from_qty": str(getattr(m, "from_qty", "")),
                                "lot_cost_per_unit": str(getattr(m, "lot_cost_per_unit", "")),
                                "lot_cost_total": str(getattr(m, "lot_cost_total", "")),
                                "acquired_at": acquired_str,
                                "disposed_at": getattr(ev, "timestamp", None),
                            }
                        )

                to_insert.append(
                    {
                        "run_id": int(run.id),
                        "tx_id": None,
                        "timestamp": getattr(ev, "timestamp", None),
                        "asset": getattr(ev, "asset", None),
                        "qty_sold": _D(getattr(ev, "qty_sold", "0")),
                        "proceeds": _D(getattr(ev, "proceeds", "0")),
                        "cost_basis": _D(getattr(ev, "cost_basis", "0")),
                        "gain": _D(getattr(ev, "gain", "0")),
                        "quote_asset": "EUR",
                        "fee_applied": (
                            _D(getattr(ev, "fee_applied", "0"))
                            if getattr(ev, "fee_applied", None) is not None
                            else None
                        ),
                        "matches_json": json.dumps(matches_payload, ensure_ascii=False) if matches_payload else None,
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to prepare RealizedEvent mapping: {e}")

        if to_insert:
            BULK_CHUNK = 2000
            for i in range(0, len(to_insert), BULK_CHUNK):
                db.bulk_insert_mappings(RealizedEvent, to_insert[i:i + BULK_CHUNK])

        db.commit()

        # Audit diagnostics + persist summary_json
        try:
            out_dir = Path("logs/calc")
            out_dir.mkdir(parents=True, exist_ok=True)

            eur_summary = {
                "totals_eur": {
                    "proceeds": str(proceeds_eur.quantize(Decimal("0.01"))),
                    "cost_basis": str(cost_eur.quantize(Decimal("0.01"))),
                    "gain": str(gain_eur.quantize(Decimal("0.01"))),
                },
                "notes": [
                    "Phase 1: transactions were converted to EUR at their own timestamps (pre-FIFO).",
                    "Realised events are EUR-canonical; filtered summaries aggregate stored EUR values.",
                ],
            }

            summary_json = {
                "timings_ms": timings_ms,
                "run_id": run.id,
                "jurisdiction": cfg.jurisdiction,
                "rule_version": cfg.rule_version,
                "tax_year": getattr(run, "tax_year", datetime.now(timezone.utc).year),
                "strict_fx_configured": strict_fx_configured,
                "strict_fx_effective": strict_fx,
                "strict_fx_source": strict_fx_source,
                "fx_batch_id": fx_batch_id,
                "fx_fallback_used": fx_fallback_used,
                "fx_fallback_days_count": fx_fallback_days_count,
                "fx_fallback_days_sample": fx_fallback_days_sample,
                "fx_fallback_pairs": fx_fallback_pairs,
                "fx_context": fx_context,
                "fee_valuation": fee_valuation,
                "lots_processed": lots_processed,
                "totals": totals.model_dump(),
                "eur_summary": eur_summary,
                "raw_fifo_totals": {
                    "proceeds_quote": str(proceeds_eur),
                    "cost_quote": str(cost_eur),
                    "gain_quote": str(gain_eur),
                },
                "warnings": warnings,
            }

            t_sjson0 = time.perf_counter()
            try:
                run.summary_json = summary_json
                db.add(run)
                db.commit()
            except Exception as e:
                logger.warning(f"Failed to persist run.summary_json: {e}")
                db.rollback()
            _mark("persist_summary_json", t_sjson0, time.perf_counter())

            payload = {
                "timestamp": start_ts,
                "run_id": getattr(run, "id", None),
                "jurisdiction": cfg.jurisdiction,
                "tx_count": len(tx_models),
                "events_count": len(events),
                "warnings_count": len(warnings),
                "summary": summary_json,
            }
            _atomic_write_json(out_dir / "last_run.json", payload)
        except Exception as e:
            logger.warning(f"Could not write calculation diagnostics: {e}")
            
        _mark("persist_events", t_events0, time.perf_counter())

    elapsed = round(time.time() - start_time, 3)
    logger.info(f"Run {getattr(run, 'id', None)} completed in {elapsed}s | events={len(events)} warnings={len(warnings)}")

    _mark("total", perf0, time.perf_counter())

    return RunSummary(
        fx_fallback_used=fx_fallback_used,
        fx_fallback_days_count=fx_fallback_days_count,
        fx_fallback_days_sample=fx_fallback_days_sample,
        run_id=int(run.id) if getattr(run, "id", None) is not None else 0,
        jurisdiction=cfg.jurisdiction,
        rule_version=cfg.rule_version,
        tax_year=getattr(run, "tax_year", datetime.now(timezone.utc).year),
        fx_batch_id=fx_batch_id,
        strict_fx=strict_fx,
        strict_fx_source=strict_fx_source,
        fx_fallback_pairs=fx_fallback_pairs,
        fx_context=fx_context,
        fee_valuation=fee_valuation,
        lots_processed=lots_processed,
        totals=totals,
        warnings=warnings,
        timings_ms=timings_ms,
    )


def run_calculation_on_subset(
    db: Session,
    cfg: CalcConfig,
    rows: list[TransactionRow],
) -> RunSummary:
    """
    Run the calculation engine on a pre-filtered subset of TransactionRow.

    - Does not persist CalcRun or RealizedEvent rows.
    - Still applies Phase 1 EUR normalization for correctness.
    """
    dummy_run = type(
        "SubsetRun",
        (),
        {"id": None, "tax_year": getattr(cfg, "tax_year", datetime.now(timezone.utc).year)},
    )()
    return _run_core(db=db, run=dummy_run, cfg=cfg, rows=rows, persist_events=False)

# calc_runner.py – EUR-canonical, auditable calculation runner
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, List

import os
import json
import time
import traceback

from sqlalchemy.orm import Session

from cryptotaxcalc.schemas import CalcConfig, RunSummary, RunTotals, Transaction
from cryptotaxcalc.rules.registry import get_rule, split_taxable_exempt_gain
from cryptotaxcalc.rules.base import TaxRule, RunContext
from cryptotaxcalc.models import TransactionRow, RealizedEvent, RunInput
from cryptotaxcalc.fifo_engine import compute_fifo
from cryptotaxcalc.fx_utils import ensure_rate_or_default_lookup, get_or_create_current_fx_batch_id
from cryptotaxcalc.logging_setup import get_logger, _atomic_write_json, _now_iso_z

logger = get_logger("calc")
WORKSPACE_LOG_DIR = Path("logs/workspace")
WORKSPACE_ERRORS_TXT = WORKSPACE_LOG_DIR / "errors.txt"
WORKSPACE_ERROR_PATH_POINTER = WORKSPACE_LOG_DIR.parent / "workspace_error_log_path.txt"


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
    }

    if extra:
        payload["extra"] = extra

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
                    max_lookback_days=0,
                )
                rate = lookup.rate if isinstance(lookup.rate, Decimal) else Decimal(str(lookup.rate))
                can_value = (not lookup.used_fallback) and (rate > 0)

                if not can_value:
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

    # Build Pydantic transactions from DB rows
    t_load0 = time.perf_counter()
    tx_models: List[Transaction] = [
        Transaction(
            timestamp=r.timestamp,
            type=r.type,
            base_asset=r.base_asset,
            base_amount=_D(r.base_amount),
            quote_asset=r.quote_asset,
            quote_amount=_D(r.quote_amount) if r.quote_amount is not None else None,
            fee_asset=r.fee_asset,
            fee_amount=_D(r.fee_amount) if r.fee_amount is not None else None,
            exchange=r.exchange,
            memo=r.memo,
            fair_value=_D(getattr(r, "fair_value", None)) if getattr(r, "fair_value", None) is not None else None,
        )
        for r in rows
    ]
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

    fx_meta: dict[str, Any] = {"fallback_days": set(), "fallback_pairs": set()}
    fee_val_meta: dict[str, Any] = {
        "third_fee_detected": 0,
        "third_fee_valued": 0,
        "missing_price_days": set(),
        "missing_price_pairs": set(),
    }

    # Phase 1: EUR-canonical quote legs BEFORE FIFO
    t_norm0 = time.perf_counter()
    try:
        tx_models_eur = _normalize_transactions_to_eur(
            tx_models,
            db=db,
            strict_fx=strict_fx,
            warnings=warnings,
            fx_meta=fx_meta,
            fee_val_meta=fee_val_meta,
        )
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
        if third_fee_valued < third_fee_detected:
            pairs_s = ", ".join(fee_price_missing_pairs_sample[:5]) or "–"
            days_s = ", ".join(fee_price_missing_days_sample[:5]) or "–"
            warnings.append(
                "Fee valuation incomplete: some third-asset fee prices are missing "
                f"(pairs: {pairs_s}; days sample: {days_s}). "
                "Those fees are recorded but not applied. Load daily prices into fx_rates (base=<ASSET>, quote=EUR) and re-run."
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
                "fx_context": {
                    "fx_batch_id": fx_batch_id,
                    "jurisdiction": cfg.jurisdiction,
                    "fx_rate_used": "USD->EUR via fx_rates table (lookback up to 7 days)",
                    "strict_fx": strict_fx,
                    "strict_fx_source": strict_fx_source,
                    "fx_fallback_used": fx_fallback_used,
                    "fx_fallback_days_count": fx_fallback_days_count,
                    "fx_fallback_days_sample": fx_fallback_days_sample,
                    "fx_fallback_pairs": fx_fallback_pairs,
                },
                "fee_valuation": {
                    "strict_fee_valuation": bool(STRICT_FEE_VALUATION),
                    "third_asset_fee_detected": third_fee_detected,
                    "third_asset_fee_valued": third_fee_valued,
                    "missing_price_days_count": len(fee_price_missing_days),
                    "missing_price_days_sample": fee_price_missing_days_sample,
                    "missing_price_pairs_sample": fee_price_missing_pairs_sample,
                },
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

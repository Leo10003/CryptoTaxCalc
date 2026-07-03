from __future__ import annotations
"""
Audit digest builder.

Responsible for generating reproducible manifests and SHA256 digests for
calculation runs. Used for audit verification and export bundling.
"""

import json
import hashlib
from decimal import Decimal
from typing import Any, Dict, List
from sqlalchemy import text
from pathlib import Path

from cryptotaxcalc.db import engine
from cryptotaxcalc.logging_setup import get_logger, _now_iso_z, _atomic_write_json

logger = get_logger("audit.digest")


# =========================================================
# Internal helpers
# =========================================================

def _dec_to_str(x: Any) -> str:
    """Convert Decimal to plain non-scientific string; otherwise str(x)."""
    if isinstance(x, Decimal):
        s = format(x, "f")
        return s.rstrip("0").rstrip(".") if "." in s else s
    return str(x)


def _json_c14n(obj: Any) -> str:
    """
    Canonical JSON dump:
      - sorted keys
      - compact separators
      - Decimals → plain strings
    """

    def normalize(o: Any):
        if isinstance(o, dict):
            return {k: normalize(o[k]) for k in sorted(o.keys())}
        if isinstance(o, list):
            return [normalize(v) for v in o]
        if isinstance(o, Decimal):
            return _dec_to_str(o)
        return o

    return json.dumps(normalize(obj), sort_keys=True, separators=(",", ":"))


def _sha256_hex(s: str) -> str:
    """Return SHA256 hexdigest for a given string."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# =========================================================
# Core manifest builder
# =========================================================

def build_run_manifest(run_id: int) -> Dict[str, Any]:
    """
    Build a canonical manifest capturing:
      - calc_runs row (jurisdiction, rule_version, lot_method, params_json, timestamps, fx_set_id)
      - fx batch meta (imported_at, source, rates_hash)
      - INPUT SET: ordered list of transaction hashes existing at run finished_at
      - OUTPUT SET: realized_events for this run (stringified numeric fields)
    """
    logger.info(f"[AUDIT] Building run manifest for run_id={run_id}")

    with engine.begin() as conn:
        run = conn.execute(
            text("SELECT * FROM calc_runs WHERE id = :rid"),
            {"rid": run_id},
        ).mappings().first()

        if not run:
            raise ValueError(f"calc_runs id={run_id} not found")

        fx_meta = None
        if run["fx_set_id"] is not None:
            fx_meta = conn.execute(
                text("SELECT * FROM fx_batches WHERE id = :bid"),
                {"bid": run["fx_set_id"]},
            ).mappings().first()

        finished_at = run["finished_at"]

        input_mode = "timestamp_cutoff"
        input_hashes: List[str] = []

        # Prefer frozen run_inputs snapshot if available (stable over time)
        try:
            snap = conn.execute(
                text("""
                    SELECT tx_hash
                    FROM run_inputs
                    WHERE run_id = :rid
                    ORDER BY tx_hash
                """),
                {"rid": run_id},
            ).fetchall()
            if snap:
                input_hashes = [r[0] for r in snap]
                input_mode = "snapshot"
        except Exception:
            # Table may not exist on older DBs; fall back safely.
            input_mode = "timestamp_cutoff"

        if not input_hashes:
            tx_rows = conn.execute(
                text("""
                    SELECT hash FROM transactions
                    WHERE timestamp <= :cutoff
                    ORDER BY hash
                """),
                {"cutoff": finished_at},
            ).fetchall()
            input_hashes = [r[0] for r in tx_rows]

        out_rows = conn.execute(
            text("""
                SELECT timestamp, asset, qty_sold, proceeds, cost_basis, gain,
                       quote_asset, fee_applied, matches_json
                FROM realized_events
                WHERE run_id = :rid
                ORDER BY id
            """),
            {"rid": run_id},
        ).mappings().all()

    outputs: List[Dict[str, Any]] = []
    for r in out_rows:
        outputs.append({
            "timestamp": r["timestamp"],
            "asset": r["asset"],
            "qty_sold": _dec_to_str(r["qty_sold"]),
            "proceeds": _dec_to_str(r["proceeds"]),
            "cost_basis": _dec_to_str(r["cost_basis"]),
            "gain": _dec_to_str(r["gain"]),
            "quote_asset": r["quote_asset"],
            "fee_applied": _dec_to_str(r["fee_applied"]) if r["fee_applied"] is not None else None,
            "matches": json.loads(r["matches_json"] or "[]"),
        })

    # Trust metadata (FX integrity + fee valuation) is stored in calc_runs.summary_json by calc_runner.
    # We include it in the manifest for human/audit review, but we do NOT include it in manifest_hash
    # to preserve backward compatibility of stored digests.
    trust: Dict[str, Any] = {}
    try:
        raw_summary = run.get("summary_json") if isinstance(run, dict) else None
        summary_obj: Any = raw_summary

        if isinstance(summary_obj, str) and summary_obj.strip():
            summary_obj = json.loads(summary_obj)

        if isinstance(summary_obj, dict):
            for k in (
                "strict_fx_configured",
                "strict_fx_effective",
                "strict_fx_source",
                "fx_batch_id",
                "fx_fallback_used",
                "fx_fallback_days_count",
                "fx_fallback_days_sample",
                "fx_fallback_pairs",
            ):
                if k in summary_obj:
                    trust[k] = summary_obj.get(k)

            fx_ctx = summary_obj.get("fx_context")
            if isinstance(fx_ctx, dict) and fx_ctx:
                trust["fx_context"] = fx_ctx

            fee_val = summary_obj.get("fee_valuation")
            if isinstance(fee_val, dict) and fee_val:
                trust["fee_valuation"] = fee_val
    except Exception:
        trust = {}

    manifest: Dict[str, Any] = {
        "timestamp_built": _now_iso_z(),
        "run_id": run["id"],
        "run": {
            "id": run["id"],
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
            "jurisdiction": run["jurisdiction"],
            "rule_version": run["rule_version"],
            "lot_method": run["lot_method"],
            "fx_set_id": run["fx_set_id"],
            "params": json.loads(run["params_json"] or "{}"),
        },
        "fx_batch": {
            "id": fx_meta["id"] if fx_meta else None,
            "imported_at": fx_meta["imported_at"] if fx_meta else None,
            "source": fx_meta["source"] if fx_meta else None,
            "rates_hash": fx_meta["rates_hash"] if fx_meta else None,
        },
        "inputs": {
            "mode": input_mode,
            "cutoff_finished_at": finished_at if input_mode == "timestamp_cutoff" else None,
            "transactions_hashes_ordered": input_hashes,
        },
        "outputs": outputs,
    }

    if trust:
        manifest["trust"] = trust

    # Write debug manifest JSON for traceability
    try:
        out_path = Path("logs/audit") / f"manifest_run_{run_id}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(out_path, manifest)
    except Exception as e:
        logger.warning(f"Could not write manifest debug file: {e}")

    logger.info(f"[AUDIT] Manifest built for run_id={run_id} with {len(outputs)} outputs.")
    return manifest


# =========================================================
# Digest computation
# =========================================================

def compute_digests(manifest: Dict[str, Any]) -> Dict[str, str]:
    """
    Compute three deterministic digests:
      - input_hash: hash of parameters, fx metadata, and input transactions
      - output_hash: hash of realized events list
      - manifest_hash: hash of the full manifest (excluding volatile timestamp_built)
    """
    logger.debug("[AUDIT] Computing digests for manifest")

    # Inputs: rule params + fx meta + ordered input tx hashes
    inputs_part = {
        "run_params": manifest["run"]["params"],
        "input_mode": manifest["inputs"].get("mode"),
        "rule_version": manifest["run"]["rule_version"],
        "lot_method": manifest["run"]["lot_method"],
        "jurisdiction": manifest["run"]["jurisdiction"],
        "fx": manifest["fx_batch"],
        "input_hashes": manifest["inputs"]["transactions_hashes_ordered"],
    }

    # Outputs: realized events (already normalized to strings and ordered)
    outputs_part = manifest["outputs"]

    input_hash = _sha256_hex(_json_c14n(inputs_part))
    output_hash = _sha256_hex(_json_c14n(outputs_part))

    # Manifest hash must ignore volatile build timestamp
    manifest_copy = dict(manifest)
    manifest_copy.pop("timestamp_built", None)
    manifest_copy.pop("trust", None)
    manifest_hash = _sha256_hex(_json_c14n(manifest_copy))

    digests = {
        "input_hash": input_hash,
        "output_hash": output_hash,
        "manifest_hash": manifest_hash,
    }

    logger.info(
        f"[AUDIT] Digests computed input={input_hash[:8]}… "
        f"output={output_hash[:8]}… manifest={manifest_hash[:8]}…"
    )
    return digests

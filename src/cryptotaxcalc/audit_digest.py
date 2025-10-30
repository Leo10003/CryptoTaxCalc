# audit_digest.py
from __future__ import annotations
import json, hashlib, datetime
from decimal import Decimal
from typing import Any, Dict, List
from sqlalchemy import text
from .db import engine

def _dec_to_str(x: Any) -> str:
    if isinstance(x, Decimal):
        return format(x, 'f').rstrip('0').rstrip('.') if '.' in format(x, 'f') else format(x, 'f')
    return str(x)

def _json_c14n(obj: Any) -> str:
    """
    Canonical JSON dump:
      - sort keys
      - no spaces (compact separators)
      - decimals rendered as plain strings
    """
    def normalize(o: Any):
        if isinstance(o, dict):
            return {k: normalize(o[k]) for k in sorted(o.keys())}
        elif isinstance(o, list):
            return [normalize(v) for v in o]
        elif isinstance(o, Decimal):
            return _dec_to_str(o)
        else:
            return o
    norm = normalize(obj)
    return json.dumps(norm, sort_keys=True, separators=(",", ":"))

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def build_run_manifest(run_id: str) -> Dict[str, Any]:
    """
    Build a canonical manifest that captures:
      - calc_runs row (jurisdiction, rule_version, lot_method, params_json, timestamps, fx_set_id)
      - fx batch meta (imported_at, source, rates_hash)
      - INPUT SET: ordered list of transaction hashes that existed at run finished_at
      - OUTPUT SET: realized_events for this run with numeric strings for amounts and per-lot matches
    """
    with engine.begin() as conn:
        # NOTE: id is stored as TEXT/UUID in current schema
        run = conn.execute(
            text("SELECT * FROM calc_runs WHERE id = :rid"),
            {"rid": run_id},
        ).mappings().first()

        if not run:
            raise ValueError(f"calc_runs id={run_id} not found")

        fx = None
        if run["fx_set_id"] is not None:
            fx = conn.execute(
                text("SELECT * FROM fx_batches WHERE id = :bid"),
                {"bid": run["fx_set_id"]},
            ).mappings().first()

        finished_at = run["finished_at"]

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
                "qty_sold": str(r["qty_sold"]),
                "proceeds": str(r["proceeds"]),
                "cost_basis": str(r["cost_basis"]),
                "gain": str(r["gain"]),
                "quote_asset": r["quote_asset"],
                "fee_applied": str(r["fee_applied"]) if r["fee_applied"] is not None else None,
                "matches": json.loads(r["matches_json"] or "[]"),
            })

        manifest: Dict[str, Any] = {
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
                "id": fx["id"] if fx else None,
                "imported_at": fx["imported_at"] if fx else None,
                "source": fx["source"] if fx else None,
                "rates_hash": fx["rates_hash"] if fx else None,
            },
            "inputs": {
                "transactions_hashes_ordered": input_hashes,
            },
            "outputs": outputs,
        }
        return manifest

def compute_digests(manifest: Dict[str, Any]) -> Dict[str, str]:
    """
    Compute:
      - input_hash: hash over the input portion (params, fx metadata, input tx hashes)
      - output_hash: hash over realized events list
      - manifest_hash: hash over the full manifest
    """
    # split logically
    inputs_part = {
        "run_params": manifest["run"]["params"],
        "rule_version": manifest["run"]["rule_version"],
        "lot_method": manifest["run"]["lot_method"],
        "jurisdiction": manifest["run"]["jurisdiction"],
        "fx": manifest["fx_batch"],
        "input_hashes": manifest["inputs"]["transactions_hashes_ordered"],
    }
    outputs_part = manifest["outputs"]

    input_hash = _sha256_hex(_json_c14n(inputs_part))
    output_hash = _sha256_hex(_json_c14n(outputs_part))
    manifest_hash = _sha256_hex(_json_c14n(manifest))
    return {
        "input_hash": input_hash,
        "output_hash": output_hash,
        "manifest_hash": manifest_hash,
    }

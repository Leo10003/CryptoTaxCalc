from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from cryptotaxcalc.db import init_db

pytestmark = pytest.mark.smoke


CRITICAL_TABLE_COLUMNS = {
    "calc_runs": {
        "id",
        "started_at",
        "finished_at",
        "jurisdiction",
        "rule_version",
        "tax_year",
        "lot_method",
        "fx_set_id",
        "params_json",
        "run_id",
        "input_hash",
        "output_hash",
        "manifest_hash",
        "summary_json",
    },
    "realized_events": {
        "id",
        "run_id",
        "tx_id",
        "timestamp",
        "asset",
        "qty_sold",
        "proceeds",
        "cost_basis",
        "gain",
        "quote_asset",
        "fee_applied",
        "matches_json",
    },
    "run_digests": {
        "id",
        "run_id",
        "input_hash",
        "output_hash",
        "manifest_hash",
        "manifest_json",
        "created_at",
    },
    "run_inputs": {
        "run_id",
        "tx_hash",
    },
    "raw_events": {
        "id",
        "source_filename",
        "file_sha256",
        "mime_type",
        "importer",
        "received_at",
        "notes",
        "blob_path",
    },
}

CRITICAL_INDEXES = {
    "calc_runs": {
        "ix_calc_runs_started_at",
        "ux_calc_runs_run_id",
        "ix_calc_runs_juris_year",
    },
    "realized_events": {
        "ix_realized_events_run_id",
        "ix_realized_events_timestamp",
        "ix_realized_events_run_asset_ts",
        "ix_realized_events_run_asset_ts_id",
    },
    "run_digests": {
        "ix_run_digests_run_id",
        "ix_run_digests_created_at",
    },
    "run_inputs": {
        "ix_run_inputs_run",
        "ix_run_inputs_hash",
    },
}


def _engine(tmp_path: Path):
    db_path = tmp_path / "schema_migration_integrity.sqlite"
    return create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )


def _columns(conn, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(text(f"PRAGMA table_info('{table}')")).fetchall()}


def _indexes(conn, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(text(f"PRAGMA index_list('{table}')")).fetchall()}


def _rows(conn, sql: str) -> list[dict]:
    return [dict(row) for row in conn.execute(text(sql)).mappings().all()]


def test_fresh_init_db_creates_audit_export_critical_tables_columns_and_indexes(tmp_path: Path):
    engine = _engine(tmp_path)

    init_db(engine)
    init_db(engine)

    with engine.connect() as conn:
        for table, expected_columns in CRITICAL_TABLE_COLUMNS.items():
            actual_columns = _columns(conn, table)
            assert expected_columns <= actual_columns, (
                f"{table} is missing critical columns: {sorted(expected_columns - actual_columns)}"
            )

        for table, expected_indexes in CRITICAL_INDEXES.items():
            actual_indexes = _indexes(conn, table)
            assert expected_indexes <= actual_indexes, (
                f"{table} is missing critical indexes: {sorted(expected_indexes - actual_indexes)}; "
                f"actual={sorted(actual_indexes)}"
            )


def test_legacy_calc_runs_are_repaired_with_run_id_and_audit_digest_columns(tmp_path: Path):
    engine = _engine(tmp_path)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT,
                    finished_at TEXT,
                    jurisdiction TEXT,
                    rule_version TEXT,
                    lot_method TEXT,
                    fx_set_id INTEGER,
                    params_json TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO calc_runs (
                    started_at, finished_at, jurisdiction, rule_version, lot_method, fx_set_id, params_json
                )
                VALUES
                    ('2025-01-01T00:00:00', '2025-01-01T00:00:01', 'HR', '2025.1', 'FIFO', NULL, '{""jurisdiction"":""HR""}'),
                    ('2025-01-02T00:00:00', '2025-01-02T00:00:01', 'IT', '2025.1', 'FIFO', NULL, '{""jurisdiction"":""IT""}')
                """
            )
        )

    init_db(engine)

    with engine.connect() as conn:
        assert CRITICAL_TABLE_COLUMNS["calc_runs"] <= _columns(conn, "calc_runs")
        rows = _rows(conn, "SELECT id, run_id, tax_year, input_hash, output_hash, manifest_hash, summary_json FROM calc_runs ORDER BY id")
        indexes = _indexes(conn, "calc_runs")

    assert rows == [
        {
            "id": 1,
            "run_id": "legacy-1",
            "tax_year": None,
            "input_hash": None,
            "output_hash": None,
            "manifest_hash": None,
            "summary_json": None,
        },
        {
            "id": 2,
            "run_id": "legacy-2",
            "tax_year": None,
            "input_hash": None,
            "output_hash": None,
            "manifest_hash": None,
            "summary_json": None,
        },
    ]
    assert "ux_calc_runs_run_id" in indexes


def test_legacy_duplicate_calc_run_external_ids_are_made_unique_before_unique_index(tmp_path: Path):
    engine = _engine(tmp_path)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE calc_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT,
                    finished_at TEXT,
                    jurisdiction TEXT,
                    rule_version TEXT,
                    tax_year INTEGER,
                    lot_method TEXT,
                    fx_set_id INTEGER,
                    params_json TEXT,
                    run_id TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO calc_runs (run_id, started_at, jurisdiction)
                VALUES ('duplicate-run', '2025-01-01T00:00:00', 'HR'),
                       ('duplicate-run', '2025-01-02T00:00:00', 'HR'),
                       ('', '2025-01-03T00:00:00', 'HR')
                """
            )
        )

    init_db(engine)

    with engine.connect() as conn:
        rows = _rows(conn, "SELECT id, run_id FROM calc_runs ORDER BY id")
        duplicate_count = conn.execute(text("SELECT COUNT(*) FROM calc_runs GROUP BY run_id HAVING COUNT(*) > 1")).fetchone()
        indexes = _indexes(conn, "calc_runs")

    run_ids = [row["run_id"] for row in rows]
    assert run_ids[0] == "duplicate-run"
    assert run_ids[1].startswith("duplicate-run-")
    assert run_ids[2] == "legacy-3"
    assert duplicate_count is None
    assert "ux_calc_runs_run_id" in indexes


def test_legacy_run_digests_without_synthetic_id_are_rebuilt_and_preserved(tmp_path: Path):
    engine = _engine(tmp_path)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE run_digests (
                    run_id INTEGER PRIMARY KEY,
                    input_hash TEXT,
                    output_hash TEXT,
                    manifest_hash TEXT,
                    manifest_json TEXT,
                    created_at TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO run_digests (run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at)
                VALUES (42, 'input-hash', 'output-hash', 'manifest-hash', '{"ok": true}', '2026-01-01T00:00:00')
                """
            )
        )

    init_db(engine)

    with engine.connect() as conn:
        assert CRITICAL_TABLE_COLUMNS["run_digests"] <= _columns(conn, "run_digests")
        rows = _rows(
            conn,
            """
            SELECT id, run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at
            FROM run_digests
            ORDER BY run_id
            """,
        )
        indexes = _indexes(conn, "run_digests")

    assert len(rows) == 1
    assert rows[0]["id"] is not None
    assert rows[0]["run_id"] == 42
    assert rows[0]["input_hash"] == "input-hash"
    assert rows[0]["output_hash"] == "output-hash"
    assert rows[0]["manifest_hash"] == "manifest-hash"
    assert rows[0]["manifest_json"] == '{"ok": true}'
    assert rows[0]["created_at"] == "2026-01-01T00:00:00"
    assert "ix_run_digests_run_id" in indexes
    assert "ix_run_digests_created_at" in indexes


def test_init_db_repairs_legacy_fx_tables_without_losing_rates(tmp_path: Path):
    engine = _engine(tmp_path)

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE fx_rates (date TEXT, usd_per_eur TEXT)"))
        conn.execute(text("INSERT INTO fx_rates (date, usd_per_eur) VALUES ('2025-01-01', '1.25')"))

    init_db(engine)

    with engine.connect() as conn:
        fx_rate_columns = _columns(conn, "fx_rates")
        fx_batch_columns = _columns(conn, "fx_batches")
        rows = _rows(conn, "SELECT date, base, quote, rate, batch_id FROM fx_rates ORDER BY date")
        fx_batches = _rows(conn, "SELECT id, date, imported_at, source FROM fx_batches ORDER BY id")
        indexes = _indexes(conn, "fx_rates")

    assert {"id", "date", "base", "quote", "rate", "batch_id"} <= fx_rate_columns
    assert {"id", "date", "created_at", "imported_at", "source", "rates_hash"} <= fx_batch_columns
    assert rows == [
        {
            "date": "2025-01-01",
            "base": "USD",
            "quote": "EUR",
            "rate": "0.80000000",
            "batch_id": 1,
        }
    ]
    assert fx_batches[0]["source"] == "legacy-bootstrap"
    assert "uq_fx_rates_date_pair" in indexes
    assert "ix_fx_rates_batch_date" in indexes
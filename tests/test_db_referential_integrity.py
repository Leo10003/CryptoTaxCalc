from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from cryptotaxcalc.db import _set_sqlite_pragmas
from cryptotaxcalc.models import Base

pytestmark = pytest.mark.smoke


def _isolated_schema(tmp_path: Path):
    db_path = tmp_path / "referential_integrity.sqlite"
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection, connection_record):
        _set_sqlite_pragmas(dbapi_connection)

    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return engine, SessionLocal


def _insert_calc_run(session, run_id: str = "ri-run") -> int:
    result = session.execute(
        text(
            """
            INSERT INTO calc_runs (started_at, jurisdiction, rule_version, tax_year, lot_method, params_json, run_id)
            VALUES (:started_at, 'HR', '2025.1', 2025, 'FIFO', '{}', :run_id)
            """
        ),
        {"started_at": datetime(2025, 1, 1, tzinfo=timezone.utc), "run_id": run_id},
    )
    return int(result.lastrowid)


def _insert_transaction(session, *, hash_value: str | None = None, raw_event_id: int | None = None) -> int:
    result = session.execute(
        text(
            """
            INSERT INTO transactions (
                hash, timestamp, type, base_asset, base_amount, quote_asset, quote_amount,
                fee_asset, fee_amount, exchange, memo, fair_value, raw_event_id, created_at
            )
            VALUES (
                :hash, :timestamp, 'buy', 'BTC', '1', 'EUR', '100',
                'EUR', '0', 'Integrity', 'fixture row', NULL, :raw_event_id, :created_at
            )
            """
        ),
        {
            "hash": hash_value,
            "timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "raw_event_id": raw_event_id,
            "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
        },
    )
    return int(result.lastrowid)


def _insert_realized_event(session, *, run_id: int, tx_id: int | None = None) -> int:
    result = session.execute(
        text(
            """
            INSERT INTO realized_events (
                run_id, tx_id, timestamp, asset, qty_sold, proceeds, cost_basis,
                gain, quote_asset, fee_applied, matches_json
            )
            VALUES (:run_id, :tx_id, '2025-02-01T00:00:00+00:00', 'BTC', '1', '200', '100', '100', 'EUR', '0', '[]')
            """
        ),
        {"run_id": run_id, "tx_id": tx_id},
    )
    return int(result.lastrowid)


def _count(session, table: str) -> int:
    return int(session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0)


def test_new_sqlite_schema_declares_required_foreign_keys_and_unique_constraints(tmp_path: Path):
    engine, SessionLocal = _isolated_schema(tmp_path)
    try:
        with engine.connect() as conn:
            realized_fks = conn.execute(text("PRAGMA foreign_key_list('realized_events')")).mappings().all()
            digest_fks = conn.execute(text("PRAGMA foreign_key_list('run_digests')")).mappings().all()
            input_fks = conn.execute(text("PRAGMA foreign_key_list('run_inputs')")).mappings().all()
            wallet_fks = conn.execute(text("PRAGMA foreign_key_list('wallet_out_overrides')")).mappings().all()
            transaction_fks = conn.execute(text("PRAGMA foreign_key_list('transactions')")).mappings().all()
            transaction_indexes = conn.execute(text("PRAGMA index_list('transactions')")).mappings().all()
            wallet_indexes = conn.execute(text("PRAGMA index_list('wallet_out_overrides')")).mappings().all()
            digest_indexes = conn.execute(text("PRAGMA index_list('run_digests')")).mappings().all()

        assert {row["table"] for row in realized_fks} == {"calc_runs", "transactions"}
        assert {row["from"]: row["on_delete"] for row in realized_fks} == {
            "run_id": "CASCADE",
            "tx_id": "SET NULL",
        }
        assert digest_fks[0]["table"] == "calc_runs"
        assert digest_fks[0]["on_delete"] == "CASCADE"
        assert input_fks[0]["table"] == "calc_runs"
        assert input_fks[0]["on_delete"] == "CASCADE"
        assert wallet_fks[0]["table"] == "transactions"
        assert wallet_fks[0]["on_delete"] == "CASCADE"
        assert transaction_fks[0]["table"] == "raw_events"
        assert transaction_fks[0]["on_delete"] == "SET NULL"
        assert any(row["unique"] for row in transaction_indexes)
        assert any(row["unique"] for row in wallet_indexes)
        assert any(row["unique"] for row in digest_indexes)
    finally:
        engine.dispose()


def test_wallet_transfer_override_requires_existing_transaction_and_cascades_on_transaction_delete(tmp_path: Path):
    engine, SessionLocal = _isolated_schema(tmp_path)
    try:
        with SessionLocal() as session:
            with pytest.raises(IntegrityError):
                session.execute(
                    text(
                        """
                        INSERT INTO wallet_out_overrides (transaction_id, classification, proceeds_eur, note, created_at)
                        VALUES (999999, 'sell', '100', 'orphan override', :created_at)
                        """
                    ),
                    {"created_at": datetime(2025, 1, 1, tzinfo=timezone.utc)},
                )
                session.commit()
            session.rollback()

            tx_id = _insert_transaction(session, hash_value="wallet-integrity-hash")
            session.execute(
                text(
                    """
                    INSERT INTO wallet_out_overrides (transaction_id, classification, proceeds_eur, note, created_at)
                    VALUES (:tx_id, 'sell', '100', 'valid override', :created_at)
                    """
                ),
                {"tx_id": tx_id, "created_at": datetime(2025, 1, 1, tzinfo=timezone.utc)},
            )
            session.commit()
            assert _count(session, "wallet_out_overrides") == 1

            session.execute(text("DELETE FROM transactions WHERE id = :tx_id"), {"tx_id": tx_id})
            session.commit()
            assert _count(session, "transactions") == 0
            assert _count(session, "wallet_out_overrides") == 0
    finally:
        engine.dispose()


def test_run_artifacts_require_existing_calc_run_and_cascade_when_run_is_deleted(tmp_path: Path):
    engine, SessionLocal = _isolated_schema(tmp_path)
    try:
        with SessionLocal() as session:
            with pytest.raises(IntegrityError):
                _insert_realized_event(session, run_id=123456)
                session.commit()
            session.rollback()

            with pytest.raises(IntegrityError):
                session.execute(
                    text(
                        """
                        INSERT INTO run_digests (run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at)
                        VALUES (123456, 'i', 'o', 'm', '{}', '2025-01-01T00:00:00Z')
                        """
                    )
                )
                session.commit()
            session.rollback()

            with pytest.raises(IntegrityError):
                session.execute(text("INSERT INTO run_inputs (run_id, tx_hash) VALUES (123456, 'missing-run-input')"))
                session.commit()
            session.rollback()

            run_id = _insert_calc_run(session, run_id="ri-cascade-run")
            tx_id = _insert_transaction(session, hash_value="ri-cascade-tx")
            _insert_realized_event(session, run_id=run_id, tx_id=tx_id)
            session.execute(
                text(
                    """
                    INSERT INTO run_digests (run_id, input_hash, output_hash, manifest_hash, manifest_json, created_at)
                    VALUES (:run_id, 'i', 'o', 'm', '{}', '2025-01-01T00:00:00Z')
                    """
                ),
                {"run_id": run_id},
            )
            session.execute(text("INSERT INTO run_inputs (run_id, tx_hash) VALUES (:run_id, 'ri-cascade-tx')"), {"run_id": run_id})
            session.commit()
            assert _count(session, "realized_events") == 1
            assert _count(session, "run_digests") == 1
            assert _count(session, "run_inputs") == 1

            session.execute(text("DELETE FROM calc_runs WHERE id = :run_id"), {"run_id": run_id})
            session.commit()
            assert _count(session, "calc_runs") == 0
            assert _count(session, "realized_events") == 0
            assert _count(session, "run_digests") == 0
            assert _count(session, "run_inputs") == 0
            assert _count(session, "transactions") == 1
    finally:
        engine.dispose()


def test_transaction_hash_is_unique_for_non_null_values_but_multiple_null_hashes_remain_allowed(tmp_path: Path):
    engine, SessionLocal = _isolated_schema(tmp_path)
    try:
        with SessionLocal() as session:
            _insert_transaction(session, hash_value=None)
            _insert_transaction(session, hash_value=None)
            _insert_transaction(session, hash_value="duplicate-protected-hash")
            session.commit()
            assert _count(session, "transactions") == 3

            with pytest.raises(IntegrityError):
                _insert_transaction(session, hash_value="duplicate-protected-hash")
                session.commit()
            session.rollback()

            assert _count(session, "transactions") == 3
    finally:
        engine.dispose()


def test_raw_event_delete_sets_transaction_raw_event_id_to_null_instead_of_orphaning_history(tmp_path: Path):
    engine, SessionLocal = _isolated_schema(tmp_path)
    try:
        with SessionLocal() as session:
            raw_id = int(
                session.execute(
                    text(
                        """
                        INSERT INTO raw_events (source_filename, file_sha256, mime_type, importer, received_at, notes, blob_path)
                        VALUES ('source.csv', 'sha', 'text/csv', 'test', '2025-01-01T00:00:00Z', NULL, 'source.csv')
                        """
                    )
                ).lastrowid
            )
            tx_id = _insert_transaction(session, hash_value="raw-event-set-null-hash", raw_event_id=raw_id)
            session.commit()

            session.execute(text("DELETE FROM raw_events WHERE id = :raw_id"), {"raw_id": raw_id})
            session.commit()

            raw_event_id = session.execute(
                text("SELECT raw_event_id FROM transactions WHERE id = :tx_id"),
                {"tx_id": tx_id},
            ).scalar_one()
            assert raw_event_id is None
            assert _count(session, "transactions") == 1
            assert _count(session, "raw_events") == 0
    finally:
        engine.dispose()
from __future__ import annotations

import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import cryptotaxcalc.app as app_module
import cryptotaxcalc.audit_utils as audit_utils
from cryptotaxcalc.app import app
from cryptotaxcalc.db import init_db
from cryptotaxcalc.models import Base, Transaction as DbTransaction, TxType, WalletOutOverride

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _isolated_import_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    db_path = tmp_path / "import_idempotency.sqlite"
    temp_engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
    init_db(temp_engine)
    Base.metadata.create_all(bind=temp_engine)
    TempSessionLocal = sessionmaker(bind=temp_engine, autoflush=False, autocommit=False)

    monkeypatch.setattr(app_module, "engine", temp_engine)
    monkeypatch.setattr(app_module, "SessionLocal", TempSessionLocal)
    monkeypatch.setattr(audit_utils, "engine", temp_engine)
    monkeypatch.setitem(app_module.audit.__globals__, "engine", temp_engine)
    return temp_engine, TempSessionLocal


def _csv(*, memo_tag: str, asset: str = "BTC") -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},1,EUR,1000,EUR,0,ImportIdempotency,{memo_tag} buy
2025-02-01T00:00:00Z,sell,{asset},0.25,EUR,400,EUR,0,ImportIdempotency,{memo_tag} sell
"""


def _post_import_multiple(files: list[tuple[str, str]], *, reset: bool = False):
    multipart = [
        (
            "files",
            (filename, content.encode("utf-8"), "text/csv"),
        )
        for filename, content in files
    ]
    return client.post(
        "/import/multiple",
        params={"reset": "true" if reset else "false"},
        files=multipart,
    )


def _scalar(SessionLocal, sql: str, params: dict | None = None) -> int:
    with SessionLocal() as db:
        return int(db.execute(text(sql), params or {}).scalar() or 0)


def _rows(SessionLocal, sql: str, params: dict | None = None) -> list[dict]:
    with SessionLocal() as db:
        return [dict(row) for row in db.execute(text(sql), params or {}).mappings().all()]


def test_reimporting_same_file_is_idempotent_for_transactions_but_preserves_raw_upload_history(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_import_db(monkeypatch, tmp_path)
    memo_tag = f"idem-repeat-{uuid.uuid4().hex}"
    filename = "same_file.csv"
    csv_text = _csv(memo_tag=memo_tag, asset="IDEMREP")

    first = _post_import_multiple([(filename, csv_text)], reset=False)
    second = _post_import_multiple([(filename, csv_text)], reset=False)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    first_result = first.json()["results"][0]
    second_result = second.json()["results"][0]

    assert first_result["inserted"] == 2
    assert first_result["skipped_duplicates"] == 0
    assert first_result["skipped_errors"] == 0
    assert second_result["inserted"] == 0
    assert second_result["skipped_duplicates"] == 2
    assert second_result["skipped_errors"] == 0

    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions WHERE memo LIKE :memo", {"memo": f"%{memo_tag}%"}) == 2
    assert _scalar(SessionLocal, "SELECT COUNT(DISTINCT hash) FROM transactions WHERE memo LIKE :memo", {"memo": f"%{memo_tag}%"}) == 2
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events WHERE source_filename = :filename", {"filename": filename}) == 2

    raw_links = _rows(
        SessionLocal,
        """
        SELECT raw_event_id, COUNT(*) AS count
        FROM transactions
        WHERE memo LIKE :memo
        GROUP BY raw_event_id
        """,
        {"memo": f"%{memo_tag}%"},
    )
    assert len(raw_links) == 1
    assert raw_links[0]["count"] == 2


def test_duplicate_content_in_same_batch_inserts_once_and_reports_second_file_as_duplicates(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_import_db(monkeypatch, tmp_path)
    memo_tag = f"idem-batch-{uuid.uuid4().hex}"
    csv_text = _csv(memo_tag=memo_tag, asset="IDEMBAT")
    first_filename = "duplicate_a.csv"
    second_filename = "duplicate_b.csv"

    response = _post_import_multiple(
        [
            (first_filename, csv_text),
            (second_filename, csv_text),
        ],
        reset=False,
    )

    assert response.status_code == 200, response.text
    results = response.json()["results"]
    assert [item["filename"] for item in results] == [first_filename, second_filename]
    assert results[0]["inserted"] == 2
    assert results[0]["skipped_duplicates"] == 0
    assert results[1]["inserted"] == 0
    assert results[1]["skipped_duplicates"] == 2
    assert results[1]["skipped_errors"] == 0

    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions WHERE memo LIKE :memo", {"memo": f"%{memo_tag}%"}) == 2
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events") == 2
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events WHERE source_filename = :filename", {"filename": first_filename}) == 1
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events WHERE source_filename = :filename", {"filename": second_filename}) == 1


def test_reset_true_replaces_transactions_clears_wallet_overrides_and_preserves_raw_event_history(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_import_db(monkeypatch, tmp_path)
    old_memo_tag = f"idem-old-{uuid.uuid4().hex}"
    new_memo_tag = f"idem-new-{uuid.uuid4().hex}"

    with SessionLocal() as db:
        db.execute(
            text(
                """
                INSERT INTO raw_events (source_filename, file_sha256, mime_type, importer, received_at, notes, blob_path)
                VALUES ('old_upload.csv', 'oldhash', 'text/csv', 'test', '2025-01-01T00:00:00Z', NULL, 'old_upload.csv')
                """
            )
        )
        old_raw_event_id = int(db.execute(text("SELECT id FROM raw_events WHERE source_filename = 'old_upload.csv'")).scalar())
        old_tx = DbTransaction(
            timestamp=app_module._dt.fromisoformat("2025-01-01T00:00:00+00:00"),
            type=TxType.TRANSFER_OUT,
            base_asset="IDEMOLD",
            base_amount=Decimal("-1"),
            quote_asset=None,
            quote_amount=None,
            fee_asset="IDEMOLD",
            fee_amount=Decimal("0.01"),
            memo=f"{old_memo_tag} old transfer",
            raw_event_id=old_raw_event_id,
        )
        db.add(old_tx)
        db.flush()
        db.add(WalletOutOverride(transaction_id=old_tx.id, classification="sell", proceeds_eur=Decimal("123")))
        db.commit()

    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions") == 1
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM wallet_out_overrides") == 1
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events") == 1

    response = _post_import_multiple(
        [("replacement.csv", _csv(memo_tag=new_memo_tag, asset="IDEMNEW"))],
        reset=True,
    )

    assert response.status_code == 200, response.text
    result = response.json()["results"][0]
    assert result["inserted"] == 2
    assert result["skipped_duplicates"] == 0
    assert result["skipped_errors"] == 0

    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions WHERE memo LIKE :memo", {"memo": f"%{old_memo_tag}%"}) == 0
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions WHERE memo LIKE :memo", {"memo": f"%{new_memo_tag}%"}) == 2
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM wallet_out_overrides") == 0
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events WHERE source_filename = 'old_upload.csv'") == 1
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events WHERE source_filename = 'replacement.csv'") == 1
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events") == 2


def test_same_economic_rows_with_different_memo_are_not_collapsed_as_duplicates(monkeypatch, tmp_path):
    _, SessionLocal = _isolated_import_db(monkeypatch, tmp_path)
    first_tag = f"idem-memo-a-{uuid.uuid4().hex}"
    second_tag = f"idem-memo-b-{uuid.uuid4().hex}"

    first = _post_import_multiple([("memo_a.csv", _csv(memo_tag=first_tag, asset="IDEMMEMO"))], reset=False)
    second = _post_import_multiple([("memo_b.csv", _csv(memo_tag=second_tag, asset="IDEMMEMO"))], reset=False)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["results"][0]["inserted"] == 2
    assert second.json()["results"][0]["inserted"] == 2
    assert second.json()["results"][0]["skipped_duplicates"] == 0

    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM transactions WHERE base_asset = 'IDEMMEMO'") == 4
    assert _scalar(SessionLocal, "SELECT COUNT(DISTINCT hash) FROM transactions WHERE base_asset = 'IDEMMEMO'") == 4
    assert _scalar(SessionLocal, "SELECT COUNT(*) FROM raw_events") == 2
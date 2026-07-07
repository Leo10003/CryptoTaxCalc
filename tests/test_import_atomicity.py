from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from cryptotaxcalc.app import app
from cryptotaxcalc.db import SessionLocal, engine, init_db
from cryptotaxcalc.models import Base

pytestmark = pytest.mark.smoke

client = TestClient(app)


def _ensure_schema() -> None:
    init_db(engine)
    Base.metadata.create_all(bind=engine)


def _valid_csv(*, memo_tag: str, asset: str = "BTC") -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
2025-01-01T00:00:00Z,buy,{asset},1,EUR,1000,EUR,0,Atomicity,{memo_tag} buy
2025-02-01T00:00:00Z,sell,{asset},0.25,EUR,400,EUR,0,Atomicity,{memo_tag} sell
"""


def _malformed_csv(*, memo_tag: str, asset: str = "ETH") -> str:
    return f"""timestamp,type,base_asset,base_amount,quote_asset,quote_amount,fee_asset,fee_amount,exchange,memo
not-a-date,buy,{asset},1,EUR,1000,EUR,0,Atomicity,{memo_tag} invalid timestamp
"""


def _unsupported_csv(*, memo_tag: str) -> str:
    return f"""Trade Time,Coin,Operation,Units,Total Value,Comment
2025-01-01,BTC,Acquire,1,10000,{memo_tag} unsupported source row
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


def _count_transactions_by_memo_fragment(fragment: str) -> int:
    with SessionLocal() as db:
        return int(
            db.execute(
                text("""
                    SELECT COUNT(*)
                    FROM transactions
                    WHERE memo LIKE :memo_fragment
                """),
                {"memo_fragment": f"%{fragment}%"},
            ).scalar()
            or 0
        )


def _delete_transactions_by_memo_fragment(fragment: str) -> int:
    with SessionLocal() as db:
        result = db.execute(
            text("""
                DELETE FROM transactions
                WHERE memo LIKE :memo_fragment
            """),
            {"memo_fragment": f"%{fragment}%"},
        )
        db.commit()
        return int(result.rowcount or 0)


def _count_raw_events_by_filename(filename: str) -> int:
    with SessionLocal() as db:
        return int(
            db.execute(
                text("""
                    SELECT COUNT(*)
                    FROM raw_events
                    WHERE source_filename = :filename
                """),
                {"filename": filename},
            ).scalar()
            or 0
        )


def _response_reports_errors(payload: dict) -> bool:
    results = payload.get("results")
    if not isinstance(results, list):
        return False
    return any(
        isinstance(item, dict)
        and (
            int(item.get("skipped_errors") or 0) > 0
            or bool(item.get("errors"))
            or bool(item.get("error"))
        )
        for item in results
    )


def test_mixed_valid_and_malformed_batch_writes_no_transactions_or_raw_events():
    _ensure_schema()
    good_tag = f"atomic-good-{uuid.uuid4().hex}"
    bad_tag = f"atomic-bad-{uuid.uuid4().hex}"
    good_filename = f"atomic_good_{uuid.uuid4().hex}.csv"
    bad_filename = f"atomic_bad_{uuid.uuid4().hex}.csv"

    try:
        before_good_tx = _count_transactions_by_memo_fragment(good_tag)
        before_bad_tx = _count_transactions_by_memo_fragment(bad_tag)
        before_good_raw = _count_raw_events_by_filename(good_filename)
        before_bad_raw = _count_raw_events_by_filename(bad_filename)

        response = _post_import_multiple(
            [
                (good_filename, _valid_csv(memo_tag=good_tag)),
                (bad_filename, _malformed_csv(memo_tag=bad_tag)),
            ],
            reset=False,
        )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert _response_reports_errors(payload), payload
        assert len(payload["results"]) == 2

        assert _count_transactions_by_memo_fragment(good_tag) == before_good_tx
        assert _count_transactions_by_memo_fragment(bad_tag) == before_bad_tx
        assert _count_raw_events_by_filename(good_filename) == before_good_raw
        assert _count_raw_events_by_filename(bad_filename) == before_bad_raw
    finally:
        _delete_transactions_by_memo_fragment(good_tag)
        _delete_transactions_by_memo_fragment(bad_tag)


def test_reset_true_malformed_batch_preserves_existing_transactions_and_writes_no_new_raw_events():
    _ensure_schema()
    existing_tag = f"atomic-existing-{uuid.uuid4().hex}"
    bad_tag = f"atomic-reset-bad-{uuid.uuid4().hex}"
    setup_filename = f"atomic_setup_{uuid.uuid4().hex}.csv"
    bad_filename = f"atomic_reset_bad_{uuid.uuid4().hex}.csv"

    try:
        setup_response = _post_import_multiple(
            [(setup_filename, _valid_csv(memo_tag=existing_tag, asset="SOL"))],
            reset=False,
        )
        assert setup_response.status_code == 200, setup_response.text
        assert _count_transactions_by_memo_fragment(existing_tag) == 2

        before_existing_tx = _count_transactions_by_memo_fragment(existing_tag)
        before_bad_tx = _count_transactions_by_memo_fragment(bad_tag)
        before_bad_raw = _count_raw_events_by_filename(bad_filename)

        response = _post_import_multiple(
            [(bad_filename, _malformed_csv(memo_tag=bad_tag, asset="ADA"))],
            reset=True,
        )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert _response_reports_errors(payload), payload

        assert _count_transactions_by_memo_fragment(existing_tag) == before_existing_tx
        assert _count_transactions_by_memo_fragment(bad_tag) == before_bad_tx
        assert _count_raw_events_by_filename(bad_filename) == before_bad_raw
    finally:
        _delete_transactions_by_memo_fragment(existing_tag)
        _delete_transactions_by_memo_fragment(bad_tag)


def test_unsupported_source_in_batch_preflight_blocks_all_persistence_and_records_no_raw_event():
    _ensure_schema()
    good_tag = f"atomic-supported-{uuid.uuid4().hex}"
    unsupported_tag = f"atomic-unsupported-{uuid.uuid4().hex}"
    good_filename = f"atomic_supported_{uuid.uuid4().hex}.csv"
    unsupported_filename = f"atomic_unsupported_{uuid.uuid4().hex}.csv"

    try:
        response = _post_import_multiple(
            [
                (good_filename, _valid_csv(memo_tag=good_tag, asset="ETH")),
                (unsupported_filename, _unsupported_csv(memo_tag=unsupported_tag)),
            ],
            reset=False,
        )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert _response_reports_errors(payload), payload
        assert any(
            item.get("recognized_source_status") == "unsupported"
            for item in payload["results"]
            if isinstance(item, dict)
        )

        assert _count_transactions_by_memo_fragment(good_tag) == 0
        assert _count_transactions_by_memo_fragment(unsupported_tag) == 0
        assert _count_raw_events_by_filename(good_filename) == 0
        assert _count_raw_events_by_filename(unsupported_filename) == 0
    finally:
        _delete_transactions_by_memo_fragment(good_tag)
        _delete_transactions_by_memo_fragment(unsupported_tag)


def test_mixed_valid_and_non_csv_filename_batch_writes_no_transactions_or_raw_events():
    _ensure_schema()
    good_tag = f"atomic-valid-ext-{uuid.uuid4().hex}"
    bad_tag = f"atomic-noncsv-{uuid.uuid4().hex}"
    good_filename = f"atomic_valid_ext_{uuid.uuid4().hex}.csv"
    bad_filename = f"atomic_bad_ext_{uuid.uuid4().hex}.txt"

    try:
        response = _post_import_multiple(
            [
                (good_filename, _valid_csv(memo_tag=good_tag, asset="XRP")),
                (bad_filename, _valid_csv(memo_tag=bad_tag, asset="DOGE")),
            ],
            reset=False,
        )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert _response_reports_errors(payload), payload
        assert any(
            item.get("filename") == bad_filename
            and item.get("errors") == ["Only .csv files are supported"]
            for item in payload["results"]
            if isinstance(item, dict)
        )

        assert _count_transactions_by_memo_fragment(good_tag) == 0
        assert _count_transactions_by_memo_fragment(bad_tag) == 0
        assert _count_raw_events_by_filename(good_filename) == 0
        assert _count_raw_events_by_filename(bad_filename) == 0
    finally:
        _delete_transactions_by_memo_fragment(good_tag)
        _delete_transactions_by_memo_fragment(bad_tag)
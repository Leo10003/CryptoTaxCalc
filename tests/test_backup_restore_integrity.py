from __future__ import annotations

import asyncio
import hashlib
import io
import sqlite3
from pathlib import Path

import pytest
from fastapi import HTTPException, UploadFile
from sqlalchemy import create_engine

import cryptotaxcalc.routes.ops_admin as ops_admin

pytestmark = pytest.mark.smoke


def _write_sqlite_file(path: Path, rows: list[tuple[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE marker (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        conn.executemany("INSERT INTO marker (k, v) VALUES (?, ?)", rows)
        conn.commit()
    finally:
        conn.close()
    return path


def _read_marker(path: Path) -> dict[str, str]:
    conn = sqlite3.connect(str(path))
    try:
        return dict(conn.execute("SELECT k, v FROM marker ORDER BY k").fetchall())
    finally:
        conn.close()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _upload_file(path: Path, filename: str = "restore.sqlite") -> UploadFile:
    return UploadFile(filename=filename, file=io.BytesIO(path.read_bytes()))


def _isolated_ops_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    db_path = project_root / "cryptotaxcalc.sqlite"
    _write_sqlite_file(db_path, [("state", "before")])
    engine = create_engine(f"sqlite:///{db_path}")

    monkeypatch.setattr(ops_admin, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(ops_admin, "engine", engine)
    monkeypatch.setattr(ops_admin, "IS_PROD", False)
    return project_root, db_path, engine


def test_export_database_creates_project_scoped_sqlite_backup_with_same_contents(monkeypatch, tmp_path):
    project_root, db_path, engine = _isolated_ops_db(monkeypatch, tmp_path)
    try:
        response = ops_admin.export_database(_admin=None)

        backup_path = Path(response.path)
        assert backup_path.parent == project_root / "backups"
        assert backup_path.name.startswith("db_backup_")
        assert backup_path.suffix == ".sqlite"
        assert backup_path.exists()
        assert backup_path.read_bytes().startswith(b"SQLite format 3\x00")
        assert _sha256(backup_path) == _sha256(db_path)
        assert _read_marker(backup_path) == {"state": "before"}
        assert response.filename == backup_path.name
        assert response.media_type == "application/x-sqlite3"
    finally:
        engine.dispose()


def test_import_database_requires_explicit_confirmation_and_does_not_mutate_current_db(monkeypatch, tmp_path):
    _, db_path, engine = _isolated_ops_db(monkeypatch, tmp_path)
    replacement = _write_sqlite_file(tmp_path / "replacement.sqlite", [("state", "after")])
    before_hash = _sha256(db_path)
    try:
        with pytest.raises(HTTPException) as raised:
            asyncio.run(ops_admin.import_database(file=_upload_file(replacement), confirm="", _admin=None))

        assert raised.value.status_code == 400
        assert "Confirmation missing" in raised.value.detail
        assert _sha256(db_path) == before_hash
        assert _read_marker(db_path) == {"state": "before"}
    finally:
        engine.dispose()


def test_import_database_restores_valid_sqlite_file_creates_pre_restore_backup_and_preserves_integrity(monkeypatch, tmp_path):
    project_root, db_path, engine = _isolated_ops_db(monkeypatch, tmp_path)
    replacement = _write_sqlite_file(
        tmp_path / "replacement.sqlite",
        [("state", "after"), ("audit", "restored")],
    )
    original_hash = _sha256(db_path)
    try:
        response = asyncio.run(
            ops_admin.import_database(
                file=_upload_file(replacement, filename="replacement.sqlite"),
                confirm="I_UNDERSTAND",
                _admin=None,
            )
        )

        body = response.body.decode("utf-8")
        assert response.status_code == 200
        assert "Database restored successfully" in body
        assert _read_marker(db_path) == {"audit": "restored", "state": "after"}
        assert ops_admin._integrity_ok(str(db_path)) is True

        backups = sorted((project_root / "backups").glob("data_before_restore_*.db"))
        assert len(backups) == 1
        assert backups[0].read_bytes().startswith(b"SQLite format 3\x00")
        assert _sha256(backups[0]) == original_hash
        assert _read_marker(backups[0]) == {"state": "before"}
    finally:
        engine.dispose()


def test_import_database_rejects_non_sqlite_upload_without_mutating_current_db(monkeypatch, tmp_path):
    _, db_path, engine = _isolated_ops_db(monkeypatch, tmp_path)
    bad = tmp_path / "not_sqlite.db"
    bad.write_bytes(b"not a sqlite database")
    before_hash = _sha256(db_path)
    try:
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                ops_admin.import_database(
                    file=_upload_file(bad, filename="not_sqlite.db"),
                    confirm="I_UNDERSTAND",
                    _admin=None,
                )
            )

        assert raised.value.status_code == 400
        assert raised.value.detail == "Uploaded file is not a valid SQLite database."
        assert _sha256(db_path) == before_hash
        assert _read_marker(db_path) == {"state": "before"}
    finally:
        engine.dispose()


def test_import_database_rolls_back_original_db_when_restored_copy_fails_integrity_check(monkeypatch, tmp_path):
    project_root, db_path, engine = _isolated_ops_db(monkeypatch, tmp_path)
    replacement = _write_sqlite_file(tmp_path / "replacement.sqlite", [("state", "after")])
    original_hash = _sha256(db_path)
    real_integrity_ok = ops_admin._integrity_ok

    def integrity_ok_once_for_upload_then_fail_restored(path: str) -> bool:
        if Path(path) == replacement:
            return True
        if Path(path) == db_path:
            return False
        return real_integrity_ok(path)

    monkeypatch.setattr(ops_admin, "_integrity_ok", integrity_ok_once_for_upload_then_fail_restored)
    try:
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                ops_admin.import_database(
                    file=_upload_file(replacement, filename="replacement.sqlite"),
                    confirm="I_UNDERSTAND",
                    _admin=None,
                )
            )

        assert raised.value.status_code == 500
        assert raised.value.detail == "Restored DB failed integrity_check; original DB has been restored."
        assert _sha256(db_path) == original_hash
        assert _read_marker(db_path) == {"state": "before"}
        backups = sorted((project_root / "backups").glob("data_before_restore_*.db"))
        assert len(backups) == 1
        assert _sha256(backups[0]) == original_hash
    finally:
        engine.dispose()
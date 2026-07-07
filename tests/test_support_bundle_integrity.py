from __future__ import annotations

import importlib.util
import json
import sqlite3
import zipfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SUPPORT_BUNDLE_SCRIPT = PROJECT_ROOT / "automation" / "collect_support_bundle.py"
_spec = importlib.util.spec_from_file_location("collect_support_bundle_for_tests", SUPPORT_BUNDLE_SCRIPT)
assert _spec is not None and _spec.loader is not None
collect_support_bundle = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(collect_support_bundle)

StateLog = collect_support_bundle.StateLog
build_manifest = collect_support_bundle.build_manifest
build_env_config_snapshot = collect_support_bundle.build_env_config_snapshot
db_diag = collect_support_bundle.db_diag
preflight = collect_support_bundle.preflight
redact_dotenv_text = collect_support_bundle.redact_dotenv_text
write_runtime_snapshot = collect_support_bundle.write_runtime_snapshot
zip_bundle = collect_support_bundle.zip_bundle

pytestmark = pytest.mark.smoke


def _create_diag_db(project_root: Path) -> None:
    db_path = project_root / "cryptotaxcalc.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(
            """
            CREATE TABLE calc_runs (id INTEGER PRIMARY KEY, jurisdiction TEXT, tax_year INTEGER);
            CREATE TABLE transactions (id INTEGER PRIMARY KEY, timestamp TEXT, base_asset TEXT);
            CREATE TABLE fx_rates (id INTEGER PRIMARY KEY, date TEXT, base TEXT, quote TEXT, rate TEXT, batch_id INTEGER);
            CREATE TABLE fx_batches (id INTEGER PRIMARY KEY, date TEXT, imported_at TEXT, source TEXT, rates_hash TEXT);
            CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT);
            CREATE TABLE realized_events (id INTEGER PRIMARY KEY, run_id INTEGER, asset TEXT);
            CREATE TABLE run_digests (id INTEGER PRIMARY KEY, run_id INTEGER, manifest_hash TEXT);
            INSERT INTO transactions (timestamp, base_asset) VALUES ('2025-01-01T00:00:00', 'BTC');
            INSERT INTO fx_rates (date, base, quote, rate, batch_id) VALUES ('2025-01-01', 'USD', 'EUR', '0.92', 1);
            INSERT INTO fx_batches (id, date, imported_at, source, rates_hash) VALUES (1, '2025-01-01', '2025-01-02T00:00:00', 'test', 'hash');
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_support_bundle_redacts_sensitive_env_and_dotenv_values():
    env_snapshot = build_env_config_snapshot(
        {
            "BUNDLE_TOKEN": "super-secret-token",
            "DATABASE_PASSWORD": "super-secret-password",
            "API_BASE": "http://127.0.0.1:8000",
            "CTC_MODE": "demo",
            "PATH": "/usr/bin",
        }
    )

    assert "BUNDLE_TOKEN=<omitted>" in env_snapshot
    assert "DATABASE_PASSWORD=<omitted>" in env_snapshot
    assert "API_BASE=http://127.0.0.1:8000" in env_snapshot
    assert "CTC_MODE=demo" in env_snapshot
    assert "PATH=<omitted>" in env_snapshot
    assert "super-secret" not in env_snapshot

    dotenv_snapshot = redact_dotenv_text(
        "BUNDLE_TOKEN=super-secret-token\n"
        "export DATABASE_PASSWORD=super-secret-password\n"
        "API_BASE=http://127.0.0.1:8000\n"
    )

    assert "BUNDLE_TOKEN=<redacted>" in dotenv_snapshot
    assert "export DATABASE_PASSWORD=<redacted>" in dotenv_snapshot
    assert "API_BASE=http://127.0.0.1:8000" in dotenv_snapshot
    assert "super-secret" not in dotenv_snapshot


def test_support_bundle_db_diag_captures_schema_counts_and_expected_table_status(tmp_path: Path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    _create_diag_db(project_root)
    db_meta_dir = tmp_path / "bundle" / "_db"
    st = StateLog(tmp_path / "bundle" / "_meta")
    expected_tables = [
        "calc_runs",
        "transactions",
        "fx_rates",
        "fx_batches",
        "audit_log",
        "realized_events",
        "run_digests",
    ]

    db_diag(project_root, db_meta_dir, expected_tables, st)

    db_hint = json.loads((db_meta_dir / "db_hint.json").read_text(encoding="utf-8"))
    db_report = json.loads((db_meta_dir / "db_diag.json").read_text(encoding="utf-8"))
    missing_tables = (db_meta_dir / "missing_tables.txt").read_text(encoding="utf-8")
    expected_table_text = (db_meta_dir / "expected_tables.txt").read_text(encoding="utf-8")

    assert db_hint["db_path"].endswith("cryptotaxcalc.db")
    assert db_report["counts"]["transactions"] == 1
    assert db_report["counts"]["fx_rates"] == 1
    assert "transactions_stats" in db_report
    assert missing_tables == "OK: All expected tables are present."
    for table in expected_tables:
        assert table in expected_table_text


def test_support_bundle_zip_contains_manifest_meta_db_diag_and_redacted_env_without_dotenv_file(tmp_path: Path, monkeypatch):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / ".env").write_text(
        "BUNDLE_TOKEN=super-secret-token\nAPI_BASE=http://127.0.0.1:8000\n",
        encoding="utf-8",
    )
    _create_diag_db(project_root)

    bundle_root = project_root / "support_bundles"
    bundle_dir = bundle_root / "bundle_test"
    meta_dir = bundle_dir / "_meta"
    db_meta_dir = bundle_dir / "_db"
    zip_path = bundle_root / "support_bundle_test.zip"
    for path in (meta_dir, db_meta_dir):
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("RUN_CONTEXT", "api")
    monkeypatch.setenv("BUNDLE_TOKEN", "super-secret-token")
    monkeypatch.setenv("API_BASE", "http://127.0.0.1:8000")

    expected_tables = ["calc_runs", "transactions", "fx_rates", "fx_batches", "audit_log", "realized_events", "run_digests"]
    preflight(project_root, bundle_dir, zip_path, meta_dir, "http://127.0.0.1:8000", 25, False, False)
    write_runtime_snapshot(project_root, meta_dir, "http://127.0.0.1:8000", 25)
    db_diag(project_root, db_meta_dir, expected_tables, StateLog(meta_dir))
    build_manifest(
        project_root,
        bundle_dir,
        zip_path,
        "http://127.0.0.1:8000",
        25,
        expected_tables,
        meta_dir,
        str(project_root / "cryptotaxcalc.db"),
    )

    assert zip_bundle(bundle_dir, zip_path) is True

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        env_config = zf.read("_meta/env_config.txt").decode("utf-8")
        dotenv_redacted = zf.read("_meta/dotenv_redacted.env").decode("utf-8")
        db_report = json.loads(zf.read("_db/db_diag.json").decode("utf-8"))
        missing_tables = zf.read("_db/missing_tables.txt").decode("utf-8")

    assert "manifest.json" in names
    assert "_meta/preflight_report.txt" in names
    assert "_meta/bundle_policy.txt" in names
    assert "_meta/env_keys.txt" in names
    assert "_meta/env_config.txt" in names
    assert "_meta/dotenv_redacted.env" in names
    assert "_meta/runtime.json" in names
    assert "_meta/debug_snapshot.json" in names
    assert "_db/db_diag.json" in names
    assert "_db/missing_tables.txt" in names
    assert ".env" not in names

    assert manifest["api_base"] == "http://127.0.0.1:8000"
    assert manifest["tail_lines"] == 25
    assert manifest["expected_tables"] == expected_tables
    assert manifest["db_hint"].endswith("cryptotaxcalc.db")
    assert db_report["counts"]["transactions"] == 1
    assert missing_tables == "OK: All expected tables are present."
    assert "BUNDLE_TOKEN=<omitted>" in env_config
    assert "BUNDLE_TOKEN=<redacted>" in dotenv_redacted
    assert "super-secret-token" not in env_config
    assert "super-secret-token" not in dotenv_redacted


def test_support_bundle_zip_api_mode_records_skipped_large_files(tmp_path: Path, monkeypatch):
    bundle_dir = tmp_path / "bundle"
    meta_dir = bundle_dir / "_meta"
    large_dir = bundle_dir / "large"
    meta_dir.mkdir(parents=True)
    large_dir.mkdir()
    (bundle_dir / "manifest.json").write_text('{"ok": true}', encoding="utf-8")
    (large_dir / "huge.bin").write_bytes(b"x" * 200)
    zip_path = tmp_path / "support_bundle_test.zip"

    monkeypatch.setenv("RUN_CONTEXT", "api")
    monkeypatch.setattr(collect_support_bundle, "ZIP_API_MAX_FILE_BYTES", 100)

    assert zip_bundle(bundle_dir, zip_path) is True

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        skipped = zf.read("_meta/skipped/large/huge.bin.txt").decode("utf-8")

    assert "manifest.json" in names
    assert "large/huge.bin" not in names
    assert "skipped in api-mode" in skipped
    assert "size=200" in skipped
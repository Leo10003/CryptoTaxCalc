from __future__ import annotations

import json
import zipfile

import pytest

from cryptotaxcalc import exporter

pytestmark = pytest.mark.smoke


def test_support_bundle_includes_per_run_calc_trace(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    logs_dir = project_root / "logs" / "calc" / "runs" / "123"
    logs_dir.mkdir(parents=True)

    trace_payload = {
        "run_id": 123,
        "jurisdiction": "HR",
        "events_count": 1,
        "warnings_count": 0,
    }
    trace_path = logs_dir / "trace.json"
    trace_path.write_text(json.dumps(trace_payload), encoding="utf-8")

    # Minimal source marker so the bundle has normal project structure too.
    src_pkg = project_root / "src" / "cryptotaxcalc"
    src_pkg.mkdir(parents=True)
    (src_pkg / "__init__.py").write_text("", encoding="utf-8")

    monkeypatch.setattr(exporter, "PROJECT_ROOT", project_root)

    bundle_path = exporter.build_support_bundle(output_dir=tmp_path / "out")

    assert bundle_path.exists()

    with zipfile.ZipFile(bundle_path) as zf:
        names = set(zf.namelist())

        assert "logs/calc/runs/123/trace.json" in names
        assert "logs/" in names
        assert "README_SUPPORT_BUNDLE.txt" in names
        assert "_meta/bundle_manifest.json" in names

        with zf.open("logs/calc/runs/123/trace.json") as fh:
            bundled_trace = json.loads(fh.read().decode("utf-8"))

        assert bundled_trace == trace_payload

        with zf.open("_meta/bundle_manifest.json") as fh:
            manifest = json.loads(fh.read().decode("utf-8"))

        manifest_paths = {item["path"] for item in manifest["files"]}
        assert "logs/calc/runs/123/trace.json" in manifest_paths
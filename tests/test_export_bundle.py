def test_export_bundle_smoke():
    from pathlib import Path
    from cryptotaxcalc.exporter import build_export_zip, ExportOptions

    result = build_export_zip(ExportOptions())

    # exporter.build_export_zip() returns Path in current builds; older builds may return bytes.
    if isinstance(result, (bytes, bytearray, memoryview)):
        data = bytes(result)
    else:
        data = Path(result).read_bytes()

    assert data[:2] == b"PK"  # zip magic

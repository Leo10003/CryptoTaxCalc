from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path


BLOCKED_EXACT_NAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "build.log",
}

BLOCKED_DIR_PARTS = {
    ".git",
    ".github",
    ".pytest_cache",
    "__pycache__",
    "tests",
    "docs",
    "tools",
}

BLOCKED_SUFFIXES = {
    ".pyc",
    ".pyo",
}

BLOCKED_DB_SIDECARS = {
    "-wal",
    "-shm",
}

BLOCKED_TEMPLATE_NAMES = {
    "admin_csv_unsupported.html",
    "admin.html",
    "admin_support.html",
}

BLOCKED_STATIC_PY_NAMES = {
    "render_backgrounds_gpu.py",
}

SECRET_TEXT_PATTERNS = [
    "CTC_SMTP_PASSWORD",
    "TELEGRAM_BOT_TOKEN",
    "GITHUB_TOKEN",
    "OPENAI_API_KEY",
    "ADMIN_TOKEN=",
    "CTC_SUPPORT_EMAIL=",
    "PRIVATE_KEY",
    "SECRET_KEY",
]

TEXT_SUFFIXES = {
    ".html",
    ".js",
    ".css",
    ".json",
    ".txt",
    ".md",
    ".bat",
    ".csv",
    ".log",
    ".ini",
    ".cfg",
    ".toml",
    ".yml",
    ".yaml",
}


@dataclass(frozen=True)
class Finding:
    severity: str
    path: str
    reason: str


def _norm_zip_path(name: str) -> str:
    return name.replace("\\", "/").strip("/")


def _parts(rel: str) -> tuple[str, ...]:
    return tuple(p for p in _norm_zip_path(rel).split("/") if p)


def _is_db_sidecar(rel: str) -> bool:
    low = rel.lower()
    return any(low.endswith(suffix) for suffix in BLOCKED_DB_SIDECARS)


def _blocked_reason(rel: str) -> str | None:
    rel = _norm_zip_path(rel)
    p = _parts(rel)
    if not p:
        return None

    name = p[-1]
    name_low = name.lower()
    parts_low = {x.lower() for x in p}

    if name_low in BLOCKED_EXACT_NAMES:
        return f"blocked exact filename: {name}"

    if any(part in BLOCKED_DIR_PARTS for part in parts_low):
        return "blocked project/dev directory"

    if _is_db_sidecar(rel):
        return "SQLite sidecar/runtime file"

    if name_low in BLOCKED_TEMPLATE_NAMES:
        return "admin/operator template should not ship in investor demo"

    if name_low in BLOCKED_STATIC_PY_NAMES:
        return "plain Python helper exposed in static assets"

    if Path(name_low).suffix in BLOCKED_SUFFIXES:
        return "compiled Python cache file outside expected PyInstaller archive"

    if name_low == "startup.json" and "logs" in parts_low:
        return "runtime log file"

    return None


def _iter_zip_files(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            yield info.filename


def _read_zip_text(zf: zipfile.ZipFile, name: str) -> str | None:
    suffix = Path(name).suffix.lower()
    if suffix not in TEXT_SUFFIXES:
        return None
    try:
        raw = zf.read(name)
    except Exception:
        return None
    if len(raw) > 2_000_000:
        return None
    return raw.decode("utf-8", errors="replace")


def audit_zip(zip_path: Path) -> list[Finding]:
    findings: list[Finding] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            rel = _norm_zip_path(info.filename)
            reason = _blocked_reason(rel)
            if reason:
                findings.append(Finding("fail", rel, reason))

            text = _read_zip_text(zf, info.filename)
            if text:
                for pattern in SECRET_TEXT_PATTERNS:
                    if pattern in text:
                        findings.append(Finding("fail", rel, f"secret/config marker found: {pattern}"))

    return findings


def audit_folder(root: Path) -> list[Finding]:
    findings: list[Finding] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        rel = path.relative_to(root).as_posix()
        reason = _blocked_reason(rel)
        if reason:
            findings.append(Finding("fail", rel, reason))

        if path.suffix.lower() in TEXT_SUFFIXES:
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            for pattern in SECRET_TEXT_PATTERNS:
                if pattern in text:
                    findings.append(Finding("fail", rel, f"secret/config marker found: {pattern}"))

    return findings


def sanitize_zip(input_zip: Path, output_zip: Path) -> dict[str, object]:
    removed: list[dict[str, str]] = []
    kept = 0

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(input_zip, "r") as zin, zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for info in zin.infolist():
            rel = _norm_zip_path(info.filename)

            if info.is_dir():
                continue

            reason = _blocked_reason(rel)
            if reason:
                removed.append({"path": rel, "reason": reason})
                continue

            data = zin.read(info.filename)

            # Preserve the same archive path, but do not preserve host-specific timestamps/attrs.
            zi = zipfile.ZipInfo(rel)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zout.writestr(zi, data)
            kept += 1

    return {
        "input": str(input_zip),
        "output": str(output_zip),
        "kept_files": kept,
        "removed_files": removed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit and sanitize CryptoTaxCalc demo ZIP/folder exposure.")
    parser.add_argument("target", type=Path, help="Demo ZIP or extracted demo folder")
    parser.add_argument("--sanitize-out", type=Path, default=None, help="Write sanitized ZIP here")
    parser.add_argument("--json-out", type=Path, default=None, help="Write JSON report here")
    args = parser.parse_args()

    target = args.target

    if not target.exists():
        raise SystemExit(f"Target does not exist: {target}")

    report: dict[str, object] = {
        "target": str(target),
        "sanitized": None,
        "findings": [],
    }

    if args.sanitize_out:
        if target.suffix.lower() != ".zip":
            raise SystemExit("--sanitize-out currently requires a ZIP input")
        report["sanitized"] = sanitize_zip(target, args.sanitize_out)
        findings = audit_zip(args.sanitize_out)
    else:
        findings = audit_zip(target) if target.suffix.lower() == ".zip" else audit_folder(target)

    report["findings"] = [f.__dict__ for f in findings]

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if args.sanitize_out:
        removed = report["sanitized"]["removed_files"]  # type: ignore[index]
        print(f"Sanitized ZIP written: {args.sanitize_out}")
        print(f"Removed files: {len(removed)}")

    if findings:
        print("Exposure audit FAILED:")
        for item in findings[:80]:
            print(f"- [{item.severity}] {item.path}: {item.reason}")
        if len(findings) > 80:
            print(f"... and {len(findings) - 80} more")
        return 1

    print("Exposure audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

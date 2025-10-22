# tests/test_smoke.py
import os
import sys
import json
import re
import platform
from pathlib import Path
import subprocess
import time
from datetime import datetime
import requests
import socket

import pytest
from fastapi.testclient import TestClient

#
# Import the FastAPI app
# Adjust module path only if your project layout differs.
#
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from cryptotaxcalc.app import app  # noqa: E402
except Exception as e:  # pragma: no cover
    raise RuntimeError(f"Failed to import app: {e}")

client = TestClient(app)

# ---------- helpers ----------

def _has_endpoint(path: str, method: str = "get") -> bool:
    """Check if the app has a given route/method, via OpenAPI schema."""
    try:
        resp = client.get("/openapi.json")
        if resp.status_code != 200:
            return False
        schema = resp.json()
        paths = schema.get("paths", {})
        if path not in paths:
            return False
        methods = paths[path].keys()
        return method.lower() in methods
    except Exception:
        return False

def _get_admin_token() -> str | None:
    # Prefer env var; fall back to a very common dev default if present.
    tok = os.getenv("ADMIN_TOKEN")
    if tok:
        return tok
    # Check if the app declares a token in /openapi.json description/params (best effort).
    # Otherwise, return None to skip admin tests.
    return None

def _is_windows() -> bool:
    return platform.system().lower().startswith("win")

UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")

# ---------- marks ----------

pytestmark = pytest.mark.smoke

# ---------- tests ----------

def test_docs_available():
    r = client.get("/docs")
    assert r.status_code == 200

def test_openapi_available():
    r = client.get("/openapi.json")
    assert r.status_code == 200
    assert "paths" in r.json()

def test_calculate_creates_run_and_persists():
    # Call /calculate (GET)
    assert _has_endpoint("/calculate", "get"), "App should expose GET /calculate"
    r = client.get("/calculate")
    assert r.status_code == 200, f"calculate failed: {r.text}"

    data = r.json()
    # We only assert on structure we promised, not internals.
    assert "run_id" in data and isinstance(data["run_id"], str), "Response must include run_id"
    run_id = data["run_id"]
    assert UUID_RE.match(run_id), f"run_id doesn't look like a UUID: {run_id}"

    # /history should exist and include our run
    assert _has_endpoint("/history", "get"), "App should expose GET /history"
    h = client.get("/history")
    assert h.status_code == 200, f"history failed: {h.text}"
    hist = h.json()
    assert isinstance(hist, list), "history must return a list"

    # Try to find our run_id in list items (flexible: item can be dict with 'id' or 'run_id')
    found = False
    for item in hist:
        if isinstance(item, dict):
            if str(item.get("id") or item.get("run_id")) == run_id:
                found = True
                break
        elif isinstance(item, str) and item == run_id:
            found = True
            break
    assert found, f"run_id {run_id} not found in /history list"

    # Retrieve run by id
    # We support either /history/{run_id} or /history/run/{run_id}, whichever exists.
    path_options = [f"/history/{run_id}", f"/history/run/{run_id}"]
    got_one = False
    for path in path_options:
        if _has_endpoint(path.replace(f"/{run_id}", "/{run_id}"), "get"):
            rr = client.get(path)
            if rr.status_code == 200:
                detail = rr.json()
                assert isinstance(detail, dict), "run detail should be an object"
                # Should echo the same run_id somewhere
                # Accept either 'id' or 'run_id'
                echoed = str(detail.get("id") or detail.get("run_id") or "")
                assert echoed == run_id, f"run detail did not echo run_id; got {echoed}"
                got_one = True
                break
    assert got_one, "Could not retrieve run details via available history endpoint"

def test_calculate_is_idempotent_and_creates_new_runs():
    # Run twice, ensure we get two different run_ids
    r1 = client.get("/calculate")
    assert r1.status_code == 200
    run_id1 = r1.json().get("run_id")
    r2 = client.get("/calculate")
    assert r2.status_code == 200
    run_id2 = r2.json().get("run_id")
    assert run_id1 != run_id2, "Two calculations should produce distinct run_ids"

@pytest.mark.parametrize("path_template", ["/history/{run_id}/download", "/history/run/{run_id}/download"])
def test_history_download_if_present(path_template):
    # Only run if the endpoint exists in the OpenAPI
    r = client.get("/calculate")
    assert r.status_code == 200
    run_id = r.json()["run_id"]

    templ = path_template.replace("{run_id}", "{id}")
    has = _has_endpoint(templ, "get")
    if not has:
        pytest.skip(f"{path_template} not exposed; skipping download check")

    # Try to download. Accept JSON description or a file response.
    path = path_template.format(run_id=run_id)
    resp = client.get(path)
    assert resp.status_code in (200, 204), f"download failed: {resp.text}"
    ctype = resp.headers.get("content-type", "")
    # We allow either application/json (returns path/metadata) or a file (octet-stream/zip)
    assert ("application/json" in ctype) or ("application/zip" in ctype) or ("octet-stream" in ctype)

@pytest.mark.skipif(not _is_windows(), reason="support bundle is Windows/PowerShell specific")
def test_admin_support_bundle_if_token_present():
    token = _get_admin_token()
    if not token:
        pytest.skip("ADMIN_TOKEN not configured; skipping bundle test")

    # Support bundle endpoint(s): accept either /admin/bundle or /admin/support/bundle
    path_options = ["/admin/bundle", "/admin/support/bundle"]
    found = False
    for base in path_options:
        templ = base
        if not _has_endpoint(templ, "post"):
            continue
        # token via query param (matches earlier design)
        r = client.post(f"{base}?token={token}")
        # Accept 200 or 202 (if async) or at least a structured error if denied
        assert r.status_code in (200, 202), f"bundle endpoint returned {r.status_code}: {r.text}"
        body = r.json()
        assert isinstance(body, dict)
        assert "status" in body, f"bundle response missing 'status': {body}"
        found = True
        break
    if not found:
        pytest.skip("No bundle endpoint exposed; skipping")

def test_history_missing_id_returns_4xx():
    # Try a bogus UUID; expect 4xx
    bogus = "00000000-0000-0000-0000-000000000000"
    for templ in ["/history/{run_id}", "/history/run/{run_id}"]:
        templ_openapi = templ.replace("{run_id}", "{id}")
        if not _has_endpoint(templ_openapi, "get"):
            continue
        r = client.get(templ.format(run_id=bogus))
        assert r.status_code in (400, 404), f"Expected 4xx for missing id, got {r.status_code}"

@pytest.mark.skipif(_get_admin_token() is None, reason="ADMIN_TOKEN not set; skipping git push")
def test_admin_git_push_returns_structured_result():
    token = _get_admin_token()
    # Accept either /admin/git/push or /admin/update-github
    for path in ["/admin/git/push", "/admin/update-github"]:
        if not _has_endpoint(path, "post"):
            continue
        r = client.post(f"{path}?token={token}")
        assert r.status_code in (200, 202)
        body = r.json()
        assert isinstance(body, dict)
        # We only assert presence of the structured keys; actual push may be blocked in CI/local.
        for k in ("status", "script", "return_code"):
            assert k in body, f"Missing key '{k}' in response: {body}"
        break
    else:
        pytest.skip("No git push endpoint exposed; skipping")

"""
===================================================================================
 Automated Smoke Test Runner + Telegram Alert System
===================================================================================
This section runs smoke tests on a fixed schedule and sends a Telegram alert
if any test fails.  It can be executed directly (`python smoke_test.py`)
or via your OS scheduler (Windows Task Scheduler, cron, etc.).
===================================================================================
"""

from dotenv import load_dotenv
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HOSTNAME = socket.gethostname()


def send_telegram_message(text: str):
    """Send a Telegram message to the configured bot/chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not set — skipping alert.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"❌ Telegram API error: {resp.text}")
        else:
            print("✅ Telegram alert sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send Telegram message: {e}")


def run_smoke_tests():
    """Run pytest smoke tests and send Telegram alerts on success or failure."""
    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"[{timestamp}] Running smoke tests...")

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-m", "smoke"],
        capture_output=True,
        text=True,
    )

    passed = result.returncode == 0

    if passed:
        alert_text = (
            f"✅ *CryptoTaxCalc Smoke Test Passed!* 🖥️ `{HOSTNAME}`\n"
            f"🕒 {timestamp}\n\n"
            f"All endpoints responded correctly.\n"
            f"No errors detected.\n"
        )
        send_telegram_message(alert_text)
        print("✅ All smoke tests passed successfully.")
    else:
        alert_text = (
            f"🚨 *CryptoTaxCalc Smoke Test FAILED!*\n"
            f"🕒 {timestamp}\n"
            f"💻 Exit code: {result.returncode}\n\n"
            f"📄 Output:\n```\n{result.stdout[-1500:]}\n```"
        )
        send_telegram_message(alert_text)
        print("❌ Smoke tests failed. Telegram alert sent.")

    return passed


def scheduler_loop(interval_hours: int = 6):
    """Run smoke tests every N hours indefinitely."""
    startup_time = datetime.now().isoformat(timespec="seconds")

    # 🔹 Immediate Telegram startup ping (so you know scheduler is alive)
    send_telegram_message(
        f"✅ *Smoke test monitor started successfully!*\n"
        f"🕒 {startup_time}\n"
        f"🧠 The first test run will begin right now."
    )

    # Run the first smoke test immediately
    run_smoke_tests()

    while True:
        print(f"Sleeping for {interval_hours} hours...\n")
        time.sleep(interval_hours * 60 * 60)
        run_smoke_tests()


if __name__ == "__main__":
    try:
        scheduler_loop(interval_hours=6)
    except KeyboardInterrupt:
        print("Scheduler stopped manually.")
        send_telegram_message("🟡 Smoke test scheduler stopped manually.")

import os, sys, time, json
from datetime import datetime, timezone
import requests

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")

def main():
    print("=== Smoke Test: starting ===", flush=True)

    # Use a session (connection reuse)
    with requests.Session() as s:

        # 1) wait for server to be up (retry /health with backoff)
        last_exc = None
        for attempt in range(1, 8):  # ~28s worst-case
            try:
                r = s.get(f"{API_URL}/health", timeout=10)
                if r.ok and r.json().get("status") == "ok":
                    break
            except Exception as e:
                last_exc = e
            time.sleep(attempt)
        else:
            print(f"Health check failed after retries: {last_exc}", file=sys.stderr, flush=True)
            sys.exit(1)

        # 2) /version
        r = s.get(f"{API_URL}/version", timeout=10)
        r.raise_for_status()
        print("version:", r.json(), flush=True)

        # 3) /transactions (should NOT crash; empty is fine)
        r = s.get(f"{API_URL}/transactions", params={"page": 1, "page_size": 1}, timeout=15)
        r.raise_for_status()
        data = r.json()
        # Your API returns summary fields at top-level (page/page_size/total). No 'meta' key.
        meta = {k: data.get(k) for k in ("page", "page_size", "total")}
        items = data.get("items") or data.get("transactions") or []
        print("transactions meta:", meta, flush=True)

        # 4) /report/summary for the year of the most recent tx (fallback to current UTC year)
        if items:
            ts = (items[0].get("timestamp") or items[0].get("time") or "")
            if len(ts) >= 4 and ts[:4].isdigit():
                yr = int(ts[:4])
            else:
                yr = datetime.now(timezone.utc).year
        else:
            yr = datetime.now(timezone.utc).year

        r = s.get(f"{API_URL}/report/summary", params={"year": yr}, timeout=20)
        r.raise_for_status()
        print("summary ok", flush=True)

    print("=== Smoke Test: PASS ===", flush=True)

if __name__ == "__main__":
    # make stdout safe on cp1250 consoles (Windows)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
    try:
        main()
    except requests.exceptions.RequestException as e:
        print(f"HTTP error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

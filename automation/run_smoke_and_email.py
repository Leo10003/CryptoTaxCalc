# automation/run_smoke_and_email.py
# Sends a start ping, runs pytest, then sends a PASS/FAIL summary to Telegram.
# Also writes full stdout/stderr to automation/smoke_test_output.log and exits
# with pytest's return code.

import os, io
import sys
import subprocess
from datetime import datetime, timezone
import requests

# --- Force UTF-8-friendly stdout/stderr, but never crash on Unicode ---
try:
    # Prefer UTF-8. Task Scheduler often ignores codepage; this still helps.
    os.environ["PYTHONIOENCODING"] = "utf-8"

    # Python 3.7+ has .reconfigure()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    else:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    else:
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    # Never let encoding setup kill the job
    pass


def safe_print(msg: str) -> None:
    """Print without crashing on Unicode in constrained consoles."""
    try:
        print(msg)
    except UnicodeEncodeError:
        # Fallback: strip un-encodable characters
        try:
            print(msg.encode("ascii", "ignore").decode("ascii"))
        except Exception:
            # Last-ditch: print a generic line
            print("<<message omitted due to encoding>>")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def send_telegram(msg: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHATID")
    if not token or not chat_id:
        print("Telegram not configured (missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID) — skipping.")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=15,
        )
        r.raise_for_status()
        print("Telegram message sent.")
    except Exception as e:
        print(f"Telegram send failed: {e}")


def main() -> None:
    safe_print("=== CryptoTaxCalc smoke runner ===")
    started = utcnow_iso()
    safe_print(f"Started at {started}")

    # 1) Startup ping
    send_telegram("🚀 Smoke test monitor started successfully.")

    # 2) Run pytest (smoke)
    pytest_exe = os.path.join(os.path.dirname(sys.executable), "pytest.exe")
    cmd = [pytest_exe, "-q", "-m", "smoke", "--maxfail=1", "--disable-warnings", "-rA"]
    t0 = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    t1 = datetime.now(timezone.utc)
    dur = (t1 - t0).total_seconds()

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    # 3) Save full output for debugging
    out_dir = os.path.dirname(__file__)
    log_path = os.path.join(out_dir, "logs", "smoke_test_output.log")
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("=== STDOUT ===\n")
            f.write(stdout)
            f.write("\n\n=== STDERR ===\n")
            f.write(stderr)
        safe_print(f"Wrote log: {log_path}")
    except Exception as e:
        safe_print(f"Failed writing log: {e}")

    # 4) Telegram finish summary
    if proc.returncode == 0:
        msg = f"✅ *Smoke test PASSED* in {dur:.1f}s.\nStarted: {started}\nFinished: {utcnow_iso()}"
    else:
        # Try to extract a short failure summary
        summary_lines = []
        for line in (stdout or "").splitlines():
            if "FAILURES" in line or "FAILED" in line or "ERROR" in line:
                summary_lines.append(line)
        if not summary_lines:
            summary_lines = (stdout or "").splitlines()[-10:] or (stderr or "").splitlines()[-10:]

        summary_block = "\n".join(summary_lines[-8:])
        if not summary_block:
            summary_block = "(no summary available)"

        msg = (
            f"❌ *Smoke test FAILED* (exit={proc.returncode}) in {dur:.1f}s.\n"
            f"Started: {started}\nFinished: {utcnow_iso()}\n"
            f"```\n{summary_block}\n```"
        )

    send_telegram(msg)
    safe_print(msg.replace("✅", "[OK]").replace("❌", "[FAIL]").replace("🚀", "[START]"))
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()

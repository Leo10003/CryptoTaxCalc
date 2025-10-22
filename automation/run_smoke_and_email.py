import os, sys, io

if sys.stdout:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr:
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
os.environ["PYTHONIOENCODING"] = "utf-8"

import subprocess, smtplib, ssl, time
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone, UTC
import json
import urllib.request
from pathlib import Path

try:
    import requests  # used for Telegram
except ImportError:
    print("requests not found. Install with: pip install requests", flush=True)
    sys.exit(2)

def _print(msg: str):
    print(msg, flush=True)

def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

LOGS = Path(__file__).resolve().parent / "logs"
LOGS.mkdir(exist_ok=True, parents=True)
with (LOGS / "smoke_runner_trace.log").open("a", encoding="utf-8") as f:
    f.write(f"[{datetime.now(UTC).isoformat(timespec='seconds')}] runner import OK\n")

# --- Config from environment or .env ---
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(ENV_PATH):
    # minimal .env parser (no extra deps)
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line: 
                continue
            k,v = line.split("=",1)
            os.environ.setdefault(k.strip(), v.strip())

SMTP_HOST   = os.getenv("SMTP_HOST", "")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER   = os.getenv("SMTP_USER", "")
SMTP_PASS   = os.getenv("SMTP_PASS", "")
MAIL_TO     = os.getenv("MAIL_TO", "")
MAIL_FROM   = os.getenv("MAIL_FROM", SMTP_USER or "cryptotaxcalc@localhost")
APP_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# --- Cooldown (minutes) to avoid alert spam ---
COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "60"))

# --- Cooldown storage (local file) ---
LOG_DIR    = os.path.join(os.path.dirname(__file__), "logs")
LAST_ALERT = os.path.join(LOG_DIR, "last_alert.txt")
os.makedirs(LOG_DIR, exist_ok=True)

# --- Helpers ---
def send_failure_email(subject: str, body: str):
    # If SMTP vars are not set, do nothing silently (we use Telegram now)
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and MAIL_TO):
        return

    msg = EmailMessage()
    msg["From"] = MAIL_FROM or SMTP_USER
    msg["To"]   = MAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT or 587, timeout=30) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            return
    except Exception:
        # No fallback needed; Telegram handles alerts now
        return

    # If SMTP vars are not set, do nothing silently (we use Telegram now)
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and MAIL_TO):
        return

    msg = EmailMessage()
    msg["From"] = MAIL_FROM or SMTP_USER
    msg["To"]   = MAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT or 587, timeout=30) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            return
    except Exception:
        # No fallback needed; Telegram handles alerts now
        return

    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and MAIL_TO):
        sys.stderr.write("SMTP not configured; cannot send email.\n")
        return

    msg = EmailMessage()
    msg["From"] = MAIL_FROM or SMTP_USER
    msg["To"]   = MAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    # Try STARTTLS on 587 first, then fallback to SSL on 465
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            return
    except smtplib.SMTPAuthenticationError as e:
        # Gmail-specific hint
        hint = ""
        if "gmail.com" in SMTP_HOST or SMTP_USER.endswith("@gmail.com"):
            hint = (
                "\nHINT (Gmail): Enable 2-Step Verification and use an App Password "
                "(Google Account -> Security -> App passwords)."
            )
        sys.stderr.write(f"SMTP auth failed on 587 STARTTLS: {e}{hint}\n")

    # Fallback to SMTPS:465
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, 465, context=context, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
            return
    except Exception as e2:
        sys.stderr.write(f"SMTP SSL (465) also failed: {repr(e2)}\n")

def send_telegram_alert(message: str, body: str | None = None) -> bool:
    """
    Sends a Telegram message.
    If 'body' is provided, the final text is 'subject\\n\\nbody'.
    Looks for TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID (or legacy TELEGRAM_TOKEN/TELEGRAM_CHATID).
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("TELEGRAM_CHATID")

    if not token or not chat_id:
        _print("⚠️  Telegram not configured (missing TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN or TELEGRAM_CHAT_ID/TELEGRAM_CHATID).")
        return False

    text = f"{message}\n\n{body}" if body else message
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200 and (r.json().get("ok") is True):
            _print("✅ Telegram message sent.")
            return True
        _print(f"❌ Telegram send failed: HTTP {r.status_code} / {r.text}")
        return False
    except Exception as e:
        _print(f"❌ Telegram send exception: {e}")
        return False

def _read_last_alert_time():
    try:
        with open(LAST_ALERT, "r", encoding="utf-8") as f:
            iso = f.read().strip()
        return datetime.fromisoformat(iso.replace("Z","")).replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _write_last_alert_time(ts: datetime):
    try:
        with open(LAST_ALERT, "w", encoding="utf-8") as f:
            f.write(ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    except Exception:
        pass

def _within_cooldown(now_utc: datetime) -> bool:
    last = _read_last_alert_time()
    if not last:
        return False
    delta = now_utc - last
    return delta.total_seconds() < COOLDOWN_MINUTES

if __name__ == "__main__" and "--ping" in sys.argv:
    _print("Sending startup ping (--ping) ...")
    ok = send_telegram_alert(f"🚀 Smoke test monitor started successfully at {_now_iso()} ✅")
    sys.exit(0 if ok else 1)

def project_root() -> Path:
    # .../CryptoTaxCalc/automation/run_smoke_and_email.py -> /CryptoTaxCalc
    return Path(__file__).resolve().parents[1]

def run_smoke_tests() -> tuple[int, str, str]:
    """
    Run the smoke test via pytest from the project root, targeting tests/smoke_test.py.
    Returns: (returncode, stdout, stderr)
    """
    root = project_root()

    # Prefer venv python if present; fall back to current interpreter
    venv_python = root / ".venv" / "Scripts" / "python.exe"
    python_exe = str(venv_python) if venv_python.exists() else sys.executable

    # Explicitly run pytest module against the correct test path
    cmd = [
        python_exe,
        "-m", "pytest",
        "-q",
        "-m", "smoke",
        "tests/smoke_test.py",
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(root),             # IMPORTANT: run from repo root
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return proc.returncode, proc.stdout, proc.stderr

def get_python_exe() -> str:
    """Return path to venv Python if available, else current interpreter."""
    root = Path(__file__).resolve().parents[1]
    venv_python = root / ".venv" / "Scripts" / "python.exe"
    return str(venv_python) if venv_python.exists() else sys.executable

def get_smoke_test_path() -> str:
    """Return path to the smoke test file (inside /tests)."""
    root = project_root()
    test_path = root / "tests" / "smoke_test.py"
    return str(test_path)

# --- Main ---
def main():
    _print("=== CryptoTaxCalc smoke runner ===")
    _print(f"Started at {_now_iso()}")

    # Optional opt-out via env
    if os.environ.get("SMOKE_STARTUP_PING", "1") not in ("0", "false", "False"):
        send_telegram_alert(f"🚀 Smoke test monitor started successfully at {_now_iso()} ✅")

    # Prefer venv python; fallback to "python" if missing
    py = get_python_exe()
    smoke_path = get_smoke_test_path()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "--ping-start" in sys.argv:
        send_telegram_alert(
            "[CryptoTaxCalc] Smoke Test Monitor Started ✅",
            f"Task scheduler launched successfully at {ts}."
        )

    try:
        proc = subprocess.run(
            [py, smoke_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
            timeout=300
        )
        stdout_txt = proc.stdout.decode("utf-8", errors="replace")
        stderr_txt = proc.stderr.decode("utf-8", errors="replace")
    except Exception as e:
        now = datetime.now(timezone.utc)
        subject = f"[CryptoTaxCalc] SMOKE TEST CRASHED (launcher) @ {ts}"
        body = f"Launcher exception:\n{repr(e)}"

        if _within_cooldown(now):
            # Suppress repeated alerts during cooldown window
            sys.stderr.write("Cooldown active: suppressing alert.\n")
            sys.exit(2)

        send_failure_email(subject, body)
        send_telegram_alert(subject, body)
        _write_last_alert_time(now)
        sys.exit(2)

    # Handle non-zero return code here (outside the except)
    if proc.returncode != 0:
        now = datetime.now(timezone.utc)
        subject = f"[CryptoTaxCalc] SMOKE TEST FAILED @ {ts}"
        body = (
            f"Return code: {proc.returncode}\n\n"
            f"--- STDOUT ---\n{stdout_txt}\n\n"
            f"--- STDERR ---\n{stderr_txt}\n"
        )

        if _within_cooldown(now):
            sys.stderr.write("Cooldown active: suppressing alert.\n")
            sys.exit(proc.returncode)

        send_failure_email(subject, body)
        send_telegram_alert(subject, body)
        _write_last_alert_time(now)
        sys.exit(proc.returncode)

    # Success: do nothing (no emails, no logs)
    sys.exit(0)

    # Prefer venv python; fallback to "python" if missing
    py = PYTHON_EXE if os.path.exists(PYTHON_EXE) else "python"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        proc = subprocess.run(
            [py, smoke_path],
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=False,                  
            timeout=300
        )

        stdout_txt = proc.stdout.decode("utf-8", errors="replace")
        stderr_txt = proc.stderr.decode("utf-8", errors="replace")
    except Exception as e:
            now = datetime.now(timezone.utc)
            subject = f"[CryptoTaxCalc] SMOKE TEST CRASHED (launcher) @ {ts}"
            body = f"Launcher exception:\n{repr(e)}"

            if _within_cooldown(now):
                # Suppress repeated alerts during cooldown window
                sys.stderr.write("Cooldown active: suppressing alert.\n")
                sys.exit(2)

            send_failure_email(subject, body)
            send_telegram_alert(subject, body)
            _write_last_alert_time(now)
            sys.exit(2)

            if proc.returncode != 0:
                now = datetime.now(timezone.utc)
                subject = f"[CryptoTaxCalc] SMOKE TEST FAILED @ {ts}"
                body = (
                f"Return code: {proc.returncode}\n\n"
                f"--- STDOUT ---\n{stdout_txt}\n\n"
                f"--- STDERR ---\n{stderr_txt}\n"
            )

            if _within_cooldown(now):
                sys.stderr.write("Cooldown active: suppressing alert.\n")
                sys.exit(proc.returncode)

            send_failure_email(subject, body)
            send_telegram_alert(subject, body)
            _write_last_alert_time(now)
            sys.exit(proc.returncode)

    # Success: do nothing (no emails, no logs)
    sys.exit(0)

if __name__ == "__main__":
    main()

import os, sys, subprocess, smtplib, ssl
from email.message import EmailMessage
from datetime import datetime, timezone
import json
import urllib.request

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

# --- Paths ---
PYTHON_EXE  = os.path.join(APP_ROOT, ".venv", "Scripts", "python.exe")
SMOKE_PATH  = os.path.join(APP_ROOT, "smoke_test.py")

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

def send_telegram_alert(subject: str, body: str):
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not (token and chat_id):
        return  # not configured
    text = f"*{subject}*\n```\n{body[:3500]}\n```"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    req = urllib.request.Request(
        url=f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
    except Exception as e:
        sys.stderr.write(f"Telegram alert failed: {repr(e)}\n")

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
    return delta.total_seconds() < COOLDOWN_MINUTES * 60

# --- Main ---
def main():
    # Prefer venv python; fallback to "python" if missing
    py = PYTHON_EXE if os.path.exists(PYTHON_EXE) else "python"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        proc = subprocess.run(
            [py, SMOKE_PATH],
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
            [py, SMOKE_PATH],
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

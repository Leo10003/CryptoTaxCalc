from __future__ import annotations
import sys, os, time, socket, webbrowser, argparse, threading
from pathlib import Path
import uvicorn
import urllib.request

# Ensure demo environment variables are set BEFORE importing the app/db layer
os.environ.setdefault("DEMO_MODE", "true")
os.environ.setdefault("SQLITE_URL", "sqlite:///demo/demo.sqlite")
os.environ.setdefault("ADMIN_TOKEN", "demo-admin")

from cryptotaxcalc.logging_setup import get_logger, _now_iso_z
from cryptotaxcalc.demo_assets import ensure_demo_env
from cryptotaxcalc.app import app as fastapi_app

logger = get_logger("demo.launch")

def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

def _normalize_cwd_for_frozen():
    """
    In PyInstaller onefile, the process extracts to a temp dir.
    Set CWD to the exe directory so relative paths like logs/ and demo/ work.
    """
    try:
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            os.chdir(exe_dir)
    except Exception:
        pass

def main():
    """
    Entry point for the packaged EXE.
    - Ensures demo env
    - Starts uvicorn on a free port
    - Opens browser to /demo/dashboard
    - Supports --smoke mode for build verification
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Run short smoke and exit")
    parser.add_argument("--port", type=int, default=0, help="Port override")
    args = parser.parse_args()

    _normalize_cwd_for_frozen()  # ✅ ensure predictable working dir

    ensure_demo_env()

    port = args.port or _free_port()
    host = "127.0.0.1"
    url = f"http://{host}:{port}/demo/dashboard"

    # ✅ pass the app object, not a string import path
    config = uvicorn.Config(fastapi_app, host=host, port=port, log_level="info", reload=False)
    server = uvicorn.Server(config)

    def _open():
        time.sleep(1.0)
        try:
            webbrowser.open(url)
        except Exception:
            pass

    if args.smoke:
        t = threading.Thread(target=server.run, daemon=True)
        t.start()

        # Actively verify the server is reachable (demo verification must be real)
        health_url = f"http://{host}:{port}/health"
        deadline = time.time() + 10.0
        last_err = None

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=1.5) as resp:
                    if getattr(resp, "status", 200) == 200:
                        return 0
            except Exception as e:
                last_err = e
                time.sleep(0.25)

        logger.error(f"Smoke failed: /health not reachable within timeout. Last error: {last_err}")
        return 2
    else:
        threading.Thread(target=_open, daemon=True).start()
        print(f"CryptoTaxCalc Demo starting @ {url}")
        return server.run()

if __name__ == "__main__":
    try:
        code = main()
        sys.exit(code if isinstance(code, int) else 0)
    except Exception as e:
        try:
            # Best-effort emergency logging (works inside the frozen exe too)
            from pathlib import Path
            log_dir = Path("logs/demo.launch")
            log_dir.mkdir(parents=True, exist_ok=True)
            with (log_dir / "events.log").open("a", encoding="utf-8") as f:
                import traceback, datetime
                f.write(f"{datetime.datetime.utcnow().isoformat()}Z [ERROR] demo.launch: {e}\n")
                traceback.print_exc(file=f)
        except Exception:
            pass
        # Also print to console so you can see it when run from cmd/PowerShell
        print("Fatal error in demo launcher:", e, file=sys.stderr)
        sys.exit(1)

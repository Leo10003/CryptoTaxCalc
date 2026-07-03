from __future__ import annotations
import sys, os, time, socket, webbrowser, argparse, threading
import shutil
from pathlib import Path
import uvicorn
import urllib.request

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
    

def _ensure_writable_demo_db() -> Path:
    """
    Ensure a writable demo.sqlite exists at ./demo/demo.sqlite (relative to CWD),
    which we already normalize to the exe directory when frozen.
    """
    exe_dir = Path(os.getcwd()).resolve()
    dst_dir = exe_dir / "demo"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "demo.sqlite"

    if dst.exists() and dst.is_file():
        return dst

    seed = _find_seed_demo_db()
    if not seed:
        raise RuntimeError("Seed demo.sqlite not found (expected demo/demo.sqlite in bundle)")
    shutil.copy2(seed, dst)
    return dst


# Ensure demo environment variables are set BEFORE importing the app/db layer
os.environ.setdefault("DEMO_MODE", "true")
# Ensure demo DB is writable next to the EXE (SQLite needs WAL/SHM write access)
try:
    _normalize_cwd_for_frozen()  # guarantee CWD is exe dir before resolving ./demo
    writable_db = _ensure_writable_demo_db()
    os.environ.setdefault("SQLITE_URL", f"sqlite:///{writable_db.as_posix()}")
except Exception:
    # Fallback to the previous relative path (may fail, but preserves behavior if something unexpected happens)
    os.environ.setdefault("SQLITE_URL", "sqlite:///demo/demo.sqlite")
os.environ.setdefault("ADMIN_TOKEN", "demo-admin")

# Disable dotenv for packaged demos to avoid accidentally reading a local .env
os.environ.setdefault("CTC_DISABLE_DOTENV", "true")

# Lock down admin surfaces in the investor demo (reduces accidental discovery + trust concerns)
os.environ.setdefault("ENABLE_ADMIN_ENDPOINTS", "false")
os.environ.setdefault("ENABLE_ADMIN_SCRIPTS", "false")
os.environ.setdefault("ALLOW_QUERY_TOKENS", "false")

# Explicitly mark this runtime as demo (not production)
os.environ.setdefault("CTC_ENV", "demo")

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
    

def _find_seed_demo_db() -> Path | None:
    """
    Locate the bundled seed demo.sqlite.
    Supports:
      - source tree (project_root/demo/demo.sqlite)
      - onedir bundle (./demo/demo.sqlite)
      - onedir internal (./_internal/demo/demo.sqlite)
    """
    project_root = Path(__file__).resolve().parents[1]
    exe_dir = Path(os.getcwd()).resolve()

    candidates = [
        project_root / "demo" / "demo.sqlite",
        exe_dir / "demo" / "demo.sqlite",
        exe_dir / "_internal" / "demo" / "demo.sqlite",
        exe_dir / "_internal" / "cryptotaxcalc" / "demo" / "demo.sqlite",
    ]
    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p
        except Exception:
            pass
    return None


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
        deadline = time.time() + 30.0
        last_err = None

        while time.time() < deadline:
            try:
                with urllib.request.urlopen(health_url, timeout=2.5) as resp:
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

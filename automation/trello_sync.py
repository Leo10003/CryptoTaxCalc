"""
trello_sync.py
- Pushes structured updates to Trello (creates a card).
- Fails safe: logs locally if Trello credentials are missing or invalid.
- Debug-first: preflight checks + clear diagnostics in support_bundles/_meta.

ENV (in .env, no quotes or inline comments):
  TRELLO_SYNC=1
  TRELLO_BOARD_ID=6fuIiPxW              # or full URL like https://trello.com/b/6fuIiPxW/cryptotaxcalc
  TRELLO_LIST_ID=                        # optional; if blank we auto-detect the first open list
  TRELLO_KEY=...
  TRELLO_TOKEN=...                       # must have scope=read,write and belong to a member of the board
"""

from __future__ import annotations
import os, sys, json, re, traceback, urllib.request, urllib.parse
from pathlib import Path
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone
from dotenv import load_dotenv

# ---------- Paths / constants ----------
load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "support_bundles" / "_meta"
META.mkdir(parents=True, exist_ok=True)

# always load .env from project root (not CWD)
load_dotenv(dotenv_path=ROOT / ".env")

# ---------- Logging helpers ----------
def get_token_member_id(key: str, token: str) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (member_id, raw_json) for the token; member_id is None on failure.
    """
    try:
        code, text = http_get(
            f"https://api.trello.com/1/tokens/{token}",
            {"key": key, "token": token, "fields": "idMember"}
        )
        log_line(f"Diag tokens/<token> -> {code}")
        if code != 200:
            return None, text
        data = json.loads(text)
        return data.get("idMember"), text
    except Exception as e:
        log_line(f"Diag token member lookup failed: {e}")
        return None, None

def get_member_profile(member_id: str, key: str, token: str) -> tuple[Optional[dict], Optional[str]]:
    """
    Returns (member_dict, raw_json) where member_dict includes username/fullName.
    """
    try:
        code, text = http_get(
            f"https://api.trello.com/1/members/{member_id}",
            {"key": key, "token": token, "fields": "username,fullName,url"}
        )
        log_line(f"Diag members/{member_id} -> {code}")
        if code != 200:
            return None, text
        return json.loads(text), text
    except Exception as e:
        log_line(f"Diag member profile failed: {e}")
        return None, None

def get_board_info(board_id: str, key: str, token: str) -> tuple[Optional[dict], Optional[str]]:
    """
    Returns (board_dict, raw_json) where board_dict includes name/url/shortLink.
    """
    try:
        code, text = http_get(
            f"https://api.trello.com/1/boards/{board_id}",
            {"key": key, "token": token, "fields": "name,url,shortLink"}
        )
        log_line(f"Diag boards/{board_id} -> {code}")
        if code != 200:
            return None, text
        return json.loads(text), text
    except Exception as e:
        log_line(f"Diag board info failed: {e}")
        return None, None

def get_board_members(board_id: str, key: str, token: str) -> tuple[list[dict], Optional[str]]:
    """
    Returns (members_list, raw_json). Each member has id, username, fullName.
    """
    try:
        code, text = http_get(
            f"https://api.trello.com/1/boards/{board_id}/members",
            {"key": key, "token": token, "fields": "id,username,fullName"}
        )
        log_line(f"Diag boards/{board_id}/members -> {code}")
        if code != 200:
            return [], text
        return json.loads(text), text
    except Exception as e:
        log_line(f"Diag board members failed: {e}")
        return [], None

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _append(path: Path, line: str) -> None:
    path.write_text((path.read_text(encoding="utf-8") if path.exists() else "") + line, encoding="utf-8")

def log_line(msg: str) -> None:
    _append(META / "trello_sync.log", f"[{ts()}] {msg}\n")

def write_payload(payload: dict) -> None:
    (META / "trello_sync_payload.last.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    with (META / "trello_sync_history.jsonl").open("a", encoding="utf-8") as w:
        w.write(json.dumps(payload, ensure_ascii=False) + "\n")

def write_error(msg: str, exc: Optional[BaseException] = None) -> None:
    body = f"[{ts()}] {msg}\n"
    if exc:
        body += f"{traceback.format_exc()}\n"
    (META / "trello_sync_error.txt").write_text(body, encoding="utf-8")
    log_line("ERROR: " + msg.replace("\n", " "))

# ---------- HTTP helpers ----------
def http_get(url: str, qs: Dict[str, str], timeout: float = 12.0) -> Tuple[int, str]:
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(qs), method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read().decode("utf-8", "replace")

def http_post_form(url: str, form: Dict[str, str], timeout: float = 12.0) -> Tuple[int, str]:
    data = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.getcode(), resp.read().decode("utf-8", "replace")

# ---------- Env sanitation ----------
def _clean(v: str | None) -> str:
    """Strip whitespace and inline comments like 'ABC  # note' -> 'ABC'."""
    return (v or "").split("#", 1)[0].strip()

def _normalize_board_id(raw: str) -> str:
    """
    Accepts shortLink '6fuIiPxW' OR full board URL like:
      https://trello.com/b/6fuIiPxW/cryptotaxcalc
    Returns '6fuIiPxW'.
    """
    s = _clean(raw)
    m = re.search(r"/b/([A-Za-z0-9]{8})", s)
    return m.group(1) if m else s

# ---------- Preflight helpers ----------
def trello_token_info(key: str, token: str) -> None:
    try:
        code, text = http_get(f"https://api.trello.com/1/tokens/{token}", {"key": key, "token": token})
        log_line(f"Preflight token -> {code}")
        if code == 200:
            (META / "trello_token_info.json").write_text(text, encoding="utf-8")
    except Exception as e:
        log_line(f"Preflight token info failed: {e}")

def trello_board_info(board_id: str, key: str, token: str) -> bool:
    try:
        code, text = http_get(
            f"https://api.trello.com/1/boards/{board_id}",
            {"key": key, "token": token, "fields": "name,shortLink,closed,prefs"}
        )
        log_line(f"Preflight board -> {code}")
        if code == 200:
            (META / "trello_board_info.json").write_text(text, encoding="utf-8")
            return True
        return False
    except Exception as e:
        log_line(f"Preflight board info failed: {e}")
        return False

def me_boards_shortlinks(key: str, token: str) -> List[str]:
    try:
        code, text = http_get(
            "https://api.trello.com/1/members/me/boards",
            {"key": key, "token": token, "fields": "name,shortLink"}
        )
        log_line(f"Preflight me/boards -> {code}")
        if code != 200:
            return []
        data = json.loads(text)
        return [b.get("shortLink") for b in data if isinstance(b, dict) and b.get("shortLink")]
    except Exception as e:
        log_line(f"Preflight me/boards failed: {e}")
        return []

# ---------- Core ----------
def find_first_open_list(board_id: str, key: str, token: str) -> Optional[str]:
    code, text = http_get(
        f"https://api.trello.com/1/boards/{board_id}/lists",
        {"key": key, "token": token, "fields": "name,closed"}
    )
    if code != 200:
        log_line(f"lists GET -> {code}")
        return None
    arr = json.loads(text)
    for lst in arr:
        if not lst.get("closed"):
            return lst.get("id")
    return None

def _fingerprint_token(tok: str) -> str:
    # short, non-sensitive fingerprint: length + first 4 + last 4
    if not tok:
        return "<none>"
    t = tok.strip()
    return f"len={len(t)}:{t[:4]}…{t[-4:]}"

def main(argv: Optional[list[str]] = None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--intent", required=True, choices=["FIX", "ADD", "REF", "DOC", "TEST"])
    ap.add_argument("--title", required=True)
    ap.add_argument("--notes", default="")
    ap.add_argument("--labels", default="")
    ap.add_argument("--force", action="store_true", help="Override TRELLO_SYNC and post anyway")
    ap.add_argument("--env-file", default=None, help="Path to .env (overrides default ROOT/.env)")
    args = ap.parse_args(argv)

    # Payload & local history
    payload = {
        "timestamp": ts(),
        "intent": args.intent,
        "title": args.title.strip(),
        "notes": args.notes.strip(),
        "labels": [x.strip() for x in args.labels.split(",") if x.strip()],
    }
    write_payload(payload)
    log_line(f"Prepared payload intent={args.intent} title={args.title.strip()}")

    # Load & sanitize env (allow explicit file)
    env_path = (Path(args.env_file).resolve()
                if args.env_file else (ROOT / ".env").resolve())
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)
    except Exception as e:
        log_line(f"WARNING: dotenv load failed from {env_path}: {e}")

    board_id = _normalize_board_id(os.getenv("TRELLO_BOARD_ID"))
    list_id  = _clean(os.getenv("TRELLO_LIST_ID"))
    key      = _clean(os.getenv("TRELLO_KEY"))
    token    = _clean(os.getenv("TRELLO_TOKEN"))
    sync_raw = _clean(os.getenv("TRELLO_SYNC"))

    log_line(
        "Env: "
        f"file={str(env_path)}, "
        f"TRELLO_SYNC={'<force>' if args.force else (sync_raw or '<unset>')}, "
        f"BOARD_ID={board_id or '<unset>'}, "
        f"KEY={'set' if bool(key) else 'missing'}, "
        f"TOKEN={_fingerprint_token(token)}"
    )

    if not args.force and (sync_raw or "").lower() not in {"1", "true", "yes"}:
        log_line("TRELLO_SYNC disabled → local-only mode.")
        return 0

    # --- Deep diagnostics: map token user <-> board membership ---
    member_id, token_raw = get_token_member_id(key, token)
    member_prof, member_raw = (None, None)
    board_info, board_raw = get_board_info(board_id, key, token)
    board_members, board_members_raw = get_board_members(board_id, key, token)

    if member_id:
        member_prof, member_raw = get_member_profile(member_id, key, token)

    # Persist raw diagnostics for forensics
    if token_raw:
        (META / "trello_token_info.json").write_text(token_raw, encoding="utf-8")
    if member_raw:
        (META / "trello_member_info.json").write_text(member_raw, encoding="utf-8")
    if board_raw:
        (META / "trello_board_info.json").write_text(board_raw, encoding="utf-8")
    if board_members_raw:
        (META / "trello_board_members.json").write_text(board_members_raw, encoding="utf-8")

    m_user = (member_prof or {}).get("username", "<unknown>")
    m_name = (member_prof or {}).get("fullName", "<unknown>")
    b_name = (board_info or {}).get("name", board_id)
    log_line(f"Diag: token user = {m_user} ({m_name}), board = {b_name} ({board_id})")

    if member_id and board_members:
        member_ids = {m.get("id") for m in board_members if isinstance(m, dict)}
        if member_id not in member_ids:
            sample = [f"{m.get('username','?')}({m.get('id','?')})" for m in board_members[:10] if isinstance(m, dict)]
            msg = (
                "Token user is NOT a member of the target board.\n"
                f"- Token user: {m_user} ({m_name}), id={member_id}\n"
                f"- Board: {b_name} (id/shortLink={board_id})\n"
                f"- Board members (sample): {', '.join(sample) or '<none>'}\n\n"
                "Action: Invite this Trello account to the board with WRITE access, then retry."
            )
            write_error(msg)
            return 1

    # Must have credentials
    if not board_id or not key or not token:
        write_error("Missing Trello env: require TRELLO_BOARD_ID, TRELLO_KEY, TRELLO_TOKEN")
        return 0

    # Preflights (write rich diagnostics to _meta)
    trello_token_info(key, token)
    my_shortlinks = me_boards_shortlinks(key, token)
    if my_shortlinks and board_id not in my_shortlinks:
        msg = (
            f"Token user is NOT a member of board '{board_id}'.\n"
            f"Accessible boards: {', '.join(my_shortlinks)}\n"
            "Invite this Trello account to the target board with WRITE access."
        )
        write_error(msg)
        return 1

    if not trello_board_info(board_id, key, token):
        write_error(f"Cannot read board '{board_id}'. Check membership and visibility.")
        return 1

    # Detect list if not provided
    if not list_id:
        list_id = find_first_open_list(board_id, key, token)
        if not list_id:
            write_error("No open lists found on the board (or cannot read lists).")
            return 1
        log_line(f"Auto-detected list_id={list_id}")

    # Build card
    card_name = f"{args.intent}: {args.title.strip()}"[:512]
    desc = args.notes.strip()
    form = {"idList": list_id, "name": card_name, "desc": desc, "key": key, "token": token}

    try:
        code, text = http_post_form("https://api.trello.com/1/cards", form)
        log_line(f"Trello card POST -> {code}")
        if code >= 300:
            (META / "trello_sync_error.txt").write_text(
                f"[{ts()}] Trello POST failed ({code}): {text[:800]}\n", encoding="utf-8"
            )
            if code == 401:
                log_line("HINT: 401 on POST→ token lacks WRITE scope OR user is not a board member.")
            return 1
        return 0
    except Exception as e:
        hint = ""
        if "401" in str(e):
            hint = "\nHINT: 401 Unauthorized on POST usually means:\n" \
                   "- Token lacks WRITE scope (regenerate with scope=read,write)\n" \
                   "- OR the token's user is not a member with write access to the board\n"
        write_error(f"Trello sync failed: {e}{hint}", e)
        return 1

if __name__ == "__main__":
    sys.exit(main())

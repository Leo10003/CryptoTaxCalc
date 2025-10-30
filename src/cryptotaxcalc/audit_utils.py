# audit_utils.py
import json, datetime
from sqlalchemy import text
from .db import engine

def audit(actor: str, action: str, target_type: str | None, target_id: int | None, details: dict | None, ip: str | None = None):
    ts = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO audit_log (actor, action, target_type, target_id, details_json, ip, ts) VALUES (:a,:b,:c,:d,:e,:f,:g)"),
            dict(a=actor, b=action, c=target_type, d=target_id, e=json.dumps(details or {}), f=ip, g=ts)
        )

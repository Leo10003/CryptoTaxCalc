from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["export"])


@router.get("/export", response_class=HTMLResponse, include_in_schema=False, tags=["export"])
def export_ui():
    return """
<!doctype html><html><head><meta charset="utf-8"><title>Project Export</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0b1020;color:#e9eefc;margin:0}
.wrap{max-width:920px;margin:28px auto;padding:0 16px}
.card{background:#111a33;border:1px solid rgba(255,255,255,.08);border-radius:16px;padding:18px;margin:14px 0}
h1{font-size:22px;margin:0 0 6px}
p{margin:6px 0 0;color:rgba(233,238,252,.72);line-height:1.45}
.row{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
input,button,label{font-size:14px}
input[type=text]{padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.12);background:#0b1020;color:#e9eefc;min-width:260px}
button{padding:10px 14px;border-radius:12px;border:1px solid rgba(255,255,255,.14);background:#1a2a55;color:#e9eefc;cursor:pointer}
button:hover{filter:brightness(1.06)}
.small{font-size:12px;color:rgba(233,238,252,.7);margin-top:8px}
.chk{display:flex;align-items:center;gap:8px}
hr{border:none;border-top:1px solid rgba(255,255,255,.08);margin:14px 0}
.status{margin-top:10px;font-size:13px;color:rgba(233,238,252,.78)}
</style></head><body>
<div class="wrap">
  <h1>Project Export Bundle</h1>
  <p>Internal operator tool: builds a ZIP bundle using <code>/export/bundle</code>. Requires admin token.</p>

  <div class="card">
    <div class="row">
      <input id="token" type="text" placeholder="X-Admin-Token (do not paste into URLs)" autocomplete="off">
      <button id="go">Build bundle</button>
    </div>

    <div class="row">
      <label class="chk"><input id="hist" type="checkbox" checked> Include history</label>
      <label class="chk"><input id="db" type="checkbox" checked> Include DB</label>
      <label class="chk"><input id="logs" type="checkbox" checked> Include logs</label>
      <label class="chk"><input id="envr" type="checkbox" checked> Include redacted env</label>
      <label class="chk"><input id="req" type="checkbox" checked> Include requirements</label>
      <label class="chk"><input id="py" type="checkbox" checked> Include pyproject</label>
      <label class="chk"><input id="git" type="checkbox" checked> Include git meta</label>
    </div>

    <div class="small">
      This page is mounted only when <b>CTC_ENV != production</b> and
      <b>ENABLE_ADMIN_ENDPOINTS=1</b> and <b>ENABLE_ADMIN_SCRIPTS=1</b>.
    </div>

    <div id="status" class="status"></div>
  </div>
</div>

<script>
const status = document.getElementById('status');
document.getElementById('go').addEventListener('click', async () => {
  status.textContent = 'Building…';

  const token = document.getElementById('token').value.trim();
  if (!token) { status.textContent = 'Missing token.'; return; }

  const payload = {
    include_history: document.getElementById('hist').checked,
    include_db: document.getElementById('db').checked,
    include_logs: document.getElementById('logs').checked,
    include_env_redacted: document.getElementById('envr').checked,
    include_requirements: document.getElementById('req').checked,
    include_pyproject: document.getElementById('py').checked,
    include_git_meta: document.getElementById('git').checked
  };

  const res = await fetch('/export/bundle', {
    method:'POST',
    headers:{'Content-Type':'application/json', 'X-Admin-Token': token},
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const t = await res.text().catch(() => '');
    status.textContent = 'Failed: ' + res.status + (t ? (' ' + t.slice(0,200)) : '');
    return;
  }

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = 'CryptoTaxCalc_Export.zip';
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
  status.textContent = 'Done.';
});
</script></body></html>
    """

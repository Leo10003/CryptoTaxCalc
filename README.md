# 💼 CryptoTaxCalc

**FastAPI + modern frontend crypto tax platform**  
Local-first, compliance-aware, and designed for professional investor demos.  
Includes full **Demo Mode**, offline EXE packaging, self-checks, smart backups,  
and visual dashboards for transaction import, calculation, and export.

---

## ✨ Key Features

### ⚙️ Core Engine
- **FIFO tax calculator** — precise cost basis, proceeds, and gains computation.
- **Persistent history** — all calculation runs stored in SQLite with unique `run_id`.
- **Jurisdiction-aware rules** — supports `HR` (Croatia) and `IT` (Italy) frameworks.
- **Automated backups** — Smart Backup & Revert Gatekeeper keeps last 30 bundles.

### 🧩 Demo Mode System
Built for **offline investor showcases**.

- 🟢 **Demo banner** – visible "Demo Mode" indicator (synthetic local data only)
- 🔄 **Reset environment** – restores DB and synthetic sample data in one click
- 🧮 **Guided flow** – prebuilt demo scenarios (`Import → Calculate → Export`)
- 🩺 **Self-check widget** – database, backup, and Git gatekeeper status
- 🧰 **Diagnostics Export** – creates a ZIP with logs, manifest, and environment info
- 🔒 **Read-only sandbox** – prevents destructive or real-world data writes
- 🌙 **Dark/Light themes** with toggle and logo adaptation

### 🖥️ Visual & UX Design
Professional fintech-grade frontend designed for investor confidence:

- **Dark theme (default):** deep navy, teal, and violet glow scheme  
- **Light theme:** clean, white-fintech minimalism  
- **Animated dashboard:** teal glow trails, smooth transitions, hover depth  
- **Context tooltips** explaining each KPI  
- **Glass panels** with rounded corners and subtle glow feedback  
- **Fully responsive layout** (desktop-first optimized for 1080p+)

### 📦 Executable Demo Builder
Generate a full offline `.exe` showcase:
```bash
POST /build/demo-exe
✅ Auto-packs app + DB + assets
✅ Verifies smoke tests
✅ Bundles version, build date, and commit info
✅ One verified EXE kept under /artifacts/demo/CryptoTaxCalc_Demo.exe

🗂️ Project Layout
bash
Kopiraj kod
CryptoTaxCalc/
├─ src/cryptotaxcalc/
│  ├─ app.py              # FastAPI app (core routes)
│  ├─ demo_mode.py        # Investor Demo Mode system (UI + API)
│  ├─ exporter.py         # Smart backup & diagnostics bundle
│  ├─ calc_runner.py      # FIFO calculation pipeline
│  ├─ csv_normalizer.py   # Transaction import / normalization
│  ├─ models.py, schemas.py
│  ├─ rules/
│  │   ├─ hr.py           # Croatia tax logic
│  │   └─ it.py           # Italy tax logic
│  ├─ demo_assets.py      # Demo data seeding & environment tools
│  └─ audit_digest.py     # Run manifests, audit summaries
│
├─ logo/                  # Brand assets (white/black icons)
├─ static/                # Web assets (theme.css, glow.js, favicon)
├─ samples/               # CSV demo inputs
├─ logs/                  # App, audit, and demo logs
├─ tests/                 # Smoke tests
├─ artifacts/             # Build outputs (demo EXE, bundles)
├─ docs/                  # Terms, Privacy, policies
└─ README.md
🚀 Quick Start
1️⃣ Setup environment
powershell
Kopiraj kod
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -r requirements.txt
2️⃣ Run development server
powershell
Kopiraj kod
uvicorn cryptotaxcalc.app:app --reload --host 127.0.0.1 --port 8000
Then open:
➡️ http://127.0.0.1:8000 — Main landing page
➡️ http://127.0.0.1:8000/demo/dashboard — Demo Mode dashboard
➡️ http://127.0.0.1:8000/docs — API docs

🧭 Demo Mode Usage
Reset Demo Environment
Restores the local SQLite DB and sample data.

Upload Sample CSVs
Import samples/sample.csv and samples/sample2.csv.

Run Calculation
Choose jurisdiction (Croatia 🇭🇷 or Italy 🇮🇹).
Results show gains, cost, and tax summary.

Export Diagnostics
Downloads a .zip with logs, configs, and manifest.

Switch Theme
Use top-right toggle for Light/Dark visual modes.

🧰 Diagnostics & Support Bundle
Create a full support bundle (for developer handover or debugging):

bash
Kopiraj kod
python -m cryptotaxcalc.exporter --support
Contents include:

Full source (src/)

Logs and audit history

Config manifests

Samples and static assets

Build metadata and environment snapshot

🧪 Testing
Run smoke tests locally:

powershell
Kopiraj kod
pytest -q -m smoke
🧾 Compliance & Legal
Local-first data handling — no cloud sync by default.

Data sandboxed; demo uses synthetic non-identifiable data.

Includes /docs/Privacy_and_Data_Processing.md and /docs/Terms_of_Use.md.

All reports labeled:

“Informational only — not tax advice.”

🧠 Visual Style Summary
Element	Dark Theme	Light Theme
Background	#0A0F1C	#F9FAFC
Primary Accent	#0CE6C8	#0CCAB4
Secondary Accent	#8F6FFF	#7C5CFF
Typography	White / Gray	Charcoal / Graphite
Feedback Glow	Teal / Violet	Teal shadow
Emotion	Trust & Intelligence	Clarity & Confidence

🧩 Build Info
Displayed on dashboard footer:

yaml
Kopiraj kod
Version: dev | Commit: local | Mode: Demo | Build: verified

Read from demo_build_manifest.json (written by the demo builder).
The demo builder uses environment variables:
- APP_VERSION (defaults to "dev")
- GIT_COMMIT (defaults to "local")

🔒 Security Notes
No secrets in demo build.

Use .env.example as a template; never ship real secrets in demo/production builds.

Sandboxed SQLite DB prevents external writes.

Telemetry optional, off by default.

📦 License
MIT (or your chosen license)

🙌 Acknowledgments
Thanks to all contributors and testers helping evolve
CryptoTaxCalc into a complete local-first investor demo system
combining professional UX, safe offline execution, and smart automation.

🧠 TL;DR
CryptoTaxCalc = professional-grade crypto tax calculator
with demo mode, smart backup, beautiful UI, and complete local control.

yaml
Kopiraj kod

---

Would you like me to:
- add **screenshot placeholders** (e.g., `![Dashboard Preview](static/demo_dashboard_dark.png)`)  
or  
- generate a **short summary section** for `support_bundle` usage at the top (developer handover paragraph)?





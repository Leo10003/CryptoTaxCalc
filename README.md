# 🧮 CryptoTaxCalc

[![COLLAB Rules](https://img.shields.io/badge/Collab_Rules-Active-brightgreen)](./COLLAB_RULES.md) you

A precision-built, fully auditable cryptocurrency tax calculator following the **COLLAB_RULES.md** collaboration framework.  
All files, modules, and endpoints are organized for clarity, reproducibility, and long-term maintainability.

---

## 🚀 Overview

CryptoTaxCalc automates the process of:
- Parsing and normalizing CSV transaction data  
- Converting between currencies using **ECB FX rates**  
- Calculating **FIFO-based** capital gains  
- Generating per-year and per-asset **summary reports**  
- Producing **audit digests** for validation  
- Running **nightly tests** and FX updates automatically  

Every workflow is verified by smoke tests and versioned database migrations.

---

## 🧩 Repository Structure

CryptoTaxCalc/
├─ src/
│ └─ cryptotaxcalc/
│ ├─ api/ # REST endpoints (FastAPI)
│ ├─ core/ # Core engines: FIFO, FX, Audit
│ ├─ models/ # ORM models
│ ├─ utils/ # Helper utilities
│ ├─ init.py
│ └─ app.py # Main FastAPI app
│
├─ automation/ # PowerShell scripts & schedulers
│ ├─ nightly_fx_task.xml
│ ├─ nightly_smoke_task.xml
│ ├─ update_fx.ps1
│ ├─ run_smoke_and_email.py
│ └─ collect_support_bundle.ps1
│
├─ support_bundles/ # Automatically generated bundles (.zip)
│
├─ fx_ecb.csv # Historical ECB FX data
├─ .env # Environment configuration
├─ smoke_test.py # Lightweight integrity test
├─ COLLAB_RULES.md # Collaboration standards & structure rules
└─ README.md # This file

---

## ⚙️ Setup & Run

### 1️⃣ Create & Activate a Virtual Environment
```bash
python -m venv .venv
.\.venv\Scripts\activate
2️⃣ Install Dependencies
bash
Kopiraj kod
pip install -r requirements.txt
3️⃣ Start the API
bash
Kopiraj kod
uvicorn cryptotaxcalc.app:app --reload --app-dir .\src
Then open http://127.0.0.1:8000/docs

🧪 Run the Smoke Test
To verify everything works correctly:

bash
Kopiraj kod
.\.venv\Scripts\python.exe .\smoke_test.py
This will:

Check /health

Fetch transactions

Generate a yearly summary

If all responses are 200 OK, your environment is good to go ✅

🕗 Scheduled Automation
Nightly Tasks
Task	Description
update_fx.ps1	Auto-fetch ECB FX rates daily
nightly_smoke_task.xml	Run smoke tests nightly
collect_support_bundle.ps1	Create zip bundle for diagnostics

All tasks are safe to re-run (idempotent) and designed for unattended operation.

🧰 Support Bundle Endpoint
To manually generate a diagnostic zip:

nginx
Kopiraj kod
POST http://127.0.0.1:8000/admin/bundle?token=12345
Output:

bash
Kopiraj kod
support_bundles/support_bundle_YYYY-MM-DD_HH-MM-SS.zip
🪄 Development Principles
This project strictly follows the COLLAB_RULES.md specification:

No guesswork in code or imports

Clear file structure & naming consistency

Deterministic results, UTC timestamps

Safe migrations, auditable calculations

Automated tests and reproducibility

🪪 License
Private internal use only.
Redistribution, publication, or resale without written authorization is prohibited.

Built with ❤️ and precision — following COLLAB_RULES.md
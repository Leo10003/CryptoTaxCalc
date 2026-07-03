CryptoTaxCalc — Privacy & Data Processing (Local-First)
Last updated: 2025-12-21

1. Summary (plain language)

CryptoTaxCalc is designed to run locally on your machine. By default, your transaction data is stored and processed locally and is not sent to a third-party cloud service.

If you choose to export files (PDF/CSV) or generate a diagnostics/support bundle, those exported files may contain sensitive financial data. You control if/when you share them.

2. Data we process

Depending on how you use the product, CryptoTaxCalc may process:

Imported CSV transaction data (timestamps, assets, amounts, exchange labels, optional notes/memos).

Normalized transaction records stored in a local database.

Calculation outputs (run summaries, realized event rows, tax breakdowns, warnings).

Exported artifacts you request (PDF reports, CSV exports).

Local logs created for troubleshooting and auditability.

3. Where data is stored

Data is stored locally on your machine in project/application folders. Typical storage includes:

A local SQLite database (transactions and calculation runs).

Local folders for uploaded raw CSVs and normalized data (if enabled in your build).

Local logs (application logs, audit logs).

Exported PDFs/CSVs and optional diagnostics/support bundles.

4. Networking and external services

By default, CryptoTaxCalc does not require a cloud backend. However, some optional workflows may involve network access:

FX rate updates may download public FX reference files (e.g., ECB EUR/USD history) and import them locally.

Developer workflows may include optional Trello/Telegram integrations. These are intended for development/operations and should be disabled in customer builds unless explicitly needed.

5. Support bundles and sharing

If you generate a diagnostics/support bundle, it may include:

Logs

Configuration metadata

Exported artifacts

Possibly database snapshots or sample data (depending on settings)

By default, CryptoTaxCalc support bundles are designed to be safer to share:
- Secrets are redacted (for example tokens/keys).
- Raw storage/backups are excluded unless explicitly enabled when generating the bundle.

Before sharing a bundle, review it and remove/redact any information you do not want to disclose. Do not share secrets (tokens, API keys, passwords).

6. Retention and deletion

Because data is stored locally, you can delete it by removing the local database file and related storage directories created by the app. Exported artifacts (PDF/CSV/bundles) are also stored locally and must be deleted separately if you want them removed.

7. Security practices (recommended)

Do not commit .env files or tokens to version control.

Use strong OS-level security (disk encryption, login password).

Treat exported bundles as sensitive documents.

8. Disclaimer

This document describes typical behavior for local-first CryptoTaxCalc builds. If you distribute a modified build (e.g., server-hosted, cloud-synced), you must update this policy accordingly.

9. Contact

For questions about privacy and data handling in your specific build/distribution, contact the distributor or maintainer of your CryptoTaxCalc build.
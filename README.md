# DQ-Flow: Automated Data Quality & Governance Engine

DQ-Flow is a lightweight, production-style data quality framework that validates transactional data, detects anomalies, and generates an audit trail for compliance and reporting.

## Why this exists
In regulated environments (finance, trading, credit risk, etc.), "bad data" cannot be allowed to flow into reporting, dashboards, or regulatory submissions. DQ-Flow acts as a gate: it scans incoming data, flags issues, and produces a traceable, auditable record of data quality.

This is the type of control that risk, compliance, audit, and data governance teams expect in mature data orgs.

## Key capabilities
- **Data ingestion and normalization**  
  Loads raw transaction data and FX reference data, normalizes types, parses timestamps, and standardizes currency codes.

- **Deterministic data quality checks**  
  Runs rule-based validation such as:
  - `amount_positive`: amount must be > 0
  - `valid_timestamp`: timestamps must parse
  - `currency_supported`: currency must exist in approved FX table
  - `fx_mapped`: all foreign currency trades must have an FX mapping
  - `no_null_trade_id`: trade IDs cannot be missing

- **Automated anomaly / outlier surfacing (extensible)**  
  Framework supports adding statistical or ML-driven anomaly checks (e.g. IsolationForest, z-score) for suspicious spikes.

- **Audit logging & governance trail**  
  Every pipeline run is written to a local SQLite database (`dq_audit.db`). For each check, the system records:
  - run_id (timestamped batch ID)
  - which rule ran
  - status (PASS/FAIL)
  - number of impacted rows
  - sample IDs of bad records
  - UTC timestamp

  This simulates the type of evidence compliance and audit teams ask for during reviews.

- **Human-readable & machine-readable reporting**  
  Each run generates a JSON report in `reports/` with:
  - total rows scanned
  - list of all checks
  - failed row counts
  - severity levels
  - generation timestamp

## High-level flow
1. Ingest raw data from `data/transactions_raw.csv` and FX mappings from `data/fx_rates.csv`.
2. Normalize and standardize the data (`dq_flow/ingest.py`).
3. Run all validation checks (`dq_flow/validators.py`).
4. Generate a structured data quality report (`dq_flow/runner.py` → `reports/`).
5. Persist the full audit trail to SQLite for traceability (`dq_flow/db.py`).

## Repo structure
```text
dq-flow/
├── dq_flow/
│   ├── __init__.py
│   ├── ingest.py         # data loading + normalization
│   ├── validators.py     # data quality rules
│   ├── anomaly.py        # placeholder for advanced anomaly detection
│   ├── db.py             # audit log persistence (SQLite)
│   └── runner.py         # pipeline orchestrator
├── data/
│   ├── transactions_raw.csv
│   └── fx_rates.csv
├── reports/
│   └── dq_report_<timestamp>.json
├── requirements.txt
├── .gitignore
└── README.md


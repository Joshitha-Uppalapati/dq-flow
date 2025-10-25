import os
import sys
from datetime import datetime
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.getcwd())


def get_engine(db_path: str = "dq_audit.db"):
    uri = f"sqlite:///{db_path}"
    return create_engine(uri, echo=False, future=True)


def init_schema(engine):
    create_stmt = """
    CREATE TABLE IF NOT EXISTS dq_audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        check_name TEXT,
        status TEXT,
        severity TEXT,
        failed_rows INTEGER,
        sample_ids TEXT,
        created_at_utc TEXT
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(create_stmt)


def persist_results(engine, run_id: str, results: List[Dict]):
    now_utc = datetime.utcnow().isoformat()
    rows = []
    for r in results:
        rows.append(
            {
                "run_id": run_id,
                "check_name": r["check_name"],
                "status": r["status"],
                "severity": r["severity"],
                "failed_rows": r["failed_rows"],
                "sample_ids": ",".join(r["sample_ids"]),
                "created_at_utc": now_utc,
            }
        )

    df = pd.DataFrame(rows)
    with engine.begin() as conn:
        df.to_sql("dq_audit_log", conn, if_exists="append", index=False)


def fetch_audit_history(engine, limit: int = 20):
    query = text(
        "SELECT run_id, check_name, status, severity, failed_rows, sample_ids, created_at_utc "
        "FROM dq_audit_log "
        "ORDER BY created_at_utc DESC "
        "LIMIT :lim"
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"lim": limit}).mappings().all()
        return list(rows)


if __name__ == "__main__":
    eng = get_engine()
    init_schema(eng)
    print(fetch_audit_history(eng))


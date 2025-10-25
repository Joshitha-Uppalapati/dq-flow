import json
import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())

from dq_flow.ingest import load_all
from dq_flow.validators import run_all_checks
from dq_flow.db import get_engine, init_schema, persist_results, fetch_audit_history



def main():
    tx_df, fx_df = load_all("data/transactions_raw.csv", "data/fx_rates.csv")
    results = run_all_checks(tx_df, fx_df)

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/dq_report_{run_id}.json"

    os.makedirs("reports", exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(
            {
                "run_id": run_id,
                "scanned_rows": len(tx_df),
                "checks": results,
                "generated_at_utc": datetime.utcnow().isoformat(),
            },
            f,
            indent=2,
        )
    eng = get_engine()
    init_schema(eng)
    persist_results(eng, run_id, results)

    print(f"Data Quality Report generated: {report_path}")
    for r in results:
        print(
            f"- {r['check_name']}: {r['status']} "
            f"(failed_rows={r['failed_rows']}, severity={r['severity']})"
        )

    history = fetch_audit_history(eng, limit=5)
    print("\nRecent audit history:")
    for row in history:
        print(
            f"[{row['created_at_utc']}] run_id={row['run_id']} "
            f"{row['check_name']} -> {row['status']} "
            f"(failed_rows={row['failed_rows']}, severity={row['severity']})"
        )
if __name__ == "__main__":
    main()


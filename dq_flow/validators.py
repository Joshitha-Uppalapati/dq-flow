import pandas as pd
from typing import List, Dict


def _result_template(check_name: str,
                     failed_rows: int,
                     status: str,
                     severity: str,
                     sample_ids: List[str]) -> Dict:
    return {
        "check_name": check_name,
        "failed_rows": failed_rows,
        "status": status,
        "severity": severity,
        "sample_ids": sample_ids,
    }


def check_no_null_trade_id(df: pd.DataFrame) -> Dict:
    mask = df["trade_id"].isna() | (df["trade_id"].astype(str).str.strip() == "")
    failed = df[mask]
    status = "PASS" if failed.empty else "FAIL"
    return _result_template(
        check_name="no_null_trade_id",
        failed_rows=len(failed),
        status=status,
        severity="HIGH",
        sample_ids=failed["trade_id"].astype(str).head(5).tolist()
    )


def check_amount_positive(df: pd.DataFrame) -> Dict:
    mask = (df["amount"].isna()) | (df["amount"] <= 0)
    failed = df[mask]
    status = "PASS" if failed.empty else "FAIL"
    return _result_template(
        check_name="amount_positive",
        failed_rows=len(failed),
        status=status,
        severity="HIGH",
        sample_ids=failed["trade_id"].astype(str).head(5).tolist()
    )


def check_valid_timestamp(df: pd.DataFrame) -> Dict:
    mask = df["timestamp"].isna()
    failed = df[mask]
    status = "PASS" if failed.empty else "FAIL"
    return _result_template(
        check_name="valid_timestamp",
        failed_rows=len(failed),
        status=status,
        severity="MEDIUM",
        sample_ids=failed["trade_id"].astype(str).head(5).tolist()
    )


def check_currency_supported(df: pd.DataFrame, fx: pd.DataFrame) -> Dict:
    valid_currencies = set(fx["currency"].unique())
    mask = ~df["currency"].isin(valid_currencies)
    failed = df[mask]
    status = "PASS" if failed.empty else "FAIL"
    return _result_template(
        check_name="currency_supported",
        failed_rows=len(failed),
        status=status,
        severity="HIGH",
        sample_ids=failed["trade_id"].astype(str).head(5).tolist()
    )


def check_fx_mapped(df: pd.DataFrame, fx: pd.DataFrame) -> Dict:
    merged = df.merge(fx, on="currency", how="left", indicator=True)
    mask = merged["_merge"] == "left_only"
    failed = merged[mask]
    status = "PASS" if failed.empty else "FAIL"
    return _result_template(
        check_name="fx_mapped",
        failed_rows=len(failed),
        status=status,
        severity="HIGH",
        sample_ids=failed["trade_id"].astype(str).head(5).tolist()
    )


def run_all_checks(df: pd.DataFrame, fx: pd.DataFrame) -> List[Dict]:
    results = []
    results.append(check_no_null_trade_id(df))
    results.append(check_amount_positive(df))
    results.append(check_valid_timestamp(df))
    results.append(check_currency_supported(df, fx))
    results.append(check_fx_mapped(df, fx))
    return results

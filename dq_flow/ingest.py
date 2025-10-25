import pandas as pd
from datetime import datetime
from typing import Tuple

def _parse_timestamp_safe(ts: str):
    try:
        return pd.to_datetime(ts)
    except Exception:
        return pd.NaT

def load_transactions(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    expected_cols = ["trade_id", "account_id", "currency", "amount", "timestamp"]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in transactions: {missing_cols}")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["timestamp"] = df["timestamp"].astype(str).apply(_parse_timestamp_safe)
    df["currency"] = df["currency"].astype(str).str.upper().str.strip()
    return df

def load_fx_rates(path: str) -> pd.DataFrame:
    fx = pd.read_csv(path)
    expected_cols = ["currency", "usd_rate"]
    missing_cols = [c for c in expected_cols if c not in fx.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in fx_rates: {missing_cols}")
    fx["currency"] = fx["currency"].astype(str).str.upper().str.strip()
    fx["usd_rate"] = pd.to_numeric(fx["usd_rate"], errors="coerce")
    return fx

def load_all(transactions_path: str, fx_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    tx = load_transactions(transactions_path)
    fx = load_fx_rates(fx_path)
    return tx, fx

if __name__ == "__main__":
    tx_df, fx_df = load_all("data/transactions_raw.csv", "data/fx_rates.csv")
    print("=== TRANSACTIONS SAMPLE ===")
    print(tx_df.head())
    print("\n=== FX RATES SAMPLE ===")
    print(fx_df.head())

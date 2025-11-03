# src/analytics_project/data_preparation/prepare_customers_data.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW_DIR = Path("data/raw")
PREP_DIR = Path("data/prepared")
RAW_FILE = RAW_DIR / "customers_data.csv"
OUT_FILE = PREP_DIR / "customers_data_prepared.csv"

# Your real headers (exactly as in the CSV)
KEEP_COLS = [
    "CustomerID",
    "Name",
    "Region",
    "JoinDate",
    "LoyaltyPoints",
    "CustomerSegment",
]


def ensure_dirs():
    PREP_DIR.mkdir(parents=True, exist_ok=True)


def read_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_FILE)
    # keep only known columns (ignore any extras safely)
    df = df[[c for c in KEEP_COLS if c in df.columns]].copy()
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # CustomerID numeric (nullable ok), LoyaltyPoints numeric
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
    df["LoyaltyPoints"] = pd.to_numeric(df["LoyaltyPoints"], errors="coerce")
    # Parse dates; invalid -> NaT
    if "JoinDate" in df.columns:
        df["JoinDate"] = pd.to_datetime(df["JoinDate"], errors="coerce")
    return df


def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows without a CustomerID (business key)
    df = df.dropna(subset=["CustomerID"])
    # Fill reasonable defaults
    if "Name" in df.columns:
        df["Name"] = df["Name"].replace({"": np.nan}).fillna("Unknown")
    if "Region" in df.columns:
        df["Region"] = df["Region"].replace({"": np.nan}).fillna("Unknown")
    if "CustomerSegment" in df.columns:
        df["CustomerSegment"] = (
            df["CustomerSegment"].astype(str).replace({"": np.nan}).fillna("Unknown").str.title()
        )
    if "LoyaltyPoints" in df.columns:
        med = df["LoyaltyPoints"].median(skipna=True)
        df["LoyaltyPoints"] = df["LoyaltyPoints"].fillna(0 if np.isnan(med) else med)
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    # De-dup by CustomerID, keep first
    return df.drop_duplicates(subset=["CustomerID"], keep="first")


def remove_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    # Remove extreme LoyaltyPoints using IQR
    if "LoyaltyPoints" in df.columns and df["LoyaltyPoints"].notna().any():
        q1 = df["LoyaltyPoints"].quantile(0.25)
        q3 = df["LoyaltyPoints"].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df[(df["LoyaltyPoints"] >= lower) & (df["LoyaltyPoints"] <= upper)]
    return df


def main():
    ensure_dirs()
    raw = read_raw()
    raw_count = len(raw)

    df = trim_strings(raw)
    df = coerce_types(df)
    df = handle_missing(df)
    df = drop_duplicates(df)
    df = remove_outliers_iqr(df)

    prepped_count = len(df)
    df.to_csv(OUT_FILE, index=False)

    print("✅ Customers cleaned and saved")
    print(f"Raw count:      {raw_count}")
    print(f"Prepared count: {prepped_count}")
    print(f"Output:         {OUT_FILE}")


if __name__ == "__main__":
    main()

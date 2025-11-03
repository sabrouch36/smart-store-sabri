# src/analytics_project/data_preparation/prepare_sales_data.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW_DIR = Path("data/raw")
PREP_DIR = Path("data/prepared")
RAW_FILE = RAW_DIR / "sales_data.csv"
OUT_FILE = PREP_DIR / "sales_data_prepared.csv"

# Keep common sales fields if they exist; ignore extras safely.
LIKELY_COLS = [
    "SaleID",
    "CustomerID",
    "ProductID",
    "Amount",
    # optional new fields you added in D3.1 (any names are OK; we handle both):
    "DiscountPercent_num",
    "DiscountPercent",
    "PaymentType_cat",
    "PaymentType",
    "State",
]


def ensure_dirs():
    PREP_DIR.mkdir(parents=True, exist_ok=True)


def read_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_FILE)
    df.columns = [c.strip() for c in df.columns]
    keep = [c for c in LIKELY_COLS if c in df.columns]
    if keep:
        df = df[keep].copy()
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    # discount may be named DiscountPercent_num or DiscountPercent
    for col in ["DiscountPercent_num", "DiscountPercent"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # IDs numeric if present
    for col in ["SaleID", "CustomerID", "ProductID"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    # require SaleID if present; otherwise require at least Amount
    required = [c for c in ["SaleID"] if c in df.columns] or [
        c for c in ["Amount"] if c in df.columns
    ]
    if required:
        df = df.dropna(subset=required)
    # fill text fields
    for c in ["PaymentType_cat", "PaymentType", "State"]:
        if c in df.columns:
            df[c] = df[c].replace({"": np.nan}).fillna("Unknown")
    # fill numeric fields with median/0
    for c in ["Amount", "DiscountPercent_num", "DiscountPercent"]:
        if c in df.columns:
            med = df[c].median(skipna=True)
            df[c] = df[c].fillna(0 if np.isnan(med) else med)
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    if "SaleID" in df.columns:
        return df.drop_duplicates(subset=["SaleID"], keep="first")
    return df.drop_duplicates(keep="first")


def remove_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Amount", "DiscountPercent_num", "DiscountPercent"]:
        if col in df.columns and df[col].notna().any():
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lo = q1 - 1.5 * iqr
            hi = q3 + 1.5 * iqr
            df = df[(df[col] >= lo) & (df[col] <= hi)]
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

    print("✅ Sales cleaned and saved")
    print(f"Raw count:      {raw_count}")
    print(f"Prepared count: {prepped_count}")
    print(f"Output:         {OUT_FILE}")


if __name__ == "__main__":
    main()

# src/analytics_project/data_preparation/prepare_products_data.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW_DIR = Path("data/raw")
PREP_DIR = Path("data/prepared")
RAW_FILE = RAW_DIR / "products_data.csv"
OUT_FILE = PREP_DIR / "products_data_prepared.csv"

# Use only the real headers you have (as seen in your CSV):
# ProductID, ProductName, Category, UnitPrice, stock quantity , supplier
KEEP_COLS = [
    "ProductID",
    "ProductName",
    "Category",
    "UnitPrice",
    "stock quantity",
    "supplier",
]


def ensure_dirs():
    PREP_DIR.mkdir(parents=True, exist_ok=True)


def read_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_FILE)
    # normalize header whitespace (e.g., "stock quantity ")
    df.columns = [c.strip() for c in df.columns]
    # keep only known columns
    df = df[[c for c in KEEP_COLS if c in df.columns]].copy()
    return df


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # Numeric coercion
    if "UnitPrice" in df.columns:
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")
    if "stock quantity" in df.columns:
        df["stock quantity"] = pd.to_numeric(df["stock quantity"], errors="coerce")
    return df


def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows without a ProductID
    if "ProductID" in df.columns:
        df = df.dropna(subset=["ProductID"])
    # Reasonable defaults
    for c in ["ProductName", "Category", "supplier"]:
        if c in df.columns:
            df[c] = df[c].replace({"": np.nan}).fillna("Unknown")
    for c in ["UnitPrice", "stock quantity"]:
        if c in df.columns:
            med = df[c].median(skipna=True)
            df[c] = df[c].fillna(0 if np.isnan(med) else med)
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    if "ProductID" in df.columns:
        return df.drop_duplicates(subset=["ProductID"], keep="first")
    return df.drop_duplicates(keep="first")


def remove_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    # Remove extreme values for UnitPrice and stock quantity when present
    for col in ["UnitPrice", "stock quantity"]:
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

    print("✅ Products cleaned and saved")
    print(f"Raw count:      {raw_count}")
    print(f"Prepared count: {prepped_count}")
    print(f"Output:         {OUT_FILE}")


if __name__ == "__main__":
    main()

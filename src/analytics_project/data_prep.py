"""Project 3 — Unified Data Preparation Pipeline.

Run:
    uv run python -m analytics_project.data_prep
"""

from __future__ import annotations

# ----------------------------
# Imports
# ----------------------------
import pathlib
from collections.abc import Iterable
from typing import TYPE_CHECKING

import pandas as pd

from analytics_project.utils_logger import init_logger, logger, project_root
from utils.data_scrubber import DataScrubber

# ----------------------------
# Paths
# ----------------------------
DATA_DIR: pathlib.Path = project_root / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DIR: pathlib.Path = DATA_DIR / "prepared"
PREPARED_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# IO helper
# ----------------------------
def read_and_log(path: pathlib.Path) -> pd.DataFrame:
    """Read CSV with friendly logging; on failure return empty DataFrame."""
    try:
        logger.info(f"Reading raw data from {path}.")
        df = pd.read_csv(path)
        logger.info(
            f"{path.name}: loaded DataFrame with shape {df.shape[0]} rows x {df.shape[1]} cols"
        )
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return pd.DataFrame()


# ----------------------------
# Cleaning helpers (small, reusable)
# ----------------------------
def format_if_present(
    scrubber: DataScrubber, df: pd.DataFrame, columns: Iterable[str]
) -> pd.DataFrame:
    """Lower+trim string columns if they exist."""
    for col in columns:
        if col in df.columns:
            scrubber.format_column_strings_to_lower_and_trim(col)
            logger.info(f"Formatted text column: {col}")
    return df


def format_first_available(
    scrubber: DataScrubber, df: pd.DataFrame, candidates: list[str]
) -> pd.DataFrame:
    """Pick the first existing column from candidates and format it."""
    for cand in candidates:
        if cand in df.columns:
            df = scrubber.format_column_strings_to_lower_and_trim(cand)
            logger.info(f"Formatted text column: {cand}")
            break
    return df


def remove_outliers_iqr(df: pd.DataFrame, preferred_cols: list[str] | None = None) -> pd.DataFrame:
    work = df.copy()
    numeric_cols = [c for c in work.columns if pd.api.types.is_numeric_dtype(work[c])]
    cols = [c for c in (preferred_cols or []) if c in numeric_cols] or numeric_cols

    for col in cols:
        q1 = work[col].quantile(0.25)
        q3 = work[col].quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        before = len(work)
        work = work[(work[col] >= lower) & (work[col] <= upper)]
        removed = before - len(work)
        if removed:
            logger.info(f"[outliers] {col}: kept [{lower:.2f}, {upper:.2f}] | removed {removed}")
    return work


def parse_first_datetime(
    scrubber: DataScrubber, df: pd.DataFrame, candidates: list[str]
) -> pd.DataFrame:
    """Try parsing the first available datetime column from candidates."""
    for cand in candidates:
        if cand in df.columns:
            try:
                df = scrubber.parse_dates_to_add_standard_datetime(cand)
                logger.info(f"Parsed datetime column: {cand} -> StandardDateTime")
                break
            except Exception as e:
                logger.info(f"Skip datetime parse for {cand}: {e}")
    return df


# ----------------------------
# Pipelines per table
# ----------------------------
def process_customers() -> None:
    path = RAW_DATA_DIR / "customers_data.csv"
    df = read_and_log(path)
    if df.empty:
        return

    # Clean headers
    df.columns = df.columns.str.strip()

    scrubber = DataScrubber(df)
    df = scrubber.remove_duplicate_records()
    df = scrubber.handle_missing_data(fill_value="Unknown")

    # Normalize key text fields
    df = format_first_available(scrubber, df, ["CustomerName", "Customer Name", "Name", "FullName"])
    df = format_if_present(scrubber, df, ["City", "State", "Country"])

    # Optional outlier cleanup if present
    df = remove_outliers_iqr(df, preferred_cols=["Age", "AnnualIncome"])

    # Save
    out = PREPARED_DIR / "customers_prepared.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved cleaned customers data to {out}")


def process_products() -> None:
    path = RAW_DATA_DIR / "products_data.csv"
    df = read_and_log(path)
    if df.empty:
        return

    df.columns = df.columns.str.strip()

    scrubber = DataScrubber(df)
    df = scrubber.remove_duplicate_records()
    df = scrubber.handle_missing_data(fill_value="Unknown")

    # Normalize text fields
    df = format_first_available(scrubber, df, ["ProductName", "Product Name", "Name"])
    df = format_if_present(scrubber, df, ["Category"])

    # Optional numeric outliers
    df = remove_outliers_iqr(df, preferred_cols=["Price", "UnitPrice", "Cost"])

    out = PREPARED_DIR / "products_prepared.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved cleaned products data to {out}")


def process_sales() -> None:
    path = RAW_DATA_DIR / "sales_data.csv"
    df = read_and_log(path)
    if df.empty:
        return

    df.columns = df.columns.str.strip()

    scrubber = DataScrubber(df)
    df = scrubber.remove_duplicate_records()
    df = scrubber.handle_missing_data(fill_value="Unknown")

    # Try to standardize date
    df = parse_first_datetime(scrubber, df, ["OrderDate", "order_date", "Date"])

    # Optional numeric outliers
    df = remove_outliers_iqr(df, preferred_cols=["Quantity", "Amount", "Total", "Sales"])

    out = PREPARED_DIR / "sales_prepared.csv"
    df.to_csv(out, index=False)
    logger.info(f"Saved cleaned sales data to {out}")


# ----------------------------
# Main / entrypoint
# ----------------------------
def main() -> None:
    logger.info("Starting data preparation...")
    process_customers()
    process_products()
    process_sales()
    logger.info("Data preparation complete.")


if __name__ == "__main__":
    init_logger()
    main()

"""
scripts/data_preparation/prepare_customers.py

This script reads customer data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting
"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
from __future__ import annotations
import pathlib
import sys
import logging

# Import from external packages (requires a virtual environment)
import pandas as pd
import numpy as np

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# ---------- Logger setup ----------
# Try to use utils.logger if available; otherwise use stdlib logging.
try:
    from utils.logger import logger  # type: ignore
except Exception:
    logger = logging.getLogger("prepare_customers")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(_h)

# ---------- Optional DataScrubber ----------
# Provide a minimal fallback so script always runs.
try:
    from utils.data_scrubber import DataScrubber  # type: ignore
except Exception:

    class DataScrubber:
        def __init__(self, df: pd.DataFrame) -> None:
            self.df = df

        def remove_duplicate_records(self, subset: list[str] | None = None) -> pd.DataFrame:
            if subset is None and "CustomerID" in self.df.columns:
                subset = ["CustomerID"]
            return self.df.drop_duplicates(subset=subset, keep="first")


#####################################
# Constants / Paths
#####################################

SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of this script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent

# IMPORTANT: your repo has /data at the project root (one level above /src)
DATA_DIR: pathlib.Path = PROJECT_ROOT.parent / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data

# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

# Your real CSV headers (from your file):
# CustomerID, Name, Region, JoinDate, LoyaltyPoints, CustomerSegment
KEEP_COLS = ["CustomerID", "Name", "Region", "JoinDate", "LoyaltyPoints", "CustomerSegment"]


#####################################
# Define Functions - Reusable blocks
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"READING: {file_path}.")
        df = pd.read_csv(file_path)
        # Keep only known columns if present (ignore extras safely)
        present = [c for c in KEEP_COLS if c in df.columns]
        if present:
            df = df[present].copy()
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame based on a business key.
    For customers, we use CustomerID if present.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    scrubber = DataScrubber(df)
    df_deduped = scrubber.remove_duplicate_records(
        subset=["CustomerID"] if "CustomerID" in df.columns else None
    )
    logger.info(f"Original dataframe shape: {df.shape}")
    logger.info(f"Deduped  dataframe shape: {df_deduped.shape}")
    return df_deduped


def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure consistent string formatting: strip leading/trailing spaces and collapse internal whitespace.
    """
    logger.info("FUNCTION START: trim_strings")
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns to appropriate types (numeric/date).
    """
    logger.info("FUNCTION START: coerce_types")
    if "CustomerID" in df.columns:
        df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
    if "LoyaltyPoints" in df.columns:
        df["LoyaltyPoints"] = pd.to_numeric(df["LoyaltyPoints"], errors="coerce")
    if "JoinDate" in df.columns:
        df["JoinDate"] = pd.to_datetime(df["JoinDate"], errors="coerce")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping (data-specific policy).
    - Drop rows with missing CustomerID (business key).
    - Fill Name/Region with 'Unknown'.
    - Normalize CustomerSegment and fill missing with 'Unknown'.
    - Fill LoyaltyPoints with median (or 0 if median is NaN).
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")

    if "CustomerID" in df.columns:
        df = df.dropna(subset=["CustomerID"])

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

    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers using the IQR rule on numeric columns that matter for customers.
    Here we apply it to LoyaltyPoints when present.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    if "LoyaltyPoints" in df.columns and df["LoyaltyPoints"].notna().any():
        q1 = df["LoyaltyPoints"].quantile(0.25)
        q3 = df["LoyaltyPoints"].quantile(0.75)
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        df = df[(df["LoyaltyPoints"] >= lower) & (df["LoyaltyPoints"] <= upper)]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


#####################################
# Define Main Function - Entry point
#####################################


def main() -> None:
    """
    Main function for processing customer data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_customers.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "customers_data.csv"
    # Keep naming consistent with your repo (e.g., customers_data_prepared.csv)
    output_file = "customers_data_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)
    if df.empty:
        logger.error("Raw DataFrame is empty. Aborting.")
        return

    # Record original shape
    original_shape = df.shape
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape  : {df.shape}")

    # Clean columns (strip)
    df = trim_strings(df)

    # Coerce types
    df = coerce_types(df)

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    cleaned_shape = df.shape
    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape : {cleaned_shape}")
    logger.info("FINISHED prepare_customers.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
#####################################

if __name__ == "__main__":
    main()

"""
scripts/data_preparation/prepare_products.py

This script reads data from the data/raw folder, cleans the data,
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
import pathlib
import sys
import logging

# Import from external packages (requires a virtual environment)
import pandas as pd
import numpy as np

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
try:
    from utils.logger import logger  # type: ignore
except Exception:
    logger = logging.getLogger("prepare_products")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(_h)

# Optional: Use a data_scrubber module for common data cleaning tasks
try:
    from utils.data_scrubber import DataScrubber  # type: ignore
except Exception:

    class DataScrubber:
        def __init__(self, df: pd.DataFrame) -> None:
            self.df = df

        def remove_duplicate_records(self, subset=None) -> pd.DataFrame:
            return self.df.drop_duplicates(subset=subset, keep="first")


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent
# IMPORTANT: your repo's /data lives one level ABOVE /src
DATA_DIR: pathlib.Path = PROJECT_ROOT.parent / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data

# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")

    # Normalize header whitespace ONLY (preserve original names/case)
    df.columns = [c.strip() for c in df.columns]
    return df


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
    Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)

    # Prefer ProductID as business key when available
    subset = ["ProductID"] if "ProductID" in df.columns else None
    df = DataScrubber(df).remove_duplicate_records(subset=subset)

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values by column before handling
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")

    # Drop rows without ProductID if present
    if "ProductID" in df.columns:
        df = df.dropna(subset=["ProductID"])

    # Fill text columns with 'Unknown'
    for c in ["ProductName", "Category", "supplier"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"": np.nan}).fillna("Unknown")

    # Coerce numeric columns and fill with median (or 0 if median is NaN)
    for c in ["UnitPrice", "stock quantity"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            med = df[c].median(skipna=True)
            df[c] = df[c].fillna(0 if np.isnan(med) else med)

    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds (IQR) for key numeric fields.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    for col in ["UnitPrice", "stock quantity"]:
        if col in df.columns and df[col].notna().any():
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            before = len(df)
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            logger.info(
                f"Applied outlier removal to {col}: bounds [{lower_bound:.4g}, {upper_bound:.4g}], removed {before - len(df)} rows"
            )

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the formatting of various columns (light-touch).
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    # Keep original header case; just trim strings and polish values
    # Title-case product names, lower-case categories, round prices
    if "ProductName" in df.columns:
        df["ProductName"] = (
            df["ProductName"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        )
    if "Category" in df.columns:
        df["Category"] = df["Category"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    if "supplier" in df.columns:
        df["supplier"] = df["supplier"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    if "UnitPrice" in df.columns:
        df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").round(2)

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against business rules.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")

    # Example validations
    if "UnitPrice" in df.columns:
        neg = (df["UnitPrice"] < 0).sum()
        if neg:
            logger.info(f"Found {neg} products with negative UnitPrice; removing")
            df = df[df["UnitPrice"] >= 0]

    if "stock quantity" in df.columns:
        neg_sq = (df["stock quantity"] < 0).sum()
        if neg_sq:
            logger.info(f"Found {neg_sq} rows with negative stock quantity; clamping to 0")
            df.loc[df["stock quantity"] < 0, "stock quantity"] = 0

    return df


def main() -> None:
    """
    Main function for processing product data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_products.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    # Keep naming consistent with your other prepared files
    output_file = "products_data_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names (only strip whitespace; do NOT force lowercase/underscores)
    original_columns = df.columns.tolist()
    df.columns = [c.strip() for c in df.columns]
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Validate data
    df = validate_data(df)

    # Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    cleaned_shape = df.shape
    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {cleaned_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_products.py")
    logger.info("==================================")


# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()

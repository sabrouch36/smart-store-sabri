"""
scripts/data_preparation/prepare_sales.py

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

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger

# Optional: Use a data_scrubber module for common data cleaning tasks
from utils.data_scrubber import DataScrubber


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
SRC_DIR: pathlib.Path = SCRIPTS_DIR.parent
PROJECT_ROOT: pathlib.Path = SRC_DIR.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
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

    # --- Basic data profiling for understanding the dataset ---
    logger.info("DATA PROFILING START")
    logger.info(f"Column datatypes:\n{df.dtypes}")
    logger.info(f"Number of unique values per column:\n{df.nunique()}")
    logger.info(f"Missing values per column:\n{df.isna().sum()}")
    logger.info("DATA PROFILING END")

    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """
    Main function for processing data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    before = len(df)
    key_cols = [c for c in ["TransactionID"] if c in df.columns]
    if key_cols:
        logger.info(f"De-duplicating using key(s): {key_cols}")
        df = df.drop_duplicates(subset=key_cols, keep="first")
    else:
        logger.warning("No TransactionID column found; using full-row de-duplication.")
        df = df.drop_duplicates(keep="first")
    logger.info(f"Removed {before - len(df)} duplicate rows")

    # Handle missing values
    logger.info("Handling missing values...")
    logger.info(f"Missing values BEFORE:\n{df.isna().sum()}")

    if "TransactionID" in df.columns:
        before = len(df)
        df = df.dropna(subset=["TransactionID"])
        logger.info(f"Dropped {before - len(df)} rows missing TransactionID")

    for key in ["CustomerID", "ProductID"]:
        if key in df.columns:
            before = len(df)
            df = df.dropna(subset=[key])
            logger.info(f"Dropped {before - len(df)} rows missing {key}")

    if "SaleDate" in df.columns:
        df["SaleDate"] = pd.to_datetime(df["SaleDate"], errors="coerce")
        before = len(df)
        df = df.dropna(subset=["SaleDate"])
        logger.info(f"Dropped {before - len(df)} rows with invalid SaleDate")

    for col in ["SaleAmount", "UnitPrice", "Quantity"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            median_val = df[col].median()
            df[col].fillna(median_val, inplace=True)
            logger.info(f"Filled {col} NaNs with median {median_val}")

    if "DiscountPercent" in df.columns:
        df["DiscountPercent"] = pd.to_numeric(df["DiscountPercent"], errors="coerce").fillna(0)

    for col in ["PaymentType", "payment_type"]:
        if col in df.columns:
            m = df[col].mode()
            if not m.empty:
                df[col].fillna(m[0], inplace=True)
                logger.info(f"Filled {col} NaNs with mode '{m[0]}'")

    logger.info(f"Missing values AFTER:\n{df.isna().sum()}")

    # Remove outliers
    logger.info("Removing outliers...")
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        logger.warning("No numeric columns found for outlier removal.")
    else:
        for col in numeric_cols:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            before = len(df)
            df = df[(df[col] >= lower) & (df[col] <= upper)]
            removed = before - len(df)
            logger.info(f"[outliers] {col}: kept [{lower:.2f}, {upper:.2f}] | removed {removed}")

    logger.info(f"Final dataframe shape after outlier removal: {df.shape}")
    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    file_path = PREPARED_DATA_DIR / file_name
    logger.info(f"Saving cleaned data to {file_path}")
    df.to_csv(file_path, index=False)


if __name__ == "__main__":
    main()

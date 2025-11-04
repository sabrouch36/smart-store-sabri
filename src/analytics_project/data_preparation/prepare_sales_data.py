"""
scripts/data_preparation/prepare_sales_data.py

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
from typing import Iterable

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
try:
    from utils.logger import logger  # type: ignore
except Exception:
    try:
        sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))  # add <root>/src
        from analytics_project.utils_logger import logger  # type: ignore
    except Exception as e:
        raise ImportError(
            "Could not import logger from utils.logger or analytics_project.utils_logger"
        ) from e

# Optional: data scrubber (shim if missing)
try:
    from utils.data_scrubber import DataScrubber  # type: ignore
except Exception:

    class DataScrubber:
        @staticmethod
        def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            df.columns = [c.strip() for c in df.columns]
            return df

        @staticmethod
        def fill_missing(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            str_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
            for c in num_cols:
                if df[c].isna().any():
                    df[c] = df[c].fillna(df[c].median())
            for c in str_cols:
                if df[c].isna().any():
                    df[c] = df[c].fillna("")
            return df

        @staticmethod
        def remove_outliers_iqr(df: pd.DataFrame, factor: float = 1.5) -> pd.DataFrame:
            df = df.copy()
            num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            if not num_cols:
                return df
            mask = pd.Series(True, index=df.index)
            for c in num_cols:
                q1 = df[c].quantile(0.25)
                q3 = df[c].quantile(0.75)
                iqr = q3 - q1
                if pd.isna(iqr) or iqr == 0:
                    continue
                low = q1 - factor * iqr
                high = q3 + factor * iqr
                mask &= df[c].between(low, high) | df[c].isna()
            return df[mask]


# ── Constants (FIX: compute real project root) ───────────────────────────────
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # .../src/analytics_project/data_preparation
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent  # .../src/analytics_project
PROJECT_ROOT: pathlib.Path = (
    pathlib.Path(__file__).resolve().parents[3]
)  # <project_root>  ✅ 3 levels up
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"

# Ensure the directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

#####################################
# Functions
#####################################


def _string_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV located in <project_root>/data/raw/.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR / file_name
    logger.info(f"Reading data from {file_path}")
    if not file_path.exists():
        existing = [p.name for p in RAW_DATA_DIR.glob("*.csv")]
        raise FileNotFoundError(f"{file_path} not found. CSVs in {RAW_DATA_DIR}: {existing}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")
    return df


#####################################
# Main
#####################################


def main() -> None:
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_data_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Initial info
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    logger.info(f"Removed duplicates: {before - len(df)} rows")

    # Handle missing values
    df = DataScrubber.fill_missing(df)
    logger.info("Handled missing values (numeric→median, strings→empty)")

    # Remove outliers (IQR on numeric columns)
    before_out = len(df)
    df = DataScrubber.remove_outliers_iqr(df, factor=1.5)
    logger.info(f"Removed outliers (IQR): {before_out - len(df)} rows")

    # Normalize string columns
    for c in _string_cols(df):
        df[c] = df[c].astype("string").str.strip()

    # Save prepared data
    out_path = PREPARED_DATA_DIR / output_file
    df.to_csv(out_path, index=False)
    logger.info(f"Saved prepared data to: {out_path}")

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
#####################################

if __name__ == "__main__":
    main()

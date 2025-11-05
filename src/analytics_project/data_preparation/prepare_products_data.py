"""Scripts/data_preparation/prepare_products.py.

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
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)

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

    # --- Identify and remove duplicates based on product identifiers ---
    candidate_keys = ["product_id", "product_code", "sku"]
    subset = [c for c in candidate_keys if c in df.columns]

    if subset:
        logger.info(f"De-duplicating using key(s): {subset}")
        df = df.drop_duplicates(subset=subset, keep="first")
    else:
        logger.warning("No product key column found; using full-row de-duplication.")
        df = df.drop_duplicates(keep="first")

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values by column before handling
    # NA means missing or "not a number" - ask your AI for details
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")

    # --- Handle missing values based on product-related rules ---
    if "product_name" in df.columns:
        df["product_name"].fillna("Unknown Product", inplace=True)

    if "description" in df.columns:
        df["description"].fillna("", inplace=True)

    if "price" in df.columns:
        median_price = df["price"].median()
        df["price"].fillna(median_price, inplace=True)
        logger.info(f"Filled missing prices with median value: {median_price}")

    if "category" in df.columns:
        mode_category = (
            df["category"].mode()[0] if not df["category"].mode().empty else "unspecified"
        )
        df["category"].fillna(mode_category, inplace=True)
        logger.info(f"Filled missing categories with mode value: {mode_category}")

    if "product_code" in df.columns:
        before = len(df)
        df.dropna(subset=["product_code"], inplace=True)
        logger.info(f"Dropped {before - len(df)} rows missing product_code")
    # --- End: business rules for missing values ---

    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    # --- Outlier removal using IQR on key numeric columns ---
    preferred = ["price", "weight", "length", "width", "height"]
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cols_to_check = [c for c in preferred if c in numeric_cols] or numeric_cols

    for col in cols_to_check:
        if df[col].dropna().nunique() <= 1:
            logger.info(f"[outliers] Skipped {col}: not enough variance")
            continue
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        before = len(df)
        df = df[(df[col].isna()) | ((df[col] >= lower) & (df[col] <= upper))]
        logger.info(
            f"[outliers] {col}: bounds [{lower:.4g}, {upper:.4g}] | removed {before - len(df)}"
        )

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the formatting of various columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with standardized formatting.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    # --- Standardize text and numeric formats for product data ---
    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].astype(str).str.title()

    if "category" in df.columns:
        df["category"] = df["category"].astype(str).str.lower()

    if "price" in df.columns and pd.api.types.is_numeric_dtype(df["price"]):
        df["price"] = df["price"].round(2)

    if "weight_unit" in df.columns:
        df["weight_unit"] = df["weight_unit"].astype(str).str.upper()

    if "brand" in df.columns:
        df["brand"] = df["brand"].astype(str).str.strip().str.title()

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Validated DataFrame.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")

    # --- Validation rules for product data ---
    # 1) Ensure product_code exists (critical identifier)
    if "product_code" in df.columns:
        before = len(df)
        df = df.dropna(subset=["product_code"])
        dropped_missing_code = before - len(df)
        if dropped_missing_code:
            logger.warning(f"Dropped {dropped_missing_code} rows missing product_code")

        # Enforce uniqueness of product_code
        before = len(df)
        df = df.drop_duplicates(subset=["product_code"], keep="first")
        dropped_dups = before - len(df)
        if dropped_dups:
            logger.warning(f"Removed {dropped_dups} duplicate product_code rows")

    # 2) Prices must be >= 0
    if "price" in df.columns and pd.api.types.is_numeric_dtype(df["price"]):
        neg_prices = (df["price"] < 0).sum()
        if neg_prices:
            logger.warning(f"Found {neg_prices} products with negative price; removing them")
            df = df[df["price"] >= 0]

        # Optional sanity flag: unusually large prices (manual review)
        very_large = (df["price"] > 1e5).sum()
        if very_large:
            logger.warning(
                f"{very_large} products have price > 100,000 (check if this is expected)"
            )

    # 3) Non-negative numeric fields for physical measures
    for col in ["weight", "length", "width", "height"]:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            invalid = (df[col] < 0).sum()
            if invalid:
                logger.warning(f"Found {invalid} rows with negative {col}; removing them")
                df = df[df[col] >= 0]

    logger.info("Data validation complete")
    return df


def main() -> None:
    """Process product data."""
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    try:
        # Read raw data
        df = read_raw_data(input_file)

        # Record original shape
        original_shape = df.shape

        # Log initial dataframe information
        logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
        logger.info(f"Initial dataframe shape: {df.shape}")

        # Clean column names
        original_columns = df.columns.tolist()
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        changed_columns = [
            f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
        ]
        if changed_columns:
            logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

        # Apply cleaning steps
        df = remove_duplicates(df)
        df = handle_missing_values(df)
        df = remove_outliers(df)
        logger.info("Validating data...")
        df = validate_data(df)
        logger.info("Standardizing formats...")
        df = standardize_formats(df)

        # Save prepared data
        save_prepared_data(df, output_file)

        logger.info("==================================")
        logger.info(f"Original shape: {original_shape}")
        logger.info(f"Cleaned shape:  {df.shape}")
        logger.info("FINISHED prepare_products_data.py")
        logger.info("==================================")

    except Exception as exc:
        logger.exception("An error occurred while preparing product data: %s", exc)


if __name__ == "__main__":
    main()

"""
Prepare customer value dataset for Custom BI Project (P7).

Reads from SQLite DW (smart_sales.db) and creates:
data/prepared/customer_value.csv
"""

from pathlib import Path
import sqlite3
import pandas as pd


# --------------------------------------------------------------------
# 1. Paths & DB connection
# --------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DW_PATH = PROJECT_ROOT / "data" / "dw" / "smart_sales.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "prepared"
OUTPUT_PATH = OUTPUT_DIR / "customer_value.csv"

print(f"Using database: {DW_PATH}")

conn = sqlite3.connect(DW_PATH)

# --------------------------------------------------------------------
# 2. Load tables from DW
# --------------------------------------------------------------------
sale_df = pd.read_sql_query("SELECT * FROM sale;", conn)
customer_df = pd.read_sql_query("SELECT * FROM customer;", conn)
product_df = pd.read_sql_query("SELECT * FROM product;", conn)

print("Data loaded successfully from smart_sales.db")
print("sale_df shape:", sale_df.shape)
print("customer_df shape:", customer_df.shape)
print("product_df shape:", product_df.shape)
print("\nSample from sale_df:")
print(sale_df.head())
print("\nSample from customer_df:")
print(customer_df.head())
print("\nSample from product_df:")
print(product_df.head())

# --------------------------------------------------------------------
# 3. Basic cleaning & type conversion
# --------------------------------------------------------------------
# Ensure numeric
sale_df["sale_amount"] = pd.to_numeric(sale_df["sale_amount"], errors="coerce")

# Convert dates
sale_df["sale_date"] = pd.to_datetime(sale_df["sale_date"], errors="coerce")
customer_df["join_date"] = pd.to_datetime(customer_df["join_date"], errors="coerce")

# Drop obvious bad rows (no customer, product, or sale_date)
sale_df = sale_df.dropna(subset=["customer_id", "product_id", "sale_date"])

# --------------------------------------------------------------------
# 4. Join sale + customer (+ product if needed)
# --------------------------------------------------------------------
merged = sale_df.merge(customer_df, on="customer_id", how="left")
merged = merged.merge(product_df, on="product_id", how="left")

print(f"\nMerged shape: {merged.shape}")
print(merged.head())

# --------------------------------------------------------------------
# 5. Build customer value table (group by customer)
# --------------------------------------------------------------------
agg_df = (
    merged.groupby("customer_id")
    .agg(
        total_spend=("sale_amount", "sum"),
        transactions=("sale_id", "count"),
        first_purchase=("sale_date", "min"),
        last_purchase=("sale_date", "max"),
    )
    .reset_index()
)

# Bring back name / region / join_date
agg_df = agg_df.merge(
    customer_df[["customer_id", "name", "region", "join_date"]],
    on="customer_id",
    how="left",
)

# Average order value
agg_df["AOV"] = agg_df["total_spend"] / agg_df["transactions"]

# --------------------------------------------------------------------
# --------------------------------------------------------------------
ref_date = merged["sale_date"].max()
agg_df["tenure_days"] = (ref_date - agg_df["join_date"]).dt.days
agg_df["tenure_years"] = agg_df["tenure_days"] / 365.0


# --------------------------------------------------------------------
# --------------------------------------------------------------------
def segment_customer(tenure_years: float) -> str:
    """Return customer segment label based on tenure in years."""
    if pd.isna(tenure_years):
        return "Unknown"
    if tenure_years < 1:
        return "New (<1y)"
    elif tenure_years < 3:
        return "Active (1-3y)"
    else:
        return "Loyal (3y+)"


agg_df["segment"] = agg_df["tenure_years"].apply(segment_customer)

# --------------------------------------------------------------------
# --------------------------------------------------------------------
cols_order = [
    "customer_id",
    "name",
    "region",
    "segment",
    "total_spend",
    "transactions",
    "AOV",
    "first_purchase",
    "last_purchase",
    "join_date",
    "tenure_days",
    "tenure_years",
]

customer_value_df = agg_df[cols_order].sort_values(by="total_spend", ascending=False)

print("\nCustomer Value Dataset:")
print(customer_value_df.head())
print(f"\n[rows x cols] = {customer_value_df.shape}")

# Ensure output dir exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
customer_value_df.to_csv(OUTPUT_PATH, index=False)

print(f"\nSaved prepared customer value file to: {OUTPUT_PATH}")

# Close connection
conn.close()

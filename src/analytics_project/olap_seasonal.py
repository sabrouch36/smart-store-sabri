import sqlite3
import pandas as pd

# --- Step 3.1: Connect to the data warehouse and load tables ---

DB_PATH = "data/dw/smart_sales.db"

# Create a connection to the SQLite data warehouse
conn = sqlite3.connect(DB_PATH)

# Load the fact and dimension tables into pandas DataFrames
sale_df = pd.read_sql("SELECT * FROM sale;", conn)
customer_df = pd.read_sql("SELECT * FROM customer;", conn)
product_df = pd.read_sql("SELECT * FROM product;", conn)

# Optional: print shapes to verify
print("Data loaded successfully from smart_sales.db")
print("sale_df shape:", sale_df.shape)
print("customer_df shape:", customer_df.shape)
print("product_df shape:", product_df.shape)

# Close the connection when done (good practice)
conn.close()


# --- Step 3.2: Join sale with customer and product tables ---

# Join sale with customer table
merged = sale_df.merge(customer_df, on="customer_id", how="left")

# Join the result with product table
merged = merged.merge(product_df, on="product_id", how="left")

print("After joins, merged shape:", merged.shape)
print(merged.head())

# --- Step 3.3: Clean date, extract year/month, and select needed columns ---

# Convert sale_date into datetime (invalid dates become NaT)
merged["sale_date"] = pd.to_datetime(merged["sale_date"], errors="coerce")

# Remove rows with invalid dates
merged = merged.dropna(subset=["sale_date"])

# Extract year and month
merged["year"] = merged["sale_date"].dt.year
merged["month"] = merged["sale_date"].dt.month

# Select only needed columns for OLAP
data = merged[["year", "month", "category", "region", "sale_amount", "sale_id"]]

print("After cleaning & selecting columns, shape:", data.shape)
print(data.head())

conn.close()


# --- Step 3.4: Aggregate data to build the OLAP cube ---

cube = data.groupby(["year", "month", "category", "region"], as_index=False).agg(
    total_sales=("sale_amount", "sum"),
    transactions=("sale_id", "count"),
)

# Average Order Value
cube["AOV"] = cube["total_sales"] / cube["transactions"]

print("OLAP cube shape:", cube.shape)
print(cube.head())

# --- Step 3.5: OLAP Slicing, Dicing, and Drilldown ---

print("\n--- Slicing Example: Only year 2025 ---")
slice_2025 = cube[cube["year"] == 2025]
print(slice_2025.head())

print("\n--- Dicing Example: Category = clothing AND Region = North ---")
dice_example = cube[(cube["category"] == "clothing") & (cube["region"].str.lower() == "north")]
print(dice_example)

print("\n--- Drilldown Example: Aggregating yearly → then monthly ---")
# First: yearly total
yearly = cube.groupby("year", as_index=False).agg(total_sales=("total_sales", "sum"))
print("Yearly totals:")
print(yearly)

# Then: drilldown to month per year
monthly = cube.groupby(["year", "month"], as_index=False).agg(total_sales=("total_sales", "sum"))
print("\nMonthly totals:")
print(monthly.head())


# --- Step 3.6: Prepare data for visualizations ---

# 1) Monthly trend (line chart)
monthly_trends = cube.groupby(["month", "category"], as_index=False).agg(
    total_sales=("total_sales", "sum")
)

print("\nMonthly trends (for line chart):")
print(monthly_trends.head())


# 2) Heatmap data (month × category)
heatmap_data = cube.pivot_table(
    index="month", columns="category", values="total_sales", aggfunc="sum"
).fillna(0)

print("\nHeatmap data (pivot table):")
print(heatmap_data.head())


# 3) Regional comparison (bar chart)
region_sales = cube.groupby("region", as_index=False).agg(total_sales=("total_sales", "sum"))

print("\nRegional comparison (for bar chart):")
print(region_sales)


# --- Task 4: Visualization 1 — Line Chart (Monthly Trends) ---
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))

for category in monthly_trends["category"].unique():
    subset = monthly_trends[monthly_trends["category"] == category]
    plt.plot(subset["month"], subset["total_sales"], marker="o", label=category)

plt.title("Seasonal Sales Trends by Month and Category")
plt.xlabel("Month")
plt.ylabel("Total Sales (USD)")
plt.xticks(range(1, 13))
plt.legend(title="Category")
plt.grid(True)

plt.tight_layout()
plt.show()


# --- Task 4: Visualization 2 — Heatmap (Month × Category) ---
import seaborn as sns

plt.figure(figsize=(8, 6))
sns.heatmap(heatmap_data, annot=True, cmap="YlGnBu", fmt=".0f")

plt.title("Sales Heatmap by Month and Category")
plt.xlabel("Category")
plt.ylabel("Month")

plt.tight_layout()
plt.show()

# --- Task 4: Visualization 3 — Bar Chart (Region Sales) ---

plt.figure(figsize=(10, 6))

plt.bar(region_sales["region"], region_sales["total_sales"], color="lightblue")

plt.title("Total Sales by Region")
plt.xlabel("Region")
plt.ylabel("Total Sales (USD)")

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

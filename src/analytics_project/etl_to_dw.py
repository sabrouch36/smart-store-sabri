# src/analytics_project/etl_to_dw.py
from __future__ import annotations
import sqlite3
import pathlib
import sys
import pandas as pd

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent  # src/analytics_project/..
DATA_DIR = PROJECT_ROOT.parent / "data"  
PREPARED_DATA_DIR = DATA_DIR / "prepared"
DW_DIR = DATA_DIR / "dw"
DB_PATH = DW_DIR / "smart_sales.db"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))


def create_schema(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INTEGER PRIMARY KEY,
            name       TEXT,
            region     TEXT,
            join_date  TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product (
            product_id   INTEGER PRIMARY KEY,
            product_name TEXT,
            category     TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sale (
            sale_id     INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            product_id  INTEGER NOT NULL,
            sale_amount REAL    NOT NULL,
            sale_date   TEXT,
            FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
            FOREIGN KEY (product_id)  REFERENCES product(product_id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_customer ON sale(customer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_product  ON sale(product_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_date     ON sale(sale_date)")


def clear_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("DELETE FROM sale")
    cur.execute("DELETE FROM product")
    cur.execute("DELETE FROM customer")


def drop_dupes(df: pd.DataFrame, key: str) -> pd.DataFrame:
    df = df.drop_duplicates(subset=[key], keep="first").copy()
    return df


def prep_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "CustomerID": "customer_id",
            "Name": "name",
            "Region": "region",
            "JoinDate": "join_date",
        }
    )
    df = df[["customer_id", "name", "region", "join_date"]].copy()
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["customer_id"])
    df = drop_dupes(df, "customer_id")
    return df


def prep_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "ProductID": "product_id",
            "ProductName": "product_name",
            "Category": "category",
        }
    )
    df = df[["product_id", "product_name", "category"]].copy()
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["product_id"])
    df = drop_dupes(df, "product_id")
    return df


def prep_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "TransactionID": "sale_id",
            "SaleDate": "sale_date",
            "CustomerID": "customer_id",
            "ProductID": "product_id",
            "SaleAmount": "sale_amount",
        }
    )
    keep = ["sale_id", "customer_id", "product_id", "sale_amount", "sale_date"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[keep].copy()

    for c in ["sale_id", "customer_id", "product_id"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["sale_amount"] = pd.to_numeric(df["sale_amount"], errors="coerce").astype(float)

    if df["sale_id"].isna().any() or df["sale_id"].duplicated().any():
        df = df.reset_index(drop=True)
        df["sale_id"] = (df.index + 1).astype("Int64")

    df = df.dropna(subset=["customer_id", "product_id", "sale_amount"])
    return df


def insert_customers(df: pd.DataFrame, cur: sqlite3.Cursor) -> None:
    df.to_sql("customer", cur.connection, if_exists="append", index=False)


def insert_products(df: pd.DataFrame, cur: sqlite3.Cursor) -> None:
    df.to_sql("product", cur.connection, if_exists="append", index=False)


def insert_sales(df: pd.DataFrame, cur: sqlite3.Cursor) -> None:
    df.to_sql("sale", cur.connection, if_exists="append", index=False)


def load_data_to_db() -> None:
    DW_DIR.mkdir(parents=True, exist_ok=True)

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA foreign_keys = ON;")
        cur = conn.cursor()

        create_schema(cur)
        clear_tables(cur)

        customers_df = pd.read_csv(PREPARED_DATA_DIR / "customers_prepared.csv")
        products_df = pd.read_csv(PREPARED_DATA_DIR / "products_prepared.csv")
        sales_df = pd.read_csv(PREPARED_DATA_DIR / "sales_prepared.csv")

        customers_df = prep_customers(customers_df)
        products_df = prep_products(products_df)
        sales_df = prep_sales(sales_df)

        insert_customers(customers_df, cur)
        insert_products(products_df, cur)
        insert_sales(sales_df, cur)

        conn.commit()

        print("[VALIDATION] Row counts:")
        for r in cur.execute("""
            SELECT 'customer', COUNT(*) FROM customer
            UNION ALL
            SELECT 'product', COUNT(*) FROM product
            UNION ALL
            SELECT 'sale', COUNT(*) FROM sale;
        """):
            print(r)

        # FK check
        cur.execute("PRAGMA foreign_key_check;")
        issues = cur.fetchall()
        if issues:
            print("\n[VALIDATION] FK violations:")
            for i in issues:
                print(i)
        else:
            print("\n[VALIDATION] Foreign keys: OK ✅")

        print("\n[VALIDATION] Sample join:")
        for r in cur.execute("""
            SELECT s.sale_id, c.name, p.product_name, s.sale_amount, s.sale_date
            FROM sale s
            JOIN customer c USING (customer_id)
            JOIN product  p USING (product_id)
            ORDER BY s.sale_id
            LIMIT 10;
        """):
            print(r)

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    load_data_to_db()

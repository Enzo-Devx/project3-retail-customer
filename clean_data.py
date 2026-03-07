# =============================================================================
# clean_data.py
# Project 03 — Retail Customer Analysis
# Purpose: Clean all 3 raw CSVs and load them into PostgreSQL (retaildb).
#          This is a full ETL pipeline:
#          Extract → raw CSVs
#          Transform → pandas cleaning
#          Load → PostgreSQL via SQLAlchemy
# =============================================================================

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import os

# =============================================================================
# DATABASE CONNECTION
# SQLAlchemy connection string format:
# postgresql://username:password@host:port/database
# =============================================================================

DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "database": "retaildb",
    "username": "postgres",
    "password": "postgres",
}

CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# --- PATHS ---
RAW_CUSTOMERS = "data/project03_customers_raw.csv"
RAW_PRODUCTS  = "data/project03_products_raw.csv"
RAW_ORDERS    = "data/project03_orders_raw.csv"
REPORT_PATH   = "outputs/cleaning_report.txt"

os.makedirs("outputs", exist_ok=True)

print("=" * 60)
print("  CLEAN_DATA.PY — ETL Pipeline")
print("  Extract → Transform → Load into PostgreSQL")
print("=" * 60)

report = []
report.append("=" * 60)
report.append("  CLEANING REPORT — Retail ETL Pipeline")
report.append(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
report.append("=" * 60)


# =============================================================================
# CONNECT TO POSTGRESQL
# =============================================================================

print("\n[DB] Connecting to PostgreSQL...")
try:
    engine = create_engine(CONNECTION_STRING)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        version = result.fetchone()[0]
        print(f"[DB] Connected! {version[:50]}...")
        report.append(f"\n[DB] Connected to PostgreSQL successfully.")
except Exception as e:
    print(f"[ERROR] Could not connect to PostgreSQL: {e}")
    print("        Check your connection details in DB_CONFIG.")
    exit(1)


# =============================================================================
# TABLE 1 — CLEAN CUSTOMERS
# =============================================================================

print("\n[ETL] Processing customers table...")

customers = pd.read_csv(RAW_CUSTOMERS, dtype=str)
customers.replace("", np.nan, inplace=True)

initial = len(customers)

# Drop blank rows
customers = customers[~customers.isnull().all(axis=1)].reset_index(drop=True)

# Drop duplicates
customers = customers.drop_duplicates().reset_index(drop=True)

# Parse signup_date
customers["signup_date"] = pd.to_datetime(
    customers["signup_date"], errors="coerce"
)

# Standardize tier — fix UNKNOWN to most common value
valid_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
customers["tier"] = customers["tier"].str.strip().str.title()
customers.loc[~customers["tier"].isin(valid_tiers), "tier"] = "Bronze"

# Fill missing region with "Unknown"
customers["region"] = customers["region"].fillna("Unknown")

# Fill missing phone with "N/A"
customers["phone"] = customers["phone"].fillna("N/A")

# Fill missing email
customers["email"] = customers["email"].fillna("no-email@unknown.com")

# Standardize channel and gender
customers["channel"] = customers["channel"].str.strip().str.title()
customers["gender"]  = customers["gender"].str.strip().str.title()

final_cust = len(customers)
print(f"   Raw: {initial} → Clean: {final_cust} rows")
report.append(f"\n[Customers] Raw: {initial} → Clean: {final_cust}")
report.append(f"            Removed: {initial - final_cust} rows")


# =============================================================================
# TABLE 2 — CLEAN PRODUCTS
# Already clean — just validate and cast types.
# =============================================================================

print("\n[ETL] Processing products table...")

products = pd.read_csv(RAW_PRODUCTS, dtype=str)
products.replace("", np.nan, inplace=True)
products = products[~products.isnull().all(axis=1)].reset_index(drop=True)
products["base_price"] = pd.to_numeric(products["base_price"], errors="coerce")

print(f"   {len(products)} products validated")
report.append(f"\n[Products] {len(products)} rows — clean reference table")


# =============================================================================
# TABLE 3 — CLEAN ORDERS
# Most complex table. Mixed dates, sentinel prices, nulls, dupes, blanks.
# =============================================================================

print("\n[ETL] Processing orders table...")

orders = pd.read_csv(RAW_ORDERS, dtype=str)
orders.replace("", np.nan, inplace=True)

initial_ord = len(orders)

# Drop blank rows
orders = orders[~orders.isnull().all(axis=1)].reset_index(drop=True)

# Drop duplicates
orders = orders.drop_duplicates().reset_index(drop=True)

# Parse mixed date formats
def parse_mixed_dates(series):
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]
    parsed  = pd.Series([pd.NaT] * len(series), dtype="datetime64[ns]")
    for fmt in formats:
        mask = parsed.isnull() & series.notnull()
        if mask.sum() == 0:
            break
        attempt = pd.to_datetime(series[mask], format=fmt, errors="coerce")
        parsed[mask] = attempt
    return parsed

orders["order_date"] = parse_mixed_dates(orders["order_date"])

# Cast numeric columns
orders["unit_price"] = pd.to_numeric(orders["unit_price"], errors="coerce")
orders["quantity"]   = pd.to_numeric(orders["quantity"],   errors="coerce")
orders["discount"]   = pd.to_numeric(orders["discount"],   errors="coerce").fillna(0.0)

# Fix sentinel values (-999 in unit_price)
sentinel_count = (orders["unit_price"] == -999).sum()
orders.loc[orders["unit_price"] == -999, "unit_price"] = np.nan

# Impute unit_price using median per product
product_median = orders.groupby("product_id")["unit_price"].transform("median")
orders["unit_price"] = orders["unit_price"].fillna(product_median)
orders["unit_price"] = orders["unit_price"].fillna(orders["unit_price"].median())

# Impute quantity with median
null_qty = orders["quantity"].isnull().sum()
orders["quantity"] = orders["quantity"].fillna(
    orders["quantity"].median()
).astype(int)

# Drop orders with missing customer_id — can't analyze without it
null_cust = orders["customer_id"].isnull().sum()
orders = orders[orders["customer_id"].notnull()].reset_index(drop=True)

# Drop orders with missing product_id
orders = orders[orders["product_id"].notnull()].reset_index(drop=True)

# Derive revenue column
orders["revenue"] = (
    orders["unit_price"] * orders["quantity"] * (1 - orders["discount"])
).round(2)

# Derive time columns
orders["order_month"]   = orders["order_date"].dt.month
orders["order_quarter"] = orders["order_date"].dt.quarter
orders["order_year"]    = orders["order_date"].dt.year

final_ord = len(orders)
print(f"   Raw: {initial_ord} → Clean: {final_ord} rows")
print(f"   Sentinel prices fixed: {sentinel_count}")
print(f"   Null quantities imputed: {null_qty}")
print(f"   Orders dropped (no customer): {null_cust}")

report.append(f"\n[Orders] Raw: {initial_ord} → Clean: {final_ord}")
report.append(f"         Sentinel prices: {sentinel_count}")
report.append(f"         Null quantities: {null_qty}")
report.append(f"         Dropped (no customer): {null_cust}")


# =============================================================================
# LOAD INTO POSTGRESQL
# if_exists="replace" → drops and recreates table every run
# This means the pipeline is fully reproducible — run it anytime,
# always get a fresh clean state in the database.
# =============================================================================

print("\n[LOAD] Loading clean tables into PostgreSQL (retaildb)...")

tables = [
    (customers, "customers", "customer_id"),
    (products,  "products",  "product_id"),
    (orders,    "orders",    "order_id"),
]

for df, table_name, pk in tables:
    try:
        df.to_sql(
            name      = table_name,
            con       = engine,
            schema    = "public",
            if_exists = "replace",   # drop + recreate = always fresh
            index     = False,
            chunksize = 500,         # memory safe for 8GB RAM
        )
        print(f"   ✓ {table_name:<12} → {len(df):>4} rows loaded")
        report.append(f"\n[Load] {table_name}: {len(df)} rows → PostgreSQL ✅")
    except Exception as e:
        print(f"   ✗ Failed to load {table_name}: {e}")
        report.append(f"\n[Load] {table_name}: FAILED — {e}")


# =============================================================================
# VERIFY LOAD — Query PostgreSQL to confirm tables exist and row counts match
# This is the professional step most juniors skip.
# Always verify your load. Never assume it worked.
# =============================================================================

print("\n[VERIFY] Confirming tables in PostgreSQL...")

with engine.connect() as conn:
    for _, table_name, _ in tables:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM public.{table_name}")
        )
        count = result.fetchone()[0]
        print(f"   ✓ public.{table_name:<12} → {count:>4} rows confirmed")
        report.append(f"[Verify] {table_name}: {count} rows in DB ✅")


# =============================================================================
# SAVE REPORT
# =============================================================================

summary = (
    f"\nETL SUMMARY"
    f"\n  customers : {len(customers)} rows loaded"
    f"\n  products  : {len(products)} rows loaded"
    f"\n  orders    : {len(orders)} rows loaded"
    f"\n  Database  : retaildb (localhost:5432)"
    f"\n  Status    : SUCCESS ✅"
)
report.append("\n" + "=" * 60)
report.append(summary)

with open(REPORT_PATH, "w", encoding="utf-8") as f:
    f.write("\n".join(report))

print("\n" + "=" * 60)
print(summary)
print(f"\n  Report → {REPORT_PATH}")
print("  Next   → Open DBeaver and verify your tables!")
print("  Then   → python analyze_data.py")
print("=" * 60)
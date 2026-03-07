# =============================================================================
# generate_data.py
# Project 03 — Retail Customer Analysis
# Purpose: Generate realistic fake data using Faker library.
#          Simulates extracting raw data from a production database.
#          This is your ETL source — after this, CSVs get loaded to PostgreSQL.
# =============================================================================

from faker import Faker
import random
import csv
import os
from datetime import datetime, timedelta

fake   = Faker("en_PH")   # Philippine locale for realistic names/addresses
random.seed(77)
Faker.seed(77)

os.makedirs("data", exist_ok=True)

print("=" * 60)
print("  GENERATE_DATA.PY — Faker-Powered Data Generation")
print("=" * 60)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(days=random.randint(0, (end - start).days))

def messy_date(dt: datetime) -> str:
    """Randomly format a date in one of 3 messy formats."""
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]
    return dt.strftime(random.choice(formats))


# =============================================================================
# TABLE 1 — CUSTOMERS (400 rows)
# Faker gives us: real Filipino names, cities, emails, phone numbers
# =============================================================================

print("\n[GEN] Generating customers table...")

regions  = ["NCR", "Cebu", "Davao", "Pampanga", "Laguna",
            "Batangas", "Iloilo", "Cagayan de Oro"]
channels = ["Online", "In-Store", "Both"]
tiers    = ["Bronze", "Silver", "Gold", "Platinum"]

signup_start = datetime(2022, 1, 1)
signup_end   = datetime(2023, 12, 31)

customers     = []
cust_headers  = [
    "customer_id", "full_name", "email",
    "phone", "gender", "region",
    "channel", "tier", "signup_date"
]

used_emails = set()

for i in range(400):
    cust_id    = f"CUST-{1001 + i}"
    full_name  = fake.name()
    gender     = random.choice(["Male", "Female"])
    region     = random.choice(regions)
    channel    = random.choice(channels)
    tier       = random.choices(tiers, weights=[40, 30, 20, 10])[0]
    signup     = random_date(signup_start, signup_end)

    # Generate unique email from name
    base_email = fake.email()
    while base_email in used_emails:
        base_email = fake.email()
    used_emails.add(base_email)

    phone = fake.numerify("+63 9## ### ####")

    # ── Inject dirty data ──────────────────────────────────
    # Missing region (4% chance)
    region = region if random.random() > 0.04 else ""
    # Unknown tier (3% chance)
    tier   = tier   if random.random() > 0.03 else "UNKNOWN"
    # Missing phone (5% chance)
    phone  = phone  if random.random() > 0.05 else ""
    # ───────────────────────────────────────────────────────

    customers.append([
        cust_id, full_name, base_email, phone,
        gender, region, channel, tier,
        signup.strftime("%Y-%m-%d")
    ])

# Inject blank rows
for idx in [50, 150, 280]:
    customers[idx] = [""] * len(cust_headers)

print(f"   ✓ {len(customers)} customer rows generated")


# =============================================================================
# TABLE 2 — PRODUCTS (25 rows)
# Clean reference table — no dirt. Real companies keep product catalogs clean.
# =============================================================================

print("\n[GEN] Generating products table...")

catalog = {
    "Electronics"  : [("Wireless Earbuds", 1299), ("Phone Case", 299),
                      ("Bluetooth Speaker", 1599), ("Power Bank", 899),
                      ("Smart Watch", 4999)],
    "Fashion"      : [("Running Shoes", 3200), ("Sunglasses", 799),
                      ("Backpack", 1750), ("Cap", 450),
                      ("Jacket", 2800)],
    "Home & Living": [("Coffee Maker", 2499), ("Desk Lamp", 899),
                      ("Water Bottle", 450), ("Yoga Mat", 599),
                      ("Air Purifier", 6999)],
    "Beauty"       : [("Skincare Set", 1899), ("Perfume", 2200),
                      ("Lip Kit", 599), ("Hair Serum", 899),
                      ("Sunscreen", 450)],
    "Sports"       : [("Resistance Bands", 799), ("Jump Rope", 350),
                      ("Gym Bag", 1599), ("Protein Shaker", 450),
                      ("Knee Brace", 899)],
}

products     = []
prod_headers = ["product_id", "product_name", "category", "base_price"]
prod_id      = 1

for cat, items in catalog.items():
    for name, price in items:
        products.append([f"PROD-{prod_id:03d}", name, cat, price])
        prod_id += 1

print(f"   ✓ {len(products)} product rows generated")


# =============================================================================
# TABLE 3 — ORDERS (1200 rows)
# Most complex table. Faker gives us realistic order patterns.
# Hidden inside: VIP customers (high frequency) and churned customers
# (stopped ordering after June 2023) for cohort analysis to detect.
# =============================================================================

print("\n[GEN] Generating orders table...")

order_start = datetime(2023, 1, 1)
order_end   = datetime(2024, 9, 30)
churn_cutoff= datetime(2023, 6, 30)

discounts   = [0, 0, 0, 0, 0.05, 0.10, 0.15, 0.20]
statuses    = ["Completed", "Completed", "Completed",
               "Returned", "Cancelled"]

# VIP customers — order frequently, high value
vip_custs   = random.sample(
    [f"CUST-{random.randint(1001,1200)}" for _ in range(50)], 30
)
# Churn customers — stop ordering after June 2023
churn_custs = random.sample(
    [f"CUST-{random.randint(1201,1400)}" for _ in range(70)], 50
)

orders       = []
order_headers= [
    "order_id", "order_date", "customer_id",
    "product_id", "quantity", "unit_price",
    "discount", "status"
]
order_num    = 10001

for i in range(1200):
    # Assign customer type
    roll = random.random()
    if roll < 0.25:
        cust_id    = random.choice(vip_custs)
        order_date = random_date(order_start, order_end)
    elif roll < 0.40:
        cust_id    = random.choice(churn_custs)
        order_date = random_date(order_start, churn_cutoff)
    else:
        cust_id    = f"CUST-{random.randint(1001, 1400)}"
        order_date = random_date(order_start, order_end)

    prod       = random.choice(products)
    base_price = prod[3]
    qty        = random.randint(1, 4)
    unit_price = round(base_price * random.uniform(0.95, 1.05), 2)
    discount   = random.choice(discounts)
    status     = random.choices(statuses, weights=[65,15,10,6,4])[0]

    # ── Inject dirty data ──────────────────────────────────
    unit_price = unit_price if random.random() > 0.05 else -999
    qty        = qty        if random.random() > 0.04 else ""
    cust_id    = cust_id    if random.random() > 0.03 else ""
    # ───────────────────────────────────────────────────────

    # Blank rows
    if i in [100, 300, 500, 700, 900]:
        orders.append([""] * len(order_headers))
        continue

    # Duplicate rows
    if i in [200, 400, 600] and orders:
        orders.append(orders[-1])
        continue

    orders.append([
        f"ORD-{order_num}",
        messy_date(order_date),
        cust_id,
        prod[0],
        qty,
        unit_price,
        discount,
        status,
    ])
    order_num += 1

print(f"   ✓ {len(orders)} order rows generated")


# =============================================================================
# SAVE ALL 3 TABLES TO CSV
# =============================================================================

print("\n[SAVE] Writing CSV files...")

tables = [
    ("project03_customers_raw.csv",  cust_headers,  customers),
    ("project03_products_raw.csv",   prod_headers,  products),
    ("project03_orders_raw.csv",     order_headers, orders),
]

for fname, headers, rows in tables:
    path = f"data/{fname}"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"   ✓ {fname} — {len(rows)} rows")

print("\n" + "=" * 60)
print("  DATA GENERATION COMPLETE")
print("  3 tables ready in data/ folder")
print("  Next step: python clean_data.py")
print("=" * 60)
# =============================================================================
# analyze_data.py
# Project 03 — Retail Customer Analysis
# Purpose: Connect to PostgreSQL, run all analysis queries programmatically,
#          save results as CSVs for visualization.
#          Answers all 4 of Jess's business questions.
# =============================================================================

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

# --- DATABASE CONFIG ---
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

os.makedirs("outputs", exist_ok=True)

print("=" * 60)
print("  ANALYZE_DATA.PY — Retail Customer Analysis")
print("  Querying PostgreSQL (retaildb)")
print("=" * 60)

report = []
report.append("=" * 60)
report.append("  ANALYSIS REPORT — Retail Customer Analysis")
report.append(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
report.append("=" * 60)

# =============================================================================
# CONNECT TO POSTGRESQL
# =============================================================================

print("\n[DB] Connecting to PostgreSQL...")
try:
    engine = create_engine(CONNECTION_STRING)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("[DB] Connected successfully.\n")
except Exception as e:
    print(f"[ERROR] Connection failed: {e}")
    exit(1)


def run_query(sql: str, label: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Prints status."""
    try:
        df = pd.read_sql(sql, engine)
        print(f"[Q] {label:<40} → {len(df)} rows")
        return df
    except Exception as e:
        print(f"[ERROR] {label}: {e}")
        return pd.DataFrame()


# =============================================================================
# QUESTION 1 — WHO ARE OUR BEST CUSTOMERS? (RFM Analysis)
# =============================================================================

print("[Q1] RFM Analysis — Best Customers...")

rfm_sql = """
WITH rfm_base AS (
    SELECT
        o.customer_id,
        c.full_name,
        c.tier,
        c.region,
        c.channel,
        c.email,
        CURRENT_DATE - MAX(o.order_date)::date    AS recency_days,
        COUNT(*)                                   AS frequency,
        ROUND(SUM(o.revenue)::numeric, 2)          AS monetary
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'Completed'
    GROUP BY
        o.customer_id, c.full_name, c.tier,
        c.region, c.channel, c.email
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency    DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary     DESC) AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT *,
        CONCAT(r_score, f_score, m_score)          AS rfm_code,
        ROUND((r_score + f_score + m_score)
            ::numeric / 3.0, 2)                    AS rfm_avg,
        CASE
            WHEN r_score >= 4
             AND f_score >= 4
             AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3
             AND f_score >= 3
             AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 3
             AND f_score <= 2 THEN 'Potential Loyalists'
            WHEN r_score <= 2
             AND f_score >= 3
             AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2
             AND f_score >= 4 THEN 'Cant Lose Them'
            WHEN r_score <= 1
             AND f_score <= 1 THEN 'Lost'
            ELSE 'Needs Attention'
        END                                        AS rfm_segment
    FROM rfm_scores
)
SELECT *
FROM rfm_segments
ORDER BY monetary DESC
"""

rfm_full = run_query(rfm_sql, "RFM full customer scores")
rfm_full.to_csv("outputs/rfm_full.csv", index=False)

# Segment summary
rfm_summary_sql = """
WITH rfm_base AS (
    SELECT
        o.customer_id,
        CURRENT_DATE - MAX(o.order_date)::date    AS recency_days,
        COUNT(*)                                   AS frequency,
        ROUND(SUM(o.revenue)::numeric, 2)          AS monetary
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'Completed'
    GROUP BY o.customer_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency    DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary     DESC) AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT *,
        CASE
            WHEN r_score >= 4
             AND f_score >= 4
             AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3
             AND f_score >= 3
             AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 3
             AND f_score <= 2 THEN 'Potential Loyalists'
            WHEN r_score <= 2
             AND f_score >= 3
             AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2
             AND f_score >= 4 THEN 'Cant Lose Them'
            WHEN r_score <= 1
             AND f_score <= 1 THEN 'Lost'
            ELSE 'Needs Attention'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT
    rfm_segment,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(recency_days))            AS avg_recency_days,
    ROUND(AVG(frequency)::numeric, 1)   AS avg_frequency,
    ROUND(AVG(monetary)::numeric, 2)    AS avg_monetary,
    ROUND(SUM(monetary)::numeric, 2)    AS total_revenue
FROM rfm_segments
GROUP BY rfm_segment
ORDER BY total_revenue DESC
"""

rfm_summary = run_query(rfm_summary_sql, "RFM segment summary")
rfm_summary.to_csv("outputs/rfm_summary.csv", index=False)

report.append("\n" + "=" * 60)
report.append("QUESTION 1 — RFM Segment Summary")
report.append("=" * 60 + "\n")
for _, row in rfm_summary.iterrows():
    report.append(
        f"  {row['rfm_segment']:<22} "
        f"Customers: {int(row['customer_count']):>3}  "
        f"Avg Spend: ₱{row['avg_monetary']:>10,.0f}  "
        f"Total Rev: ₱{row['total_revenue']:>12,.0f}"
    )


# =============================================================================
# QUESTION 2 — WHICH CATEGORIES DRIVE THE MOST VALUE?
# =============================================================================

print("\n[Q2] Category Revenue Analysis...")

category_sql = """
SELECT
    p.category,
    COUNT(DISTINCT o.customer_id)          AS unique_customers,
    COUNT(*)                               AS total_orders,
    ROUND(SUM(o.revenue)::numeric, 2)      AS total_revenue,
    ROUND(AVG(o.revenue)::numeric, 2)      AS avg_order_value,
    ROUND(AVG(o.discount)::numeric * 100, 1) AS avg_discount_pct
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.status = 'Completed'
GROUP BY p.category
ORDER BY total_revenue DESC
"""

category_rev = run_query(category_sql, "Category revenue breakdown")
category_rev.to_csv("outputs/category_revenue.csv", index=False)

# Top products per category
top_products_sql = """
SELECT
    p.category,
    p.product_name,
    COUNT(*)                               AS total_orders,
    ROUND(SUM(o.revenue)::numeric, 2)      AS total_revenue,
    ROUND(AVG(o.revenue)::numeric, 2)      AS avg_order_value
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.status = 'Completed'
GROUP BY p.category, p.product_name
ORDER BY total_revenue DESC
LIMIT 10
"""

top_products = run_query(top_products_sql, "Top 10 products by revenue")
top_products.to_csv("outputs/top_products.csv", index=False)

report.append("\n" + "=" * 60)
report.append("QUESTION 2 — Category Revenue")
report.append("=" * 60 + "\n")
for _, row in category_rev.iterrows():
    report.append(
        f"  {row['category']:<15} "
        f"Revenue: ₱{row['total_revenue']:>12,.0f}  "
        f"Orders: {int(row['total_orders']):>4}  "
        f"Avg Order: ₱{row['avg_order_value']:>8,.0f}"
    )


# =============================================================================
# QUESTION 3 — WHICH COHORTS ARE RETAINING WELL?
# =============================================================================

print("\n[Q3] Cohort Retention Analysis...")

cohort_sql = """
WITH first_orders AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM orders
    WHERE status = 'Completed'
    GROUP BY customer_id
),
order_months AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)    AS order_month,
        f.cohort_month
    FROM orders o
    JOIN first_orders f ON o.customer_id = f.customer_id
    WHERE o.status = 'Completed'
),
cohort_data AS (
    SELECT
        cohort_month,
        order_month,
        EXTRACT(YEAR  FROM AGE(order_month, cohort_month))
            * 12 +
        EXTRACT(MONTH FROM AGE(order_month, cohort_month))
                                             AS month_number,
        COUNT(DISTINCT customer_id)          AS active_customers
    FROM order_months
    GROUP BY cohort_month, order_month
),
cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id)          AS cohort_customers
    FROM first_orders
    GROUP BY cohort_month
)
SELECT
    TO_CHAR(cd.cohort_month, 'YYYY-MM')      AS cohort,
    cs.cohort_customers                       AS cohort_size,
    cd.month_number::int                      AS month_number,
    cd.active_customers                       AS active,
    ROUND(
        cd.active_customers * 100.0
        / cs.cohort_customers, 1
    )                                         AS retention_pct
FROM cohort_data cd
JOIN cohort_size cs
    ON cd.cohort_month = cs.cohort_month
WHERE cd.month_number <= 12
  AND cd.cohort_month >= '2023-01-01'
ORDER BY cd.cohort_month, cd.month_number
"""

cohort_data = run_query(cohort_sql, "Cohort retention data")
cohort_data.to_csv("outputs/cohort_retention.csv", index=False)

# Pivot for heatmap visualization
cohort_pivot = cohort_data.pivot_table(
    index="cohort",
    columns="month_number",
    values="retention_pct",
    aggfunc="first"
).reset_index()
cohort_pivot.to_csv("outputs/cohort_pivot.csv", index=False)

report.append("\n" + "=" * 60)
report.append("QUESTION 3 — Cohort Retention (Month 0 vs Month 3)")
report.append("=" * 60 + "\n")
for cohort in cohort_data["cohort"].unique()[:6]:
    m0 = cohort_data[
        (cohort_data["cohort"] == cohort) &
        (cohort_data["month_number"] == 0)
    ]["retention_pct"].values
    m3 = cohort_data[
        (cohort_data["cohort"] == cohort) &
        (cohort_data["month_number"] == 3)
    ]["retention_pct"].values
    m0_val = f"{m0[0]}%" if len(m0) > 0 else "N/A"
    m3_val = f"{m3[0]}%" if len(m3) > 0 else "N/A"
    report.append(
        f"  Cohort {cohort}  "
        f"Month 0: {m0_val:>6}  →  Month 3: {m3_val:>6}"
    )


# =============================================================================
# QUESTION 4 — WHO IS AT RISK OF CHURNING?
# =============================================================================

print("\n[Q4] Churn Risk Analysis...")

churn_sql = """
WITH customer_stats AS (
    SELECT
        o.customer_id,
        c.full_name,
        c.email,
        c.tier,
        c.region,
        c.channel,
        COUNT(*)                                   AS total_orders,
        ROUND(SUM(o.revenue)::numeric, 2)          AS total_spent,
        MAX(o.order_date)::date                    AS last_order_date,
        CURRENT_DATE - MAX(o.order_date)::date     AS days_since_order,
        MIN(o.order_date)::date                    AS first_order_date
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'Completed'
    GROUP BY
        o.customer_id, c.full_name, c.email,
        c.tier, c.region, c.channel
)
SELECT *,
    CASE
        WHEN days_since_order > 365
         AND total_orders     >= 3
         AND total_spent      > 10000
        THEN 'High Value Churned'
        WHEN days_since_order > 180
         AND total_orders     >= 2
        THEN 'At Risk'
        WHEN days_since_order > 90
        THEN 'Slipping Away'
        ELSE 'Active'
    END AS churn_status
FROM customer_stats
WHERE days_since_order > 90
ORDER BY total_spent DESC, days_since_order DESC
"""

churn_risk = run_query(churn_sql, "Churn risk customer list")
churn_risk.to_csv("outputs/churn_risk.csv", index=False)

# Churn summary
churn_summary = (
    churn_risk
    .groupby("churn_status")
    .agg(
        customer_count=("customer_id", "count"),
        avg_spent=("total_spent", "mean"),
        total_spent=("total_spent", "sum"),
        avg_days_silent=("days_since_order", "mean"),
    )
    .reset_index()
    .sort_values("total_spent", ascending=False)
)
churn_summary.to_csv("outputs/churn_summary.csv", index=False)

report.append("\n" + "=" * 60)
report.append("QUESTION 4 — Churn Risk Summary")
report.append("=" * 60 + "\n")
for _, row in churn_summary.iterrows():
    report.append(
        f"  {row['churn_status']:<22} "
        f"Customers: {int(row['customer_count']):>3}  "
        f"Avg Spent: ₱{row['avg_spent']:>10,.0f}  "
        f"Avg Silent: {int(row['avg_days_silent']):>3} days"
    )


# =============================================================================
# OVERALL SUMMARY
# =============================================================================

total_revenue_sql = """
SELECT
    ROUND(SUM(revenue)::numeric, 2)       AS total_revenue,
    COUNT(DISTINCT customer_id)           AS active_customers,
    COUNT(*)                              AS total_orders,
    ROUND(AVG(revenue)::numeric, 2)       AS avg_order_value
FROM orders
WHERE status = 'Completed'
"""

overall = run_query(total_revenue_sql, "Overall summary")

summary = (
    f"\nOVERALL SUMMARY"
    f"\n  Total Revenue     : ₱{overall['total_revenue'].values[0]:>12,.0f}"
    f"\n  Active Customers  : {int(overall['active_customers'].values[0]):>6}"
    f"\n  Total Orders      : {int(overall['total_orders'].values[0]):>6}"
    f"\n  Avg Order Value   : ₱{overall['avg_order_value'].values[0]:>10,.0f}"
    f"\n  RFM Segments      : {len(rfm_summary)}"
    f"\n  Churn Risk        : {len(churn_risk)} customers > 90 days silent"
)

report.append("\n" + "=" * 60)
report.append(summary)

with open("outputs/analysis_report.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(report))

print("\n" + "=" * 60)
print(summary)
print("\n  Output files saved to outputs/")
print("  Next → python visualize_results.py")
print("=" * 60)
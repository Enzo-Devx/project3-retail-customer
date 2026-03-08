# 🛍️ Retail Customer Analysis

> **Role:** Junior Data Analyst  
> **Scenario:** RetailPH is losing customers. Marketing needs to know who to target.  
> **Stack:** Python · PostgreSQL · DuckDB · Plotly · Streamlit · Faker

---

## 🧠 Business Problem

The retail team noticed declining customer activity across all channels. The manager needed to understand:

- Who are our best customers? (RFM Analysis)
- Which cohorts are retaining well vs dropping off?
- Which product categories drive the most value?
- Which customers are at risk of churning?

---

## 📊 Key Findings

| Metric | Value |
|---|---|
| Total Revenue | ₱3,804,511 |
| Active Customers | 329 |
| Total Orders | 1,050 |
| Avg Order Value | ₱3,623 |
| Customers Gone Silent | **326 of 329 (99%) 🚨** |

### RFM Segments
| Segment | Customers | Total Revenue |
|---|---|---|
| Needs Attention | 74 | ₱1,239,262 |
| Potential Loyalists | 55 | ₱991,785 |
| Lost | 26 | ₱921,912 |
| Loyal Customers | 62 | ₱314,409 |
| At Risk | 50 | ₱162,584 |
| Champions | 56 | ₱109,028 |
| Cant Lose Them | 3 | ₱52,539 |

### Category Revenue
| Category | Revenue |
|---|---|
| 🥇 Home & Living | ₱1,146,221 |
| 🥈 Fashion | ₱834,598 |
| 🥉 Electronics | ₱825,239 |
| Beauty | ₱586,414 |
| Sports | ₱412,039 |

---

## 🛠 Tech Stack

| Tool | Purpose |
|---|---|
| Python / pandas | Data cleaning & transformation |
| Faker | Realistic synthetic data generation |
| PostgreSQL | Production-style database storage |
| SQLAlchemy | Python → PostgreSQL ETL pipeline |
| DBeaver | SQL query development & verification |
| Plotly | Interactive HTML charts |
| Streamlit | Live dashboard connected to PostgreSQL |
| Git | Version control |

---

## 📁 Project Structure

```
project3_retail/
├── data/
│   ├── project03_customers_raw.csv    ← Faker-generated customers
│   ├── project03_products_raw.csv     ← product catalog
│   └── project03_orders_raw.csv       ← 1200 orders (messy)
├── outputs/
│   ├── rfm_full.csv                   ← full RFM scores
│   ├── rfm_summary.csv                ← segment summary
│   ├── category_revenue.csv           ← category breakdown
│   ├── cohort_retention.csv           ← cohort analysis
│   ├── churn_risk.csv                 ← at-risk customers
│   ├── analysis_report.txt            ← full text report
│   └── charts/                        ← HTML + PNG charts
├── generate_data.py                   ← Faker data generator
├── clean_data.py                      ← ETL → PostgreSQL
├── analyze_data.py                    ← PostgreSQL queries
├── visualize_results.py               ← Plotly charts
├── dashboard.py                       ← Streamlit + live DB
└── .gitignore
```

---

## ▶️ How to Run

```bash
# 1. Clone the repo
git clone https://github.com/Enzo-Devx/project3-retail-customer.git
cd project3-retail-customer

# 2. Create and activate virtual environment
python -m venv venv
venv\Scripts\Activate.ps1      # Windows
source venv/bin/activate       # Mac/Linux

# 3. Install dependencies
pip install pandas numpy faker sqlalchemy psycopg2-binary plotly streamlit kaleido

# 4. Set up PostgreSQL
# Create a database called: retaildb
# Update DB credentials in clean_data.py if needed

# 5. Run the pipeline in order
python generate_data.py          # generate synthetic data
python clean_data.py             # clean + load into PostgreSQL
python analyze_data.py           # run analysis queries
python visualize_results.py      # generate charts

# 6. Launch the dashboard
streamlit run dashboard.py
```

---

## 🔑 Key Concepts Demonstrated

**RFM Scoring with NTILE:**
```sql
-- Score customers 1-5 on Recency, Frequency, Monetary
SELECT *,
    NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
    NTILE(5) OVER (ORDER BY frequency    DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary     DESC) AS m_score
FROM rfm_base
```

**Cohort Retention:**
```sql
-- Track how many customers return each month after first purchase
EXTRACT(YEAR  FROM AGE(order_month, cohort_month)) * 12 +
EXTRACT(MONTH FROM AGE(order_month, cohort_month)) AS month_number
```

**ETL Pipeline:**
```python
# Load clean data directly into PostgreSQL
df.to_sql("customers", engine, if_exists="replace",
          index=False, chunksize=500)
```

---

## 📈 Dashboard Features

- 5 KPI cards — Revenue, Customers, Orders, AOV, Churn Risk
- Live alert banner — churn count updates with filters
- RFM bubble chart — segment landscape at a glance
- Cohort retention heatmap — full red = retention crisis
- Category donut chart — revenue share
- Top 10 products bar chart
- Churn risk table — sortable, filterable, with email contacts

---

## 💡 Business Recommendation

1. **Immediate** — Win-back campaign targeting High Value Churned customers
2. **Short term** — Post-purchase email sequence (7, 14, 30 days)
3. **Short term** — Introduce a loyalty program
4. **Medium term** — Customer survey to understand WHY they're leaving
5. **Medium term** — Double down on Home & Living (highest AOV category)

> ⚠️ **Analyst Note:** The data shows *when* customers left. It does not show *why*. Qualitative research (surveys, interviews) is needed to complete the picture.

---

*Built as part of a data analyst portfolio. Simulated business scenario with realistic synthetic data generated using the Faker library.*

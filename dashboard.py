# =============================================================================
# dashboard.py
# Project 03 — Retail Customer Analysis
# Purpose: Full interactive Streamlit dashboard connecting to PostgreSQL.
#          Run with: streamlit run dashboard.py
# =============================================================================

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# =============================================================================
# PAGE CONFIG
# =============================================================================

st.set_page_config(
    page_title="RetailPH — Customer Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# CUSTOM CSS
# =============================================================================

st.markdown("""
<style>
    .stApp { background-color: #F8FAFC; }

    .kpi-card {
        background: white;
        border-radius: 12px;
        padding: 18px 22px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        border-left: 5px solid #2563EB;
        margin-bottom: 8px;
    }
    .kpi-card.danger  { border-left-color: #DC2626; }
    .kpi-card.warning { border-left-color: #D97706; }
    .kpi-card.success { border-left-color: #16A34A; }
    .kpi-card.purple  { border-left-color: #7C3AED; }

    .kpi-label { font-size: 12px; color: #64748B;
                 font-weight: 500; margin-bottom: 4px; }
    .kpi-value { font-size: 24px; font-weight: 700; color: #1E293B; }
    .kpi-sub   { font-size: 11px; color: #94A3B8; margin-top: 2px; }

    .section-header {
        font-size: 15px; font-weight: 700; color: #1E293B;
        margin: 20px 0 10px 0; padding-bottom: 6px;
        border-bottom: 2px solid #E2E8F0;
    }
    .alert-box {
        background: #FEF2F2; border: 1px solid #FECACA;
        border-radius: 8px; padding: 12px 16px;
        color: #DC2626; font-weight: 600; margin-bottom: 12px;
    }
    .insight-box {
        background: #EFF6FF; border: 1px solid #BFDBFE;
        border-radius: 8px; padding: 12px 16px;
        color: #1D4ED8; margin-bottom: 12px;
    }
    #MainMenu { visibility: hidden; }
    footer     { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATABASE CONNECTION
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

COLORS = {
    "primary" : "#2563EB",
    "danger"  : "#DC2626",
    "success" : "#16A34A",
    "warning" : "#D97706",
    "purple"  : "#7C3AED",
    "teal"    : "#0891B2",
    "bg"      : "#F8FAFC",
    "grid"    : "#E2E8F0",
}

SEGMENT_COLORS = {
    "Champions"          : "#16A34A",
    "Loyal Customers"    : "#2563EB",
    "Potential Loyalists": "#0891B2",
    "Needs Attention"    : "#D97706",
    "At Risk"            : "#EA580C",
    "Cant Lose Them"     : "#DC2626",
    "Lost"               : "#6B7280",
}


# =============================================================================
# LOAD DATA — cached so filters don't re-query DB every second
# =============================================================================

@st.cache_data
def load_all_data():
    engine = create_engine(CONNECTION_STRING)

    rfm_summary = pd.read_sql("""
        WITH rfm_base AS (
            SELECT o.customer_id,
                CURRENT_DATE - MAX(o.order_date)::date AS recency_days,
                COUNT(*)                               AS frequency,
                ROUND(SUM(o.revenue)::numeric, 2)      AS monetary
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
        )
        SELECT *,
            CASE
                WHEN r_score>=4 AND f_score>=4 AND m_score>=4
                    THEN 'Champions'
                WHEN r_score>=3 AND f_score>=3 AND m_score>=3
                    THEN 'Loyal Customers'
                WHEN r_score>=3 AND f_score<=2
                    THEN 'Potential Loyalists'
                WHEN r_score<=2 AND f_score>=3 AND m_score>=3
                    THEN 'At Risk'
                WHEN r_score<=2 AND f_score>=4
                    THEN 'Cant Lose Them'
                WHEN r_score<=1 AND f_score<=1
                    THEN 'Lost'
                ELSE 'Needs Attention'
            END AS rfm_segment
        FROM rfm_scores
    """, engine)

    rfm_seg_summary = (
        rfm_summary
        .groupby("rfm_segment")
        .agg(
            customer_count=("customer_id", "count"),
            avg_recency   =("recency_days", "mean"),
            avg_frequency =("frequency", "mean"),
            avg_monetary  =("monetary", "mean"),
            total_revenue =("monetary", "sum"),
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    category_rev = pd.read_sql("""
        SELECT p.category,
            COUNT(DISTINCT o.customer_id)        AS unique_customers,
            COUNT(*)                             AS total_orders,
            ROUND(SUM(o.revenue)::numeric, 2)    AS total_revenue,
            ROUND(AVG(o.revenue)::numeric, 2)    AS avg_order_value
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        WHERE o.status = 'Completed'
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """, engine)

    cohort_data = pd.read_sql("""
        WITH first_orders AS (
            SELECT customer_id,
                DATE_TRUNC('month', MIN(order_date)) AS cohort_month
            FROM orders WHERE status='Completed'
            GROUP BY customer_id
        ),
        order_months AS (
            SELECT o.customer_id,
                DATE_TRUNC('month', o.order_date) AS order_month,
                f.cohort_month
            FROM orders o
            JOIN first_orders f ON o.customer_id = f.customer_id
            WHERE o.status = 'Completed'
        ),
        cohort_data AS (
            SELECT cohort_month, order_month,
                EXTRACT(YEAR  FROM AGE(order_month, cohort_month))*12 +
                EXTRACT(MONTH FROM AGE(order_month, cohort_month))
                    AS month_number,
                COUNT(DISTINCT customer_id) AS active_customers
            FROM order_months
            GROUP BY cohort_month, order_month
        ),
        cohort_size AS (
            SELECT cohort_month,
                COUNT(DISTINCT customer_id) AS cohort_customers
            FROM first_orders GROUP BY cohort_month
        )
        SELECT TO_CHAR(cd.cohort_month,'YYYY-MM') AS cohort,
            cs.cohort_customers                   AS cohort_size,
            cd.month_number::int                  AS month_number,
            cd.active_customers                   AS active,
            ROUND(cd.active_customers*100.0
                /cs.cohort_customers,1)            AS retention_pct
        FROM cohort_data cd
        JOIN cohort_size cs ON cd.cohort_month=cs.cohort_month
        WHERE cd.month_number <= 12
          AND cd.cohort_month >= '2023-01-01'
        ORDER BY cd.cohort_month, cd.month_number
    """, engine)

    churn_risk = pd.read_sql("""
        WITH stats AS (
            SELECT o.customer_id, c.full_name, c.email,
                c.tier, c.region, c.channel,
                COUNT(*)                              AS total_orders,
                ROUND(SUM(o.revenue)::numeric,2)      AS total_spent,
                MAX(o.order_date)::date               AS last_order_date,
                CURRENT_DATE-MAX(o.order_date)::date  AS days_since_order
            FROM orders o
            JOIN customers c ON o.customer_id=c.customer_id
            WHERE o.status='Completed'
            GROUP BY o.customer_id, c.full_name,
                     c.email, c.tier, c.region, c.channel
        )
        SELECT *,
            CASE
                WHEN days_since_order>365
                 AND total_orders>=3
                 AND total_spent>10000 THEN 'High Value Churned'
                WHEN days_since_order>180
                 AND total_orders>=2   THEN 'At Risk'
                WHEN days_since_order>90 THEN 'Slipping Away'
                ELSE 'Active'
            END AS churn_status
        FROM stats
        WHERE days_since_order > 90
        ORDER BY total_spent DESC
    """, engine)

    overall = pd.read_sql("""
        SELECT
            ROUND(SUM(revenue)::numeric,2)     AS total_revenue,
            COUNT(DISTINCT customer_id)        AS active_customers,
            COUNT(*)                           AS total_orders,
            ROUND(AVG(revenue)::numeric,2)     AS avg_order_value
        FROM orders WHERE status='Completed'
    """, engine)

    top_products = pd.read_sql("""
        SELECT p.product_name, p.category,
            COUNT(*)                           AS total_orders,
            ROUND(SUM(o.revenue)::numeric,2)   AS total_revenue
        FROM orders o
        JOIN products p ON o.product_id=p.product_id
        WHERE o.status='Completed'
        GROUP BY p.product_name, p.category
        ORDER BY total_revenue DESC LIMIT 10
    """, engine)

    return (rfm_summary, rfm_seg_summary, category_rev,
            cohort_data, churn_risk, overall, top_products)

# =============================================================================
# LOAD DATA
# =============================================================================

try:
    (rfm_full, rfm_summary, category_rev,
     cohort_data, churn_risk, overall, top_products) = load_all_data()
    db_ok = True
except Exception as e:
    st.error(f"❌ Could not connect to PostgreSQL: {e}")
    st.stop()


# =============================================================================
# SIDEBAR
# =============================================================================

st.sidebar.image("https://img.icons8.com/fluency/48/shopping-bag.png", width=44)
st.sidebar.title("RetailPH Analytics")
st.sidebar.markdown("**Customer Intelligence Dashboard**")
st.sidebar.markdown("---")

all_segments = sorted(rfm_summary["rfm_segment"].tolist())
selected_segments = st.sidebar.multiselect(
    "🎯 RFM Segment",
    options=all_segments,
    default=all_segments,
)

all_regions = sorted(rfm_full["customer_id"].apply(
    lambda x: x).unique().tolist())

churn_statuses = sorted(churn_risk["churn_status"].unique().tolist())
selected_churn = st.sidebar.multiselect(
    "⚠️ Churn Status",
    options=churn_statuses,
    default=churn_statuses,
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "📊 **Project 03** — Retail Customer Analysis  \n"
    "🛠 Stack: Python · PostgreSQL · Plotly · Streamlit  \n"
    "👤 Analyst: Junior DA"
)


# =============================================================================
# HEADER
# =============================================================================

st.markdown("## 🛍️ RetailPH — Customer Analytics Dashboard")
st.markdown("**Jan 2023 – Sep 2024 · Prepared for Jess & Leadership Team**")
st.markdown("---")

# Alert banner
churn_count = len(churn_risk)
hv_churn    = len(churn_risk[churn_risk["churn_status"]=="High Value Churned"])
st.markdown(f"""
<div class="alert-box">
    🚨 {churn_count} customers have gone silent (90+ days).
    {hv_churn} are high-value churned customers who spent ₱10,000+.
    Immediate win-back campaign recommended.
</div>""", unsafe_allow_html=True)


# =============================================================================
# KPI CARDS
# =============================================================================

k1, k2, k3, k4, k5 = st.columns(5)

total_rev = overall["total_revenue"].values[0]
total_ord = int(overall["total_orders"].values[0])
avg_ord   = overall["avg_order_value"].values[0]
total_cust= int(overall["active_customers"].values[0])

with k1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">💰 Total Revenue</div>
        <div class="kpi-value">₱{total_rev/1_000_000:.2f}M</div>
        <div class="kpi-sub">Completed orders</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi-card purple">
        <div class="kpi-label">👥 Customers</div>
        <div class="kpi-value">{total_cust}</div>
        <div class="kpi-sub">With completed orders</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi-card success">
        <div class="kpi-label">🛒 Total Orders</div>
        <div class="kpi-value">{total_ord:,}</div>
        <div class="kpi-sub">Completed</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="kpi-card warning">
        <div class="kpi-label">💳 Avg Order Value</div>
        <div class="kpi-value">₱{avg_ord:,.0f}</div>
        <div class="kpi-sub">Per completed order</div>
    </div>""", unsafe_allow_html=True)

with k5:
    st.markdown(f"""
    <div class="kpi-card danger">
        <div class="kpi-label">🚨 Churn Risk</div>
        <div class="kpi-value">{churn_count}</div>
        <div class="kpi-sub">{hv_churn} high-value churned</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# =============================================================================
# ROW 2 — RFM BUBBLE + REVENUE BY SEGMENT
# =============================================================================

st.markdown('<div class="section-header">🎯 RFM Customer Segments</div>',
            unsafe_allow_html=True)

filtered_rfm = rfm_summary[
    rfm_summary["rfm_segment"].isin(selected_segments)
]

col1, col2 = st.columns([3, 2])

with col1:
    fig_bubble = go.Figure()
    for _, row in filtered_rfm.iterrows():
        seg   = row["rfm_segment"]
        color = SEGMENT_COLORS.get(seg, COLORS["primary"])
        fig_bubble.add_trace(go.Scatter(
            x=[row["avg_frequency"]],
            y=[row["avg_monetary"]],
            mode="markers+text",
            name=seg,
            text=[seg],
            textposition="top center",
            textfont=dict(size=9, color=color),
            marker=dict(
                size=row["customer_count"] * 1.8,
                color=color, opacity=0.75,
                line=dict(color="white", width=2),
            ),
            hovertemplate=(
                f"<b>{seg}</b><br>"
                f"Customers: {int(row['customer_count'])}<br>"
                f"Avg Orders: {row['avg_frequency']:.1f}<br>"
                f"Avg Spend: ₱{row['avg_monetary']:,.0f}"
                "<extra></extra>"
            ),
        ))
    fig_bubble.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        height=360, showlegend=False,
        margin=dict(l=40,r=20,t=30,b=40),
        xaxis=dict(title="Avg Frequency", showgrid=False),
        yaxis=dict(title="Avg Monetary (₱)",
                   tickprefix="₱", tickformat=",.0f",
                   gridcolor=COLORS["grid"]),
        font=dict(size=10),
    )
    st.plotly_chart(fig_bubble, use_container_width=True)

with col2:
    seg_sorted = filtered_rfm.sort_values("total_revenue", ascending=True)
    fig_segbar = go.Figure(go.Bar(
        x=seg_sorted["total_revenue"],
        y=seg_sorted["rfm_segment"],
        orientation="h",
        marker_color=[
            SEGMENT_COLORS.get(s, COLORS["primary"])
            for s in seg_sorted["rfm_segment"]
        ],
        text=[f"₱{v:,.0f}" for v in seg_sorted["total_revenue"]],
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Revenue: ₱%{x:,.0f}<extra></extra>"
        ),
    ))
    fig_segbar.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        height=360, margin=dict(l=10,r=80,t=30,b=40),
        font=dict(size=10),
    )
    fig_segbar.update_xaxes(
        tickprefix="₱", tickformat=",.0f",
        showgrid=True, gridcolor=COLORS["grid"],
        range=[0, seg_sorted["total_revenue"].max()*1.4]
    )
    fig_segbar.update_yaxes(showgrid=False)
    st.plotly_chart(fig_segbar, use_container_width=True)


# =============================================================================
# ROW 3 — COHORT HEATMAP (full width — this deserves the spotlight)
# =============================================================================

st.markdown('<div class="section-header">📅 Cohort Retention Heatmap</div>',
            unsafe_allow_html=True)

st.markdown("""
<div class="insight-box">
    💡 <b>How to read this:</b> Each row = a customer cohort (month they
    first bought). Each column = months since first purchase.
    Green = customers came back. Red = they didn't.
    No green = zero retention. That's the crisis.
</div>""", unsafe_allow_html=True)

pivot = cohort_data.pivot_table(
    index="cohort",
    columns="month_number",
    values="retention_pct",
    aggfunc="first"
)
cohorts    = pivot.index.tolist()
month_nums = [str(int(c)) for c in pivot.columns.tolist()]
z_clean    = [
    [float(v) if str(v) not in ["nan","None"] else 0
     for v in row]
    for row in pivot.values.tolist()
]

fig_heat = go.Figure(go.Heatmap(
    z=z_clean,
    x=[f"M{m}" for m in month_nums],
    y=cohorts,
    colorscale=[
        [0.0, "#DC2626"],
        [0.3, "#F97316"],
        [0.6, "#FACC15"],
        [1.0, "#16A34A"],
    ],
    text=[[f"{v:.0f}%" if v > 0 else ""
           for v in row] for row in z_clean],
    texttemplate="%{text}",
    textfont=dict(size=9, color="white"),
    hovertemplate=(
        "Cohort: %{y}<br>Month: %{x}<br>"
        "Retention: %{z:.1f}%<extra></extra>"
    ),
    colorbar=dict(title="Retention %", ticksuffix="%"),
    zmin=0, zmax=100,
))
fig_heat.update_layout(
    paper_bgcolor="white",
    height=420,
    margin=dict(l=80,r=40,t=30,b=40),
    font=dict(size=10),
    xaxis=dict(side="top"),
)
st.plotly_chart(fig_heat, use_container_width=True)


# =============================================================================
# ROW 4 — CATEGORY REVENUE + TOP PRODUCTS
# =============================================================================

st.markdown('<div class="section-header">🏷️ Category & Product Performance</div>',
            unsafe_allow_html=True)

col3, col4 = st.columns([2, 3])

with col3:
    cat_colors = [
        COLORS["primary"], COLORS["purple"], COLORS["teal"],
        COLORS["warning"], COLORS["success"]
    ]
    fig_donut = go.Figure(go.Pie(
        labels=category_rev["category"],
        values=category_rev["total_revenue"],
        hole=0.55,
        marker_colors=cat_colors,
        textinfo="label+percent",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Revenue: ₱%{value:,.0f}<extra></extra>"
        ),
    ))
    fig_donut.update_layout(
        paper_bgcolor="white",
        height=340,
        margin=dict(l=10,r=10,t=30,b=10),
        font=dict(size=10),
        showlegend=False,
    )
    st.plotly_chart(fig_donut, use_container_width=True)

with col4:
    top_sorted = top_products.sort_values("total_revenue", ascending=True)
    fig_prod   = go.Figure(go.Bar(
        x=top_sorted["total_revenue"],
        y=top_sorted["product_name"],
        orientation="h",
        marker_color=COLORS["primary"],
        text=[f"₱{v:,.0f}" for v in top_sorted["total_revenue"]],
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Revenue: ₱%{x:,.0f}<extra></extra>"
        ),
    ))
    fig_prod.update_layout(
        paper_bgcolor="white", plot_bgcolor="white",
        height=340, margin=dict(l=10,r=80,t=30,b=40),
        font=dict(size=10),
    )
    fig_prod.update_xaxes(
        tickprefix="₱", tickformat=",.0f",
        showgrid=True, gridcolor=COLORS["grid"],
        range=[0, top_sorted["total_revenue"].max()*1.3]
    )
    fig_prod.update_yaxes(showgrid=False)
    st.plotly_chart(fig_prod, use_container_width=True)


# =============================================================================
# ROW 5 — CHURN RISK TABLE
# =============================================================================

st.markdown('<div class="section-header">🚨 Churn Risk — Customer List</div>',
            unsafe_allow_html=True)

filtered_churn = churn_risk[
    churn_risk["churn_status"].isin(selected_churn)
].copy()

# Color code churn status
churn_color_map = {
    "High Value Churned": "🔴",
    "At Risk"           : "🟠",
    "Slipping Away"     : "🟡",
}
filtered_churn["status_icon"] = filtered_churn["churn_status"].map(
    churn_color_map
)
filtered_churn["total_spent"] = filtered_churn["total_spent"].apply(
    lambda x: f"₱{x:,.0f}"
)
filtered_churn["last_order_date"] = pd.to_datetime(
    filtered_churn["last_order_date"], errors="coerce"
).dt.strftime("%Y-%m-%d")

display_cols = {
    "status_icon"     : "⚠",
    "full_name"       : "Customer",
    "email"           : "Email",
    "tier"            : "Tier",
    "region"          : "Region",
    "total_orders"    : "Orders",
    "total_spent"     : "Total Spent",
    "last_order_date" : "Last Order",
    "days_since_order": "Days Silent",
    "churn_status"    : "Status",
}

st.dataframe(
    filtered_churn[list(display_cols.keys())]
    .rename(columns=display_cols)
    .reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)
st.caption(
    f"Showing {len(filtered_churn)} customers. "
    "Sort by 'Days Silent' or 'Total Spent' to prioritize "
    "win-back outreach."
)


# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#94A3B8;font-size:12px;'>"
    "RetailPH Customer Analytics · Project 03 · "
    "Built with Python, PostgreSQL, Plotly & Streamlit"
    "</div>",
    unsafe_allow_html=True,
)
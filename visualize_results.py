# =============================================================================
# visualize_results.py
# Project 03 — Retail Customer Analysis
# Purpose: Generate presentation-ready Plotly charts from analysis outputs.
#          HTML = interactive (Zoom calls)
#          PNG  = static (emails, slides)
# =============================================================================

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os

# --- PATHS ---
CHARTS_DIR = "outputs/charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

# --- LOAD DATA ---
rfm_summary  = pd.read_csv("outputs/rfm_summary.csv")
rfm_full     = pd.read_csv("outputs/rfm_full.csv")
category_rev = pd.read_csv("outputs/category_revenue.csv")
top_products = pd.read_csv("outputs/top_products.csv")
cohort_data  = pd.read_csv("outputs/cohort_retention.csv")
cohort_pivot = pd.read_csv("outputs/cohort_pivot.csv")
churn_risk   = pd.read_csv("outputs/churn_risk.csv")
churn_summary= pd.read_csv("outputs/churn_summary.csv")

print("=" * 60)
print("  VISUALIZE_RESULTS.PY — Retail Customer Analysis")
print("=" * 60)

# =============================================================================
# GLOBAL STYLE
# =============================================================================

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

CHURN_COLORS = {
    "High Value Churned" : "#DC2626",
    "At Risk"            : "#D97706",
    "Slipping Away"      : "#2563EB",
}

def save_chart(fig, name):
    fig.write_html(f"{CHARTS_DIR}/{name}.html")
    try:
        fig.write_image(f"{CHARTS_DIR}/{name}.png",
                        width=1000, height=520, scale=2)
    except Exception:
        pass
    print(f"   ✓ {name}")

def base_layout(title, height=520):
    return dict(
        title=dict(
            text=title,
            font=dict(size=15, color="#1E293B"),
            x=0.02
        ),
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(family="sans-serif", size=11, color="#475569"),
        height=height,
        margin=dict(l=60, r=40, t=70, b=60),
        xaxis=dict(showgrid=False, linecolor=COLORS["grid"]),
        yaxis=dict(gridcolor=COLORS["grid"], linecolor=COLORS["grid"]),
    )


# =============================================================================
# CHART 1 — RFM SEGMENT BUBBLE CHART
# X = Avg Frequency, Y = Avg Monetary, Size = Customer Count
# Each bubble = one segment. One glance = full customer landscape.
# =============================================================================

print("\n[CHART 1] RFM Segment Bubble Chart...")

fig1 = go.Figure()

for _, row in rfm_summary.iterrows():
    seg   = row["rfm_segment"]
    color = SEGMENT_COLORS.get(seg, COLORS["primary"])
    fig1.add_trace(go.Scatter(
        x=[row["avg_frequency"]],
        y=[row["avg_monetary"]],
        mode="markers+text",
        name=seg,
        text=[seg],
        textposition="top center",
        textfont=dict(size=10, color=color),
        marker=dict(
            size=row["customer_count"] * 1.8,
            color=color,
            opacity=0.75,
            line=dict(color="white", width=2),
        ),
        hovertemplate=(
            f"<b>{seg}</b><br>"
            f"Customers: {int(row['customer_count'])}<br>"
            f"Avg Frequency: {row['avg_frequency']:.1f} orders<br>"
            f"Avg Spend: ₱{row['avg_monetary']:,.0f}<br>"
            f"Total Revenue: ₱{row['total_revenue']:,.0f}"
            "<extra></extra>"
        ),
    ))

fig1.update_layout(
    **base_layout(
        "RFM Segments — Customer Landscape "
        "(bubble size = customer count)", height=540
    ),
    showlegend=False,
    xaxis_title="Avg Purchase Frequency",
    yaxis_title="Avg Monetary Value (₱)",
)
fig1.update_yaxes(tickprefix="₱", tickformat=",.0f")
save_chart(fig1, "chart1_rfm_segments")


# =============================================================================
# CHART 2 — RFM SEGMENT REVENUE BAR
# Total revenue contribution per segment.
# Shows which segments are actually worth the most to the business.
# =============================================================================

print("\n[CHART 2] RFM Revenue by Segment...")

rfm_sorted = rfm_summary.sort_values("total_revenue", ascending=True)
bar_colors = [
    SEGMENT_COLORS.get(s, COLORS["primary"])
    for s in rfm_sorted["rfm_segment"]
]

fig2 = go.Figure(go.Bar(
    x=rfm_sorted["total_revenue"],
    y=rfm_sorted["rfm_segment"],
    orientation="h",
    marker_color=bar_colors,
    text=[f"₱{v:,.0f}  ({int(c)} customers)"
          for v, c in zip(
              rfm_sorted["total_revenue"],
              rfm_sorted["customer_count"]
          )],
    textposition="outside",
    hovertemplate=(
        "<b>%{y}</b><br>"
        "Total Revenue: ₱%{x:,.0f}<extra></extra>"
    ),
))

fig2.update_layout(
    title=dict(
        text="Revenue Contribution by RFM Segment — Needs Attention Dominates 🚨",
        font=dict(size=15, color="#1E293B"), x=0.02
    ),
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg"],
    height=460,
    margin=dict(l=60, r=40, t=70, b=60),
    font=dict(family="sans-serif", size=11, color="#475569"),
)
fig2.update_xaxes(
    title="Total Revenue (₱)",
    tickprefix="₱", tickformat=",.0f",
    showgrid=True, gridcolor=COLORS["grid"],
    range=[0, rfm_sorted["total_revenue"].max() * 1.35]
)
fig2.update_yaxes(showgrid=False)
save_chart(fig2, "chart2_rfm_revenue")


# =============================================================================
# CHART 3 — CATEGORY REVENUE (Donut + Bar combo)
# Donut shows proportional share. Bar shows exact numbers.
# =============================================================================

print("\n[CHART 3] Category Revenue...")

cat_colors = [
    COLORS["primary"], COLORS["purple"], COLORS["teal"],
    COLORS["warning"], COLORS["success"]
]

fig3 = make_subplots(
    rows=1, cols=2,
    specs=[[{"type": "domain"}, {"type": "bar"}]],
    subplot_titles=["Revenue Share", "Revenue & Avg Order Value"],
)

# Donut
fig3.add_trace(go.Pie(
    labels=category_rev["category"],
    values=category_rev["total_revenue"],
    hole=0.55,
    marker_colors=cat_colors,
    textinfo="label+percent",
    hovertemplate=(
        "<b>%{label}</b><br>"
        "Revenue: ₱%{value:,.0f}<br>"
        "Share: %{percent}<extra></extra>"
    ),
), row=1, col=1)

# Bar
fig3.add_trace(go.Bar(
    x=category_rev["category"],
    y=category_rev["total_revenue"],
    marker_color=cat_colors,
    name="Total Revenue",
    hovertemplate=(
        "<b>%{x}</b><br>"
        "Revenue: ₱%{y:,.0f}<extra></extra>"
    ),
    showlegend=False,
), row=1, col=2)

fig3.update_layout(
    title=dict(
        text="Category Revenue — Home & Living Leads 🏠",
        font=dict(size=15, color="#1E293B"), x=0.02
    ),
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg"],
    height=480,
    margin=dict(l=40, r=40, t=70, b=60),
    font=dict(size=11),
)
fig3.update_yaxes(
    tickprefix="₱", tickformat=",.0f",
    gridcolor=COLORS["grid"], row=1, col=2
)
save_chart(fig3, "chart3_category_revenue")


# =============================================================================
# CHART 4 — COHORT RETENTION HEATMAP
# The most powerful visual in this project.
# Rows = cohort month, Columns = months since first purchase
# Color = retention %. Dark red = everyone left. Green = sticky.
# =============================================================================

print("\n[CHART 4] Cohort Retention Heatmap...")

# Prepare pivot matrix
pivot = cohort_data.pivot_table(
    index="cohort",
    columns="month_number",
    values="retention_pct",
    aggfunc="first"
)

cohorts     = pivot.index.tolist()
month_nums  = [str(int(c)) for c in pivot.columns.tolist()]
z_values    = pivot.values.tolist()

# Replace None with 0 for heatmap
z_clean = [
    [v if v is not None and str(v) != "nan" else 0
     for v in row]
    for row in z_values
]

fig4 = go.Figure(go.Heatmap(
    z=z_clean,
    x=[f"Month {m}" for m in month_nums],
    y=cohorts,
    colorscale=[
        [0.0,  "#DC2626"],   # red   = low retention
        [0.3,  "#F97316"],   # orange
        [0.6,  "#FACC15"],   # yellow
        [1.0,  "#16A34A"],   # green = high retention
    ],
    text=[[f"{v:.0f}%" if v > 0 else ""
           for v in row]
          for row in z_clean],
    texttemplate="%{text}",
    textfont=dict(size=10, color="white"),
    hovertemplate=(
        "Cohort: %{y}<br>"
        "%{x}<br>"
        "Retention: %{z:.1f}%"
        "<extra></extra>"
    ),
    colorbar=dict(
        title="Retention %",
        ticksuffix="%",
    ),
    zmin=0,
    zmax=100,
))

fig4.update_layout(
    title=dict(
        text="Cohort Retention Heatmap — "
             "Retention Collapses After Month 0 🚨",
        font=dict(size=15, color="#1E293B"), x=0.02
    ),
    paper_bgcolor=COLORS["bg"],
    height=520,
    margin=dict(l=80, r=40, t=70, b=60),
    font=dict(size=11),
    xaxis=dict(side="top"),
)
save_chart(fig4, "chart4_cohort_heatmap")


# =============================================================================
# CHART 5 — CHURN RISK BREAKDOWN
# Who's gone, who's leaving, and how much revenue is at stake.
# =============================================================================

print("\n[CHART 5] Churn Risk Breakdown...")

churn_colors = [
    CHURN_COLORS.get(s, COLORS["primary"])
    for s in churn_summary["churn_status"]
]

fig5 = make_subplots(
    rows=1, cols=2,
    subplot_titles=[
        "Customers by Churn Status",
        "Revenue at Stake by Status"
    ],
)

# Customer count
fig5.add_trace(go.Bar(
    x=churn_summary["churn_status"],
    y=churn_summary["customer_count"],
    marker_color=churn_colors,
    text=churn_summary["customer_count"],
    textposition="outside",
    hovertemplate=(
        "<b>%{x}</b><br>"
        "Customers: %{y}<extra></extra>"
    ),
    showlegend=False,
), row=1, col=1)

# Revenue at stake
fig5.add_trace(go.Bar(
    x=churn_summary["churn_status"],
    y=churn_summary["total_spent"],
    marker_color=churn_colors,
    text=[f"₱{v:,.0f}" for v in churn_summary["total_spent"]],
    textposition="outside",
    hovertemplate=(
        "<b>%{x}</b><br>"
        "Revenue: ₱%{y:,.0f}<extra></extra>"
    ),
    showlegend=False,
), row=1, col=2)

fig5.update_layout(
    title=dict(
        text="Churn Risk — 326 Customers Gone Silent 🚨",
        font=dict(size=15, color="#1E293B"), x=0.02
    ),
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg"],
    height=480,
    margin=dict(l=40, r=40, t=70, b=80),
    font=dict(size=11),
)
fig5.update_yaxes(
    gridcolor=COLORS["grid"],
    tickprefix="₱", tickformat=",.0f",
    row=1, col=2
)
fig5.update_yaxes(gridcolor=COLORS["grid"], row=1, col=1)
fig5.update_xaxes(showgrid=False, tickangle=15)

save_chart(fig5, "chart5_churn_risk")


# =============================================================================
# CHART 6 — TOP 10 PRODUCTS BY REVENUE
# Simple but essential. Jess needs to know what's actually selling.
# =============================================================================

print("\n[CHART 6] Top 10 Products...")

top_sorted = top_products.sort_values("total_revenue", ascending=True)
prod_colors = [
    COLORS["primary"] if i >= len(top_sorted) - 3
    else COLORS["teal"]
    for i in range(len(top_sorted))
]

fig6 = go.Figure(go.Bar(
    x=top_sorted["total_revenue"],
    y=top_sorted["product_name"],
    orientation="h",
    marker_color=prod_colors,
    text=[f"₱{v:,.0f}" for v in top_sorted["total_revenue"]],
    textposition="outside",
    hovertemplate=(
        "<b>%{y}</b><br>"
        "Revenue: ₱%{x:,.0f}<extra></extra>"
    ),
))

fig6.update_layout(
    title=dict(
        text="Top 10 Products by Revenue",
        font=dict(size=15, color="#1E293B"), x=0.02
    ),
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg"],
    height=500,
    margin=dict(l=60, r=40, t=70, b=60),
    font=dict(family="sans-serif", size=11, color="#475569"),
)
fig6.update_xaxes(
    title="Total Revenue (₱)",
    tickprefix="₱", tickformat=",.0f",
    showgrid=True, gridcolor=COLORS["grid"],
    range=[0, top_sorted["total_revenue"].max() * 1.3]
)
fig6.update_yaxes(showgrid=False)
save_chart(fig6, "chart6_top_products")


# =============================================================================
# DONE
# =============================================================================

all_files = sorted(os.listdir(CHARTS_DIR))
print("\n" + "=" * 60)
print("  ALL CHARTS GENERATED")
print(f"\n  HTML (interactive):")
for f in all_files:
    if f.endswith(".html"):
        print(f"    → {f}")
print(f"\n  PNG (static):")
for f in all_files:
    if f.endswith(".png"):
        print(f"    → {f}")
print("=" * 60)
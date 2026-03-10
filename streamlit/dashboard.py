"""
Medallion Lakehouse — Real-Time Streamlit Dashboard
Reads from postgres-analytics gold schema and refreshes every N seconds.
"""
import os, time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Medallion Lakehouse Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

REFRESH = int(os.getenv("REFRESH_INTERVAL_SECONDS", 5))

DB_URL = (
    f"postgresql://{os.getenv('POSTGRES_ANALYTICS_USER')}:"
    f"{os.getenv('POSTGRES_ANALYTICS_PASSWORD')}@"
    f"{os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres-analytics')}:5432/"
    f"{os.getenv('POSTGRES_ANALYTICS_DB')}"
)

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)

def query(sql):
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()

# ── Sidebar ───────────────────────────────────────────────────────────────
st.sidebar.title("⚙️ Controls")
lookback = st.sidebar.slider("Lookback (hours)", 1, 48, 24)
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
st.sidebar.markdown(f"Refresh: every **{REFRESH}s**")

# ── Header ────────────────────────────────────────────────────────────────
st.title("🏦 Medallion Lakehouse — Live Financial Dashboard")
st.caption("Batch UPI Transactions + Real-Time Credit Card Swipes → Bronze → Silver → Gold → PostgreSQL")

# ── KPI Row ───────────────────────────────────────────────────────────────
kpi_df = query(f"""
    SELECT payment_type,
           SUM(txn_count)   AS txn_count,
           SUM(volume_usd)  AS volume_usd,
           AVG(failure_rate) AS failure_rate
    FROM gold.gold_combined_kpis
    WHERE metric_hour >= NOW() - INTERVAL '{lookback} hours'
    GROUP BY payment_type
""")

col1, col2, col3, col4 = st.columns(4)
for _, row in kpi_df.iterrows():
    ptype = row["payment_type"]
    if ptype == "UPI":
        col1.metric("UPI Transactions", f"{int(row['txn_count']):,}")
        col2.metric("UPI Volume (USD)", f"${row['volume_usd']:,.0f}")
    else:
        col3.metric("CC Swipes", f"{int(row['txn_count']):,}")
        col4.metric("CC Volume (USD)", f"${row['volume_usd']:,.0f}")

st.divider()

# ── Charts Row 1 ──────────────────────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader("📊 UPI Volume by Hour")
    upi_df = query(f"""
        SELECT txn_hour, sender_bank, SUM(total_volume_inr) AS vol
        FROM gold.gold_upi_hourly
        WHERE txn_hour >= NOW() - INTERVAL '{lookback} hours'
        GROUP BY 1,2 ORDER BY 1
    """)
    if not upi_df.empty:
        fig = px.bar(upi_df, x="txn_hour", y="vol", color="sender_bank",
                     title="UPI Volume (INR) by Bank")
        st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("💳 CC Swipes by Card Type")
    cc_df = query(f"""
        SELECT swipe_hour, card_type, SUM(total_swipes) AS swipes
        FROM gold.gold_cc_hourly
        WHERE swipe_hour >= NOW() - INTERVAL '{lookback} hours'
        GROUP BY 1,2 ORDER BY 1
    """)
    if not cc_df.empty:
        fig = px.area(cc_df, x="swipe_hour", y="swipes", color="card_type",
                      title="CC Swipes by Card Type")
        st.plotly_chart(fig, use_container_width=True)

# ── Charts Row 2 ──────────────────────────────────────────────────────────
c3, c4 = st.columns(2)

with c3:
    st.subheader("🚨 Fraud Rate Over Time")
    fraud_df = query(f"""
        SELECT swipe_hour, AVG(fraud_rate_pct) AS fraud_rate
        FROM gold.gold_cc_hourly
        WHERE swipe_hour >= NOW() - INTERVAL '{lookback} hours'
        GROUP BY 1 ORDER BY 1
    """)
    if not fraud_df.empty:
        fig = px.line(fraud_df, x="swipe_hour", y="fraud_rate",
                      title="CC Fraud Rate (%)", color_discrete_sequence=["red"])
        fig.add_hline(y=2.0, line_dash="dash", line_color="orange",
                      annotation_text="2% threshold")
        st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("📂 UPI by Category")
    cat_df = query(f"""
        SELECT category, SUM(total_transactions) AS txns
        FROM gold.gold_upi_hourly
        WHERE txn_hour >= NOW() - INTERVAL '{lookback} hours'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    if not cat_df.empty:
        fig = px.pie(cat_df, names="category", values="txns", title="UPI by Category")
        st.plotly_chart(fig, use_container_width=True)

# ── Live feed ─────────────────────────────────────────────────────────────
st.divider()
st.subheader("⚡ Latest CC Swipes (Live)")
live_cc = query("""
    SELECT event_time, card_type, merchant_name, amount_usd, txn_status,
           fraud_tier, city, country
    FROM silver.cc_swipes
    ORDER BY event_time DESC LIMIT 20
""")
if not live_cc.empty:
    st.dataframe(live_cc.style.applymap(
        lambda v: "background-color: #ffcccc" if v == "HIGH" else "",
        subset=["fraud_tier"]), use_container_width=True)

st.caption(f"Last refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")

if auto_refresh:
    time.sleep(REFRESH)
    st.rerun()
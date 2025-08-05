import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import plotly.express as px

# Initialize Snowpark session
session = Session.get_active_session()
   
if st.button("🔄 Refresh Now"):
    st.rerun()

# Cached data loading functions (auto-refresh every 30s)
@st.cache_data(ttl=30)
def get_latest():
    return session.table("vw_latest_stock_prices").to_pandas()

@st.cache_data(ttl=30)
def get_moving_avg():
    return session.table("dt_moving_avg").to_pandas()

@st.cache_data(ttl=30)
def get_anomalies():
    return session.table("dt_price_anomalies").to_pandas()

@st.cache_data(ttl=30)
def get_leaderboard():
    return session.table("vw_price_leaderboard").to_pandas()

# Load data
latest = get_latest()
moving_avg = get_moving_avg()
anomalies = get_anomalies()
leaderboard = get_leaderboard()

# Streamlit UI components
st.title("📈 Yahoo Finance Dashboard")

# Latest prices
st.subheader("💲 Latest Prices")
st.dataframe(latest)

# Leaderboard
st.subheader("🏆 Price Leaderboard")
st.dataframe(leaderboard)


# Moving Average Chart
st.subheader("📉 5-Minutes Moving Averages")

# Date filter
min_date = pd.to_datetime(moving_avg["MINUTE_BUCKET"]).min().to_pydatetime()
max_date = pd.to_datetime(moving_avg["MINUTE_BUCKET"]).max().to_pydatetime()

# Set date slider
date_range = st.slider(
    "Date Range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="YYYY-MM-DD HH:mm"
)

# Set price filter
price_range = st.slider("Price Range", float(moving_avg["AVG_PRICE_5MIN"].min()), 
                        float(moving_avg["AVG_PRICE_5MIN"].max()), 
                        (0.0, float(moving_avg["AVG_PRICE_5MIN"].max())))

# Symbol multi-select
available_symbols = sorted(moving_avg['SYMBOL'].unique().tolist())
selected_symbols = st.multiselect("Select Symbols", options=available_symbols, 
                                  default=available_symbols[:2])

# Filter the DataFrame
start_date, end_date = date_range

filtered_data = moving_avg[
    (moving_avg["SYMBOL"].isin(selected_symbols)) &
    (moving_avg["MINUTE_BUCKET"] >= start_date) &
    (moving_avg["MINUTE_BUCKET"] <= end_date) &
    (moving_avg["AVG_PRICE_5MIN"] >= price_range[0]) &
    (moving_avg["AVG_PRICE_5MIN"] <= price_range[1])
]

fig = px.line(
    filtered_data.sort_values(["SYMBOL", "MINUTE_BUCKET"]),
    x="MINUTE_BUCKET",
    y="AVG_PRICE_5MIN",
    color="SYMBOL",
    title="Multi-Symbol 5-Minutes Moving Averages", 
    render_mode='svg'
)
fig.update_layout(xaxis_title="Time", yaxis_title="5-Min Avg Price", height=500)
st.plotly_chart(fig, use_container_width=True)

# Anomalies
st.subheader("🚨 Anomalies")
st.dataframe(anomalies[anomalies['SYMBOL'].isin(selected_symbols)])

# Dashboard SQL (Copyable)
st.markdown("### 🧾 Dashboard SQL")

sql_script = """
-- Latest Price
CREATE OR REPLACE VIEW vw_latest_stock_prices AS
SELECT symbol, price, time
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) AS rn
  FROM stock_prices
)
WHERE rn = 1;

-- 5-Min Moving Average
CREATE OR REPLACE DYNAMIC TABLE dt_moving_avg
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH recent AS (
  SELECT 
    symbol,
    DATE_TRUNC('minute', time::TIMESTAMP_NTZ) AS minute_bucket,    
    AVG(price) OVER (PARTITION BY symbol ORDER BY minute_bucket ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_price_5min
  FROM stock_prices
)
SELECT *
FROM recent;

-- Price Anomalies
CREATE OR REPLACE DYNAMIC TABLE dt_price_anomalies
TARGET_LAG = '1 minute'
WAREHOUSE = COMPUTE_WH
AS
WITH recent AS (
  SELECT 
    symbol,
    price,
    time,
    AVG(price) OVER (PARTITION BY symbol ORDER BY time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_avg
  FROM stock_prices
)
SELECT *
FROM recent
WHERE ABS(price - moving_avg) / NULLIF(moving_avg, 0) > 0.05;

-- Leaderboard by Latest Price
CREATE OR REPLACE VIEW vw_price_leaderboard AS
SELECT symbol, price, RANK() OVER (ORDER BY price DESC) AS price_rank
FROM (
  SELECT symbol, price
  FROM stock_prices
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time DESC) = 1
);
"""

with st.expander("📋 SQL Script"):
    st.code(sql_script, language="sql")















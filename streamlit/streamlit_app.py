import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark import Session

# Initialize Snowpark session
#session = Session.builder.configs(st.secrets["snowflake"]).create
session = Session.get_active_session()

# Load data
latest = session.table("vw_latest_stock_prices").to_pandas()
moving_avg = session.table("vw_5min_moving_avg").to_pandas()
anomalies = session.table("vw_price_anomalies").to_pandas()
hourly = session.table("vw_hourly_avg_prices").to_pandas()
leaderboard = session.table("vw_price_leaderboard").to_pandas()

# Streamlit UI components
st.title("📈 Yahoo Finance Dashboard")
st.dataframe(latest)
st.dataframe(leaderboard)

# Moving Average Chart
symbol = st.selectbox("Select Symbol", moving_avg['SYMBOL'].unique())
filtered_data = moving_avg[moving_avg['SYMBOL'] == symbol]

chart = alt.Chart(filtered_data).mark_line(point=True).encode(
    x=alt.X('MINUTE_BUCKET:T', title='Time'),
    y=alt.Y('AVG_PRICE_5MIN:Q', title='5-Min Avg Price'),
    tooltip=['MINUTE_BUCKET:T', 'AVG_PRICE_5MIN:Q']
).properties(
    title=f"5-Minute Average for {symbol}",
    height=300
)

st.altair_chart(chart)

# Alternative to altair

import plotly.express as px

fig = px.line(
    filtered_data,
    x="MINUTE_BUCKET",
    y="AVG_PRICE_5MIN",
    title=f"5-Minute Average for {symbol}"
)
st.plotly_chart(fig, use_container_width=True)


# Anomalies Table
st.dataframe(anomalies[anomalies['SYMBOL'] == symbol])





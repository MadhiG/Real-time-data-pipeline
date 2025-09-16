# streamlit_app.py
"""Streamlit dashboard to view stored stock & weather data
Run locally: streamlit run streamlit_app.py
"""

import os
import pandas as pd
import streamlit as st
from db import get_engine
import matplotlib.pyplot as plt

st.set_page_config(page_title="Realtime Stock & Weather Dashboard", layout="wide")

st.title("Realtime Stock & Weather Dashboard")

engine = get_engine()

# Sidebar controls
st.sidebar.header("Configuration")
symbols = st.sidebar.text_input("Stock symbols (comma-separated)", value=os.getenv("STOCK_SYMBOLS","AAPL,MSFT,GOOGL"))
loc_name = st.sidebar.text_input("Weather Location Name", value=os.getenv("WEATHER_LOCATION_NAME","Chennai,India"))

@st.cache_data(ttl=60)
def load_stock_data(limit=5000):
    q = "SELECT * FROM stocks ORDER BY timestamp DESC LIMIT :limit"
    df = pd.read_sql(q, con=engine, params={"limit": limit})
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@st.cache_data(ttl=60)
def load_weather_data(limit=500):
    q = "SELECT * FROM weather ORDER BY timestamp DESC LIMIT :limit"
    df = pd.read_sql(q, con=engine, params={"limit": limit})
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

stock_df = load_stock_data()
weather_df = load_weather_data()

st.subheader("Latest Stock Samples")
if stock_df.empty:
    st.write("No stock data yet. Run the ingestion script or wait for scheduled job.")
else:
    st.dataframe(stock_df.head(20))

st.subheader("Latest Weather Samples")
if weather_df.empty:
    st.write("No weather data yet.")
else:
    st.dataframe(weather_df.head(20))

# Simple chart: recent closing price for selected symbol
st.subheader("Stock Closing Price (recent)")
selected_symbol = st.selectbox("Choose symbol", options=sorted(stock_df['symbol'].unique().tolist()) if not stock_df.empty else symbols.split(","))
if not stock_df.empty and selected_symbol:
    s = stock_df[stock_df['symbol'] == selected_symbol].sort_values('timestamp')
    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(s['timestamp'], s['close'])
    ax.set_title(f"{selected_symbol} close price")
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Close")
    st.pyplot(fig)

st.subheader("Weather Trends (temperature recent)")
if not weather_df.empty:
    w = weather_df.sort_values('timestamp').tail(72)  # last 72 hours (if available)
    fig2, ax2 = plt.subplots(figsize=(10,4))
    ax2.plot(w['timestamp'], w['temperature'])
    ax2.set_title(f"Temperature - {loc_name}")
    ax2.set_xlabel("Timestamp")
    ax2.set_ylabel("Temperature (°C)")
    st.pyplot(fig2)

st.markdown("---")
st.markdown("**Note:** This demo uses the DB configured by `DB_URL` environment variable. Default is local SQLite `data.db`.")

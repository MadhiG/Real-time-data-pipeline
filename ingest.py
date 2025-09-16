# ingest.py
"""Ingest script to fetch stock & weather data, transform, and load into DB.
Usage: python ingest.py
You can set environment variables:
 - DB_URL (optional, default sqlite:///data.db)
 - STOCK_SYMBOLS (comma-separated, default AAPL,MSFT,GOOGL)
 - WEATHER_COORDS (lat,lon; default 13.0827,80.2707 => Chennai)
"""

import os
import pandas as pd
import yfinance as yf
import requests
from db import init_db, insert_df

# Configs (read from env)
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
WEATHER_COORDS = os.getenv("WEATHER_COORDS", "13.0827,80.2707")  # default Chennai
WEATHER_LOCATION_NAME = os.getenv("WEATHER_LOCATION_NAME", "Chennai,India")
OPEN_METEO_BASE = "https://api.open-meteo.com/v1/forecast"

def fetch_stocks(symbols):
    all_rows = []
    for s in symbols:
        s = s.strip().upper()
        try:
            df = yf.download(tickers=s, period="1d", interval="5m", progress=False, threads=False)
            if df.empty:
                df = yf.download(tickers=s, period="5d", interval="1d", progress=False, threads=False)
            df = df.reset_index()
            df['symbol'] = s
            df = df.rename(columns={
                'Datetime': 'timestamp',
                'Date': 'timestamp',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            sup = ['timestamp','open','high','low','close','volume','symbol']
            df = df[[c for c in sup if c in df.columns]]
            all_rows.append(df)
        except Exception as e:
            print(f"Error fetching {s}: {e}")
    if all_rows:
        res = pd.concat(all_rows, ignore_index=True)
        if pd.api.types.is_datetime64_any_dtype(res['timestamp']):
            res['timestamp'] = pd.to_datetime(res['timestamp']).dt.tz_localize(None)
        return res
    return pd.DataFrame()

def fetch_weather(coords):
    lat, lon = coords.split(",")
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "UTC"
    }
    try:
        r = requests.get(OPEN_METEO_BASE, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
        hourly = j.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        prec = hourly.get("precipitation", [])
        wind = hourly.get("windspeed_10m", [])
        rows = []
        for t, temp, p, w in zip(times, temps, prec, wind):
            rows.append({
                "timestamp": pd.to_datetime(t),
                "temperature": temp,
                "precipitation": p,
                "windspeed": w,
                "location": os.getenv("WEATHER_LOCATION_NAME", "Unknown")
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print("Weather fetch error:", e)
        return pd.DataFrame()

def transform_stock_df(df):
    df = df.rename(columns={ 'symbol':'symbol','timestamp':'timestamp','open':'open','high':'high','low':'low','close':'close','volume':'volume'})
    df = df.dropna(subset=['timestamp'])
    for c in ['open','high','low','close','volume']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    wanted = ['symbol','timestamp','open','high','low','close','volume']
    df = df[[c for c in wanted if c in df.columns]]
    return df

def transform_weather_df(df):
    if df.empty:
        return df
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
    df['windspeed'] = pd.to_numeric(df['windspeed'], errors='coerce')
    df = df[['location','timestamp','temperature','windspeed','precipitation']]
    return df

def main():
    print("Initializing DB...")
    init_db()
    print("Fetching stocks:", STOCK_SYMBOLS)
    stock_df = fetch_stocks(STOCK_SYMBOLS)
    if not stock_df.empty:
        s_df = transform_stock_df(stock_df)
        print("Inserting stocks:", len(s_df), "rows")
        insert_df("stocks", s_df)
    else:
        print("No stock data fetched.")
    print("Fetching weather for:", WEATHER_COORDS)
    w_df = fetch_weather(WEATHER_COORDS)
    if not w_df.empty:
        w_df = transform_weather_df(w_df)
        print("Inserting weather rows:", len(w_df))
        insert_df("weather", w_df)
    else:
        print("No weather data fetched.")
    print("Done.")

if __name__ == "__main__":
    main()

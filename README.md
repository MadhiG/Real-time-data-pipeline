# Real-time Stock & Weather Data Pipeline (Demo)

This repository contains a simple data pipeline that fetches stock data (via yfinance) and weather data (via Open-Meteo), stores them in a database (SQLite or PostgreSQL) and exposes a Streamlit dashboard.

## Files
- ingest.py        # fetch -> transform -> load
- db.py            # database helpers (SQLite or PostgreSQL via DB_URL)
- streamlit_app.py # Streamlit dashboard
- requirements.txt
- .github/workflows/ingest.yml # GitHub Actions scheduled ingestion

## Quick start (local demo)
1. Clone repo
2. Create venv & install:
   ```
   python -m venv venv
   source venv/bin/activate   # mac/linux
   venv\Scripts\activate      # windows
   pip install -r requirements.txt
   ```
3. Run single ingestion:
   ```
   python ingest.py
   ```
   Default: SQLite `data.db` will be created locally.

4. Run dashboard:
   ```
   streamlit run streamlit_app.py
   ```

## Using PostgreSQL (production/demo on cloud)
Set `DB_URL` environment variable to a valid SQLAlchemy URL, for example:
```
postgresql+psycopg2://username:password@host:port/dbname
```
You can use free hosted PostgreSQL providers (e.g., ElephantSQL) for demo.

## Schedule ingestion on GitHub Actions
- Add repo secrets:
  - `DB_URL` (optional) if using remote DB
  - `STOCK_SYMBOLS` (optional), `WEATHER_COORDS`, `WEATHER_LOCATION_NAME`
- The included workflow runs hourly (UTC). You can trigger manually via "Actions" → "Run workflow".

## Deploy Streamlit app (free)
- Push repo to GitHub.
- Go to https://streamlit.io/cloud and connect your GitHub.
- Create a new app pointing to `streamlit_app.py` in your repo and set any environment variables.
- Streamlit Cloud will deploy the dashboard.

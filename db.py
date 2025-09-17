# db.py
"""Database helper: works with SQLite (default) or PostgreSQL via DB_URL env variable."""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd

# Default to SQLite if DB_URL not provided
DB_URL = os.getenv("DB_URL", "sqlite:///data.db")

def get_engine() -> Engine:
    return create_engine(DB_URL, future=True)

def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        # Stocks table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        );
        """))
        # Weather table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            temperature REAL,
            windspeed REAL,
            precipitation REAL
        );
        """))
    print("Initialized DB (tables created if not exists).")

def insert_df(table_name: str, df: pd.DataFrame):
    engine = get_engine()
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

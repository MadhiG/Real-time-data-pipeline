# db.py
"""Database helper: supports SQLite (default) or PostgreSQL via DB_URL env variable."""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import pandas as pd

DB_URL = os.getenv("DB_URL", "sqlite:///data.db")  # default SQLite file: data.db

def get_engine() -> Engine:
    engine = create_engine(DB_URL, future=True)
    return engine

def init_db():
    engine = get_engine()
    with engine.begin() as conn:
        # stock table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stocks (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC
        );
        """))
        # weather table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weather (
            id SERIAL PRIMARY KEY,
            location TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            temperature NUMERIC,
            windspeed NUMERIC,
            precipitation NUMERIC
        );
        """))
    print("Initialized DB (tables created if not exists).")

def insert_df(table_name: str, df: pd.DataFrame):
    engine = get_engine()
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

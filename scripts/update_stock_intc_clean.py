import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

TICKER = "INTC"
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
FILE_PATH = f"{OUTPUT_DIR}/{TICKER}_daily_clean.csv"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df


def load_fresh_data() -> pd.DataFrame:
    end = datetime.today()
    start = end - timedelta(days=365)
    df = yf.download(TICKER, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if df.empty:
        raise ValueError("No data fetched. Check ticker.")

    df = normalize_columns(df)
    keep = ["Open", "High", "Low", "Close", "Volume"]
    df = df[keep].reset_index()
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    return df


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].round(2)
    df["volume"] = df["volume"].round(0).astype("int64")
    df = df.drop_duplicates(subset=["date"]).sort_values("date")
    return df[["date", "open", "high", "low", "close", "volume"]]


def main() -> None:
    fresh = clean_frame(load_fresh_data())

    if os.path.exists(FILE_PATH):
        old = pd.read_csv(FILE_PATH)
        old = clean_frame(old)
        fresh = pd.concat([old, fresh], ignore_index=True)
        fresh = fresh.drop_duplicates(subset=["date"], keep="last").sort_values("date")

    fresh.to_csv(FILE_PATH, index=False)
    print(f"Updated {FILE_PATH}")


if __name__ == "__main__":
    main()

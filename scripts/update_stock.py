import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

TICKER = "__SET_TICKER__"  # <-- change this

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

FILE_PATH = f"{OUTPUT_DIR}/{TICKER.upper()}_daily.csv"

end = datetime.today()
start = end - timedelta(days=365)

# Download data

df = yf.download(TICKER, start=start, end=end, interval="1d", auto_adjust=False)

if df.empty:
    raise ValueError("No data fetched. Check ticker.")

# Keep only OHLCV

df = df[["Open", "High", "Low", "Close", "Volume"]]

df.reset_index(inplace=True)

df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

# If file exists, merge
if os.path.exists(FILE_PATH):
    old = pd.read_csv(FILE_PATH)
    df = pd.concat([old, df])
    df = df.drop_duplicates(subset=["date"]).sort_values("date")

# Save

df.to_csv(FILE_PATH, index=False)

print(f"Updated {FILE_PATH}")

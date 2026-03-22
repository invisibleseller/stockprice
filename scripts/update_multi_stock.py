import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")
TICKERS_FILE = os.path.join(BASE_DIR, "tickers.txt")

os.makedirs(DATA_DIR, exist_ok=True)


def safe_name(ticker: str) -> str:
    return (
        ticker.replace("^", "")
        .replace("/", "_")
        .replace("-", "_")
        .replace("=", "_")
        .replace(".", "_")
    )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df


def load_tickers():
    with open(TICKERS_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


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


def update_one_ticker(ticker: str) -> None:
    end = datetime.today()
    start = end - timedelta(days=365)

    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        print(f"Skipped {ticker}: no data fetched")
        return

    df = normalize_columns(df)
    df = df[["Open", "High", "Low", "Close", "Volume"]].reset_index()
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    df = clean_frame(df)

    file_name = f"{safe_name(ticker)}_daily_clean.csv"
    file_path = os.path.join(DATA_DIR, file_name)

    if os.path.exists(file_path):
        old = pd.read_csv(file_path)
        old = clean_frame(old)
        df = pd.concat([old, df], ignore_index=True)
        df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date")

    df.to_csv(file_path, index=False)
    print(f"Saved {file_path}")


def main() -> None:
    for ticker in load_tickers():
        update_one_ticker(ticker)


if __name__ == "__main__":
    main()

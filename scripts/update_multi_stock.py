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


def wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    rsi = pd.Series(index=close.index, dtype="float64")
    if len(close) < period + 1:
        return rsi

    avg_gain = gain.iloc[1:period + 1].mean()
    avg_loss = loss.iloc[1:period + 1].mean()

    if avg_loss == 0 and avg_gain == 0:
        rsi.iloc[period] = 50.0
    elif avg_loss == 0:
        rsi.iloc[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi.iloc[period] = 100 - 100 / (1 + rs)

    for i in range(period + 1, len(close)):
        current_gain = gain.iloc[i]
        current_loss = loss.iloc[i]
        avg_gain = (avg_gain * (period - 1) + current_gain) / period
        avg_loss = (avg_loss * (period - 1) + current_loss) / period

        if avg_loss == 0 and avg_gain == 0:
            rsi.iloc[i] = 50.0
        elif avg_loss == 0:
            rsi.iloc[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi.iloc[i] = 100 - 100 / (1 + rs)

    return rsi


def wilder_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = pd.Series(index=close.index, dtype="float64")
    if len(close) < period + 1:
        return atr

    first_atr = tr.iloc[1:period + 1].mean()
    atr.iloc[period] = first_atr

    prev_atr = first_atr
    for i in range(period + 1, len(close)):
        prev_atr = (prev_atr * (period - 1) + tr.iloc[i]) / period
        atr.iloc[i] = prev_atr

    return atr


def ema_state(close: float, ema8: float, ema21: float, ema55: float, ema200: float) -> str:
    values = [close, ema8, ema21, ema55, ema200]
    if any(pd.isna(v) for v in values):
        return "混合"
    if close > ema8 > ema21 > ema55 > ema200:
        return "完整多头"
    if ema21 > ema55 and close > ema21 and close < ema200:
        return "部分多头"
    if close < ema8 < ema21 < ema55 < ema200:
        return "空头排列"
    return "混合"


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # SMA
    df["sma8"] = df["close"].rolling(8).mean()
    df["sma21"] = df["close"].rolling(21).mean()
    df["sma55"] = df["close"].rolling(55).mean()
    df["sma200"] = df["close"].rolling(200).mean()

    # EMA (Futu alignment)
    df["ema8"] = df["close"].ewm(span=8, adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema55"] = df["close"].ewm(span=55, adjust=False).mean()
    df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

    # RSI / ATR
    df["rsi14"] = wilder_rsi(df["close"], 14)
    df["atr14"] = wilder_atr(df["high"], df["low"], df["close"], 14)

    # Volume
    df["vol20_avg"] = df["volume"].rolling(20).mean()
    df["vol_ratio20"] = df["volume"] / df["vol20_avg"]

    # EMA state
    df["ema_state"] = [
        ema_state(c, e8, e21, e55, e200)
        for c, e8, e21, e55, e200 in zip(df["close"], df["ema8"], df["ema21"], df["ema55"], df["ema200"])
    ]

    # Distances
    df["close_vs_ema200_pct"] = ((df["close"] - df["ema200"]) / df["ema200"]) * 100
    df["close_vs_sma200_pct"] = ((df["close"] - df["sma200"]) / df["sma200"]) * 100

    # Validation fields
    sma200_start_dates = []
    sma200_end_dates = []
    sma200_counts = []
    rsi_start_dates = []

    dates = list(df["date"])

    for i in range(len(df)):
        if i >= 199:
            sma200_start_dates.append(dates[i - 199])
            sma200_end_dates.append(dates[i])
            sma200_counts.append(200)
        else:
            sma200_start_dates.append(pd.NA)
            sma200_end_dates.append(pd.NA)
            sma200_counts.append(pd.NA)

        if i >= 14:
            rsi_start_dates.append(dates[1])
        else:
            rsi_start_dates.append(pd.NA)

    df["sma200_start_date"] = sma200_start_dates
    df["sma200_end_date"] = sma200_end_dates
    df["sma200_window_count"] = sma200_counts
    df["rsi_start_date"] = rsi_start_dates
    df["close_check"] = df["close"]

    return df


def clean_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]

    numeric_cols = [
        "open", "high", "low", "close", "volume",
        "sma8", "sma21", "sma55", "sma200",
        "ema8", "ema21", "ema55", "ema200",
        "close_vs_ema200_pct", "close_vs_sma200_pct",
        "rsi14", "atr14",
        "vol20_avg", "vol_ratio20",
        "close_check"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "sma200_window_count" in df.columns:
        df["sma200_window_count"] = pd.to_numeric(df["sma200_window_count"], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["date"]).sort_values("date")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    price_cols = [
        "open", "high", "low", "close",
        "sma8", "sma21", "sma55", "sma200",
        "ema8", "ema21", "ema55", "ema200",
        "atr14", "close_check"
    ]
    pct_cols = ["close_vs_ema200_pct", "close_vs_sma200_pct"]
    one_dec_cols = ["rsi14", "vol_ratio20"]
    vol_avg_cols = ["vol20_avg"]

    for col in price_cols:
        if col in df.columns:
            df[col] = df[col].round(2)
    for col in pct_cols:
        if col in df.columns:
            df[col] = df[col].round(2)
    for col in one_dec_cols:
        if col in df.columns:
            df[col] = df[col].round(1)
    for col in vol_avg_cols:
        if col in df.columns:
            df[col] = df[col].round(0)

    df["volume"] = df["volume"].round(0).astype("int64")

    ordered_cols = [
        "date", "open", "high", "low", "close", "volume",
        "sma8", "sma21", "sma55", "sma200",
        "ema8", "ema21", "ema55", "ema200",
        "ema_state", "close_vs_ema200_pct", "close_vs_sma200_pct",
        "rsi14", "atr14",
        "vol20_avg", "vol_ratio20",
        "sma200_start_date", "sma200_end_date", "sma200_window_count",
        "rsi_start_date", "close_check"
    ]
    existing_cols = [c for c in ordered_cols if c in df.columns]
    return df[existing_cols]


def update_one_ticker(ticker: str) -> None:
    end = datetime.today()
    start = end - timedelta(days=500)

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

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].sort_values("date").reset_index(drop=True)
    df = add_indicators(df)
    df = clean_frame(df)

    file_name = f"{safe_name(ticker)}_daily_clean.csv"
    file_path = os.path.join(DATA_DIR, file_name)
    df.to_csv(file_path, index=False)
    print(f"Saved {file_path}")


def main() -> None:
    for ticker in load_tickers():
        update_one_ticker(ticker)


if __name__ == "__main__":
    main()

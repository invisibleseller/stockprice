import csv
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(BASE_DIR, "data")
OUT_DIR = os.path.join(BASE_DIR, "md")
TICKERS_FILE = os.path.join(BASE_DIR, "tickers.txt")

os.makedirs(OUT_DIR, exist_ok=True)

def safe_name(ticker: str) -> str:
    return (
        ticker.replace("^", "")
        .replace("/", "_")
        .replace("-", "_")
        .replace("=", "_")
        .replace(".", "_")
    )

with open(TICKERS_FILE, "r", encoding="utf-8") as f:
    tickers = [line.strip() for line in f if line.strip()]

for ticker in tickers:
    csv_path = os.path.join(DATA_DIR, f"{safe_name(ticker)}_daily_clean.csv")
    md_path = os.path.join(OUT_DIR, f"{safe_name(ticker)}_latest.md")

    if not os.path.exists(csv_path):
        continue

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if len(rows) < 3:
        continue

    rows = rows[1:]
    latest = rows[-1]
    prev = rows[-2]

    date, open_, high, low, close, volume = latest
    prev_close = float(prev[4])
    close_f = float(close)
    change = close_f - prev_close
    pct = (change / prev_close) * 100 if prev_close else 0

    with open(md_path, "w", encoding="utf-8") as out:
        out.write(f"# {ticker}\n\n")
        out.write(f"Latest: {close_f:.2f}\n")
        out.write(f"Change: {change:+.2f} ({pct:+.2f}%)\n")
        out.write(f"Volume: {int(volume):,}\n\n")
        out.write("---\n\n")
        out.write("## Full Data (Newest First)\n\n")
        out.write("| Date | Open | High | Low | Close | Volume |\n")
        out.write("|------|------|------|-----|-------|--------|\n")

        for row in reversed(rows):
            if len(row) >= 6:
                d, o, h, l, c, v = row[:6]
                out.write(f"| {d} | {o} | {h} | {l} | {c} | {v} |\n")

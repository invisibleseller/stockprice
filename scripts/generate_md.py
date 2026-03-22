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


def fmt_num(val, decimals=2):
    if val in ("", None):
        return ""
    try:
        return f"{float(val):.{decimals}f}"
    except Exception:
        return str(val)


def fmt_millions(val):
    if val in ("", None):
        return ""
    try:
        return f"{float(val)/1_000_000:.1f}M"
    except Exception:
        return str(val)


def position_text(val):
    if val in ("", None):
        return "数据不足"
    try:
        pct = float(val)
        if pct >= 0:
            return f"上方+{pct:.2f}%"
        return f"下方{pct:.2f}%"
    except Exception:
        return "数据不足"


with open(TICKERS_FILE, "r", encoding="utf-8") as f:
    tickers = [line.strip() for line in f if line.strip()]

for ticker in tickers:
    csv_path = os.path.join(DATA_DIR, f"{safe_name(ticker)}_daily_clean.csv")
    md_path = os.path.join(OUT_DIR, f"{safe_name(ticker)}_latest.md")

    if not os.path.exists(csv_path):
        continue

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if len(rows) < 2:
        continue

    header = rows[0]
    data_rows = rows[1:]
    latest = data_rows[-1]
    row_map = dict(zip(header, latest))

    prev = data_rows[-2] if len(data_rows) >= 2 else None
    prev_close = None
    if prev:
        prev_map = dict(zip(header, prev))
        try:
            prev_close = float(prev_map.get("close", ""))
        except Exception:
            prev_close = None

    try:
        close_f = float(row_map.get("close", ""))
    except Exception:
        close_f = None

    change = None
    pct = None
    if close_f is not None and prev_close not in (None, 0):
        change = close_f - prev_close
        pct = (change / prev_close) * 100

    with open(md_path, "w", encoding="utf-8") as out:
        out.write(f"# {ticker}\n\n")
        out.write(f"Latest: {close_f:.2f}\n" if close_f is not None else "Latest: \n")
        out.write(f"Change: {change:+.2f} ({pct:+.2f}%)\n" if change is not None and pct is not None else "Change: \n")
        out.write(f"Volume: {fmt_millions(row_map.get('volume'))}\n\n")
        out.write("---\n\n")

        out.write("## 均线系统\n\n")
        out.write(f"EMA8：{fmt_num(row_map.get('ema8'))}\n")
        out.write(f"EMA21：{fmt_num(row_map.get('ema21'))}\n")
        out.write(f"EMA55：{fmt_num(row_map.get('ema55'))}\n")
        out.write(f"EMA200：{fmt_num(row_map.get('ema200'))}\n")
        out.write(f"SMA8：{fmt_num(row_map.get('sma8'))}\n")
        out.write(f"SMA21：{fmt_num(row_map.get('sma21'))}\n")
        out.write(f"SMA55：{fmt_num(row_map.get('sma55'))}\n")
        out.write(f"SMA200：{fmt_num(row_map.get('sma200'))}\n")
        out.write(f"多头排列：{row_map.get('ema_state', '')}\n")
        out.write(f"收盘在EMA200：{position_text(row_map.get('close_vs_ema200_pct'))}\n")
        out.write(f"SMA200趋势参考：{position_text(row_map.get('close_vs_sma200_pct'))}\n\n")

        out.write("## 技术指标\n\n")
        out.write(f"RSI(14)：{fmt_num(row_map.get('rsi14'), 1)}\n")
        out.write(f"ATR(14)：{fmt_num(row_map.get('atr14'))}\n\n")

        out.write("## 成交量\n\n")
        out.write(f"今日：{fmt_millions(row_map.get('volume'))}\n")
        out.write(f"20日均量：{fmt_millions(row_map.get('vol20_avg'))}\n")
        ratio = row_map.get("vol_ratio20")
        if ratio not in ("", None):
            try:
                out.write(f"倍数：{float(ratio):.1f}倍\n\n")
            except Exception:
                out.write(f"倍数：{ratio}倍\n\n")
        else:
            out.write("倍数：\n\n")

        out.write("## 数据验证\n\n")
        start_date = row_map.get("sma200_start_date", "")
        end_date = row_map.get("sma200_end_date", "")
        window_count = row_map.get("sma200_window_count", "")
        if start_date and end_date and window_count:
            out.write(f"SMA200计算窗口：{start_date} 至 {end_date}（共{window_count}交易日）\n")
        else:
            out.write("SMA200计算窗口：数据不足，SMA200未计算\n")
        rsi_start = row_map.get("rsi_start_date", "")
        if rsi_start:
            out.write(f"RSI计算起始：{rsi_start}\n")
        else:
            out.write("RSI计算起始：数据不足，RSI未计算\n")
        out.write(f"收盘价核对：{fmt_num(row_map.get('close_check'))}\n\n")

        out.write("## Full Data (Newest First)\n\n")
        out.write("| Date | Open | High | Low | Close | Volume | EMA8 | EMA21 | EMA55 | EMA200 | SMA8 | SMA21 | SMA55 | SMA200 | 多头排列 | 收盘在EMA200 | SMA200参考 | RSI14 | ATR14 | 20日均量 | 量比 |\n")
        out.write("|------|------|------|-----|-------|--------|------|-------|-------|--------|------|-------|-------|--------|----------|-------------|------------|-------|-------|----------|------|\n")

        for row in reversed(data_rows):
            m = dict(zip(header, row))
            out.write(
                f"| {m.get('date','')} | {m.get('open','')} | {m.get('high','')} | {m.get('low','')} | {m.get('close','')} | {m.get('volume','')} | "
                f"{m.get('ema8','')} | {m.get('ema21','')} | {m.get('ema55','')} | {m.get('ema200','')} | "
                f"{m.get('sma8','')} | {m.get('sma21','')} | {m.get('sma55','')} | {m.get('sma200','')} | "
                f"{m.get('ema_state','')} | {position_text(m.get('close_vs_ema200_pct'))} | {position_text(m.get('close_vs_sma200_pct'))} | "
                f"{m.get('rsi14','')} | {m.get('atr14','')} | {m.get('vol20_avg','')} | {m.get('vol_ratio20','')} |\n"
            )

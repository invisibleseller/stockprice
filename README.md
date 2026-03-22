# stockprice

This repo tracks one stock's daily OHLCV data for Elliott Wave study.

## What is stored

Only these columns are kept:
- Date
- Open
- High
- Low
- Close
- Volume

## Setup

Edit `scripts/update_stock.py` and replace `__SET_TICKER__` with your ticker, for example `TSLA`.

## Output

The workflow writes to:
- `data/<TICKER>_daily.csv`

## Automation

A GitHub Actions workflow runs daily and can also be triggered manually.

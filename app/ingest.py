from __future__ import annotations

from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import yfinance as yf

from . import bq


def _default_start(symbol: str) -> datetime:
    # sensible defaults if table is empty
    if symbol.upper() == "BTC-USD":
        return datetime(2017, 1, 1)
    return datetime(2015, 1, 1)


def fetch_prices(symbol: str) -> pd.DataFrame:
    last = bq.max_date(symbol)
    start = datetime.combine(last + timedelta(days=1), datetime.min.time()) if last else _default_start(symbol)
    end = datetime.utcnow()

    if start.date() > end.date():
        return pd.DataFrame(columns=["date","open","high","low","close","adj_close","volume","load_ts"])  # up-to-date

    tkr = yf.Ticker(symbol)
    hist = tkr.history(start=start.date(), end=end.date(), interval="1d", auto_adjust=False)
    if hist.empty:
        return pd.DataFrame(columns=["date","open","high","low","close","adj_close","volume","load_ts"])  # nothing new

    hist = hist.reset_index().rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    hist["date"] = pd.to_datetime(hist["date"]).dt.date
    hist["load_ts"] = datetime.utcnow()
    return hist[["date","open","high","low","close","adj_close","volume","load_ts"]]


def compute_ma_predictions(df: pd.DataFrame, fast: int, slow: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["as_of","fast_ma","slow_ma","signal","next_day_signal","model_version","load_ts"])
    work = df.sort_values("date").copy()
    work["fast_ma"] = work["close"].rolling(window=fast, min_periods=1).mean()
    work["slow_ma"] = work["close"].rolling(window=slow, min_periods=1).mean()
    work["signal"] = (work["fast_ma"] > work["slow_ma"]).map(lambda x: "BUY" if x else "SELL")
    work["next_day_signal"] = work["signal"].shift(-1).fillna(method="ffill")
    out = work.rename(columns={"date": "as_of"})[
        ["as_of", "fast_ma", "slow_ma", "signal", "next_day_signal"]
    ]
    out["model_version"] = f"ma_{fast}_{slow}_v1"
    out["load_ts"] = datetime.utcnow()
    return out


def run_for_symbol(symbol: str, fast: int, slow: int) -> Tuple[int, int]:
    bq.ensure_price_table(symbol)
    bq.ensure_prediction_table(symbol)
    prices = fetch_prices(symbol)
    n_prices = 0
    n_preds = 0
    if not prices.empty:
        n_prices = bq.upsert_prices(symbol, prices)
        pred = compute_ma_predictions(prices, fast, slow)
        if not pred.empty:
            n_preds = bq.upsert_predictions(symbol, pred)
    return n_prices, n_preds


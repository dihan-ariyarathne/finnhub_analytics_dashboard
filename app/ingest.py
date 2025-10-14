from __future__ import annotations

from datetime import datetime, timedelta
from time import sleep
from typing import Tuple, Optional

import logging
import pandas as pd
import yfinance as yf
import requests
from . import config

try:
    import finnhub  # type: ignore
except Exception:  # library optional
    finnhub = None

from . import bq


def _default_start(symbol: str) -> datetime:
    # sensible defaults if table is empty
    if symbol.upper() == "BTC-USD":
        return datetime(2017, 1, 1)
    return datetime(2015, 1, 1)


def _requests_session() -> requests.Session:
    s = requests.Session()
    # Explicit User-Agent to avoid occasional 403/empty responses in server environments
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    })
    return s


def fetch_prices(symbol: str, start_override: Optional[datetime] = None, end_override: Optional[datetime] = None) -> pd.DataFrame:
    logger = logging.getLogger(__name__)
    last = bq.max_date(symbol)
    start = start_override or (datetime.combine(last + timedelta(days=1), datetime.min.time()) if last else _default_start(symbol))
    end = end_override or datetime.utcnow()

    if start.date() > end.date():
        logger.info("%s up-to-date; start=%s end=%s", symbol, start.date(), end.date())
        return pd.DataFrame(columns=["date","open","high","low","close","adj_close","volume","load_ts"])  # up-to-date

    # Special handling for BTC-USD via Finnhub if API key present
    if symbol.upper() == "BTC-USD" and config.FINNHUB_API_KEY and finnhub is not None:
        try:
            df_fh = _fetch_btc_finnhub(start, end)
            if not df_fh.empty:
                logger.info("Finnhub fetched %d rows for BTC-USD", len(df_fh))
                return df_fh
        except Exception as e:
            logger.warning("Finnhub crypto fetch failed: %s; will try Yahoo", e)

    sess = _requests_session()
    tries = 3
    hist = pd.DataFrame()
    for attempt in range(1, tries + 1):
        try:
            # yfinance.download is more robust than Ticker.history in headless environments
            hist = yf.download(
                tickers=symbol,
                start=start.date(),
                end=end.date(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                session=sess,
            )
        except Exception as e:
            logger.warning("yfinance download failed for %s (attempt %d/%d): %s", symbol, attempt, tries, e)
        if not hist.empty:
            break
        sleep(1 * attempt)

    if hist.empty:
        logger.warning("Yahoo returned empty for %s; trying stooq fallback", symbol)
        try:
            import pandas_datareader.data as pdr

            stooq_symbol = symbol
            if symbol.upper() == "BTC-USD":
                stooq_symbol = "BTCUSD"
            sdf = pdr.DataReader(stooq_symbol, "stooq", start=start.date(), end=end.date())
            if not sdf.empty:
                sdf = sdf.sort_index()
                sdf = sdf.reset_index().rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                })
                sdf["adj_close"] = sdf["close"]
                sdf["date"] = pd.to_datetime(sdf["date"]).dt.date
                sdf["load_ts"] = datetime.utcnow()
                logger.info("Stooq fallback fetched %d rows for %s", len(sdf), symbol)
                return sdf[["date","open","high","low","close","adj_close","volume","load_ts"]]
        except Exception as e:
            logger.warning("Stooq fallback failed for %s: %s", symbol, e)
        logger.error("No data returned for %s; start=%s end=%s", symbol, start.date(), end.date())
        return pd.DataFrame(columns=["date","open","high","low","close","adj_close","volume","load_ts"])  # nothing new


def _fetch_btc_finnhub(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch BTC-USD daily candles from Finnhub (requires FINNHUB_API_KEY)."""
    assert config.FINNHUB_API_KEY
    cli = finnhub.Client(api_key=config.FINNHUB_API_KEY)  # type: ignore
    # Use Coinbase spot market which denominates in USD
    symbol = "COINBASE:BTC-USD"
    fr = int(datetime(start.year, start.month, start.day).timestamp())
    to = int(datetime(end.year, end.month, end.day).timestamp())
    data = cli.crypto_candles(symbol, "D", fr, to)
    if not data or data.get("s") != "ok":
        return pd.DataFrame()
    import pandas as pd  # local import to keep namespace tidy

    df = pd.DataFrame({
        "date": pd.to_datetime(data["t"], unit="s").date,
        "open": data["o"],
        "high": data["h"],
        "low": data["l"],
        "close": data["c"],
        "volume": data.get("v", [None] * len(data["c"]))
    })
    df["adj_close"] = df["close"]
    df["load_ts"] = datetime.utcnow()
    return df[["date","open","high","low","close","adj_close","volume","load_ts"]]

    # Ensure columns are flat and named consistently
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)

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
    logger.info("Fetched %d rows for %s (%s -> %s)", len(hist), symbol, start.date(), end.date())
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


def backfill_symbol(symbol: str, fast: int, slow: int, start_date: datetime, end_date: Optional[datetime] = None) -> Tuple[int, int]:
    bq.ensure_price_table(symbol)
    bq.ensure_prediction_table(symbol)
    prices = fetch_prices(symbol, start_override=start_date, end_override=end_date)
    n_prices = 0
    n_preds = 0
    if not prices.empty:
        n_prices = bq.upsert_prices(symbol, prices)
        pred = compute_ma_predictions(prices, fast, slow)
        if not pred.empty:
            n_preds = bq.upsert_predictions(symbol, pred)
    return n_prices, n_preds

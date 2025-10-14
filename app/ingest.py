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

    # Special handling for BTC-USD: try Alpha Vantage (if key), then Finnhub (if key), then Binance, then Yahoo
    if symbol.upper() == "BTC-USD":
        if config.ALPHAVANTAGE_API_KEY:
            df_av = _fetch_btc_alphavantage(start, end)
            if not df_av.empty:
                logger.info("AlphaVantage fetched %d rows for BTC-USD", len(df_av))
                return df_av
        if config.FINNHUB_API_KEY and finnhub is not None:
            try:
                df_fh = _fetch_btc_finnhub(start, end)
                if not df_fh.empty:
                    logger.info("Finnhub fetched %d rows for BTC-USD", len(df_fh))
                    return df_fh
            except Exception as e:
                logger.warning("Finnhub crypto fetch failed: %s; trying Binance", e)
        # Binance public market data (USDT treated ~ USD)
        df_bi = _fetch_btc_binance(start, end)
        if not df_bi.empty:
            logger.info("Binance fetched %d rows for BTC-USD (USDT)", len(df_bi))
            return df_bi

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


def _fetch_btc_alphavantage(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch BTC-USD daily from Alpha Vantage if API key is set.

    API: https://www.alphavantage.co/documentation/#digital-currency-daily
    """
    import pandas as pd
    key = config.ALPHAVANTAGE_API_KEY
    if not key:
        return pd.DataFrame()
    url = (
        "https://www.alphavantage.co/query"
        "?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey=" + key
    )
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return pd.DataFrame()
    js = resp.json()
    data = js.get("Time Series (Digital Currency Daily)", {})
    if not data:
        return pd.DataFrame()
    recs = []
    for d, vals in data.items():
        # AV keys: '1a. open (USD)', '2a. high (USD)', '3a. low (USD)', '4a. close (USD)', '5. volume'
        try:
            recs.append({
                "date": pd.to_datetime(d).date(),
                "open": float(vals.get("1a. open (USD)", 0.0)),
                "high": float(vals.get("2a. high (USD)", 0.0)),
                "low": float(vals.get("3a. low (USD)", 0.0)),
                "close": float(vals.get("4a. close (USD)", 0.0)),
                "adj_close": float(vals.get("4a. close (USD)", 0.0)),
                "volume": float(vals.get("5. volume", 0.0)),
            })
        except Exception:
            continue
    if not recs:
        return pd.DataFrame()
    df = pd.DataFrame(recs)
    df = df[(df["date"] >= pd.to_datetime(start.date())) & (df["date"] <= pd.to_datetime(end.date()))]
    if df.empty:
        return pd.DataFrame()
    df["load_ts"] = datetime.utcnow()
    return df[["date","open","high","low","close","adj_close","volume","load_ts"]]


def _fetch_btc_binance(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch BTCUSDT daily klines from Binance public API (no key).

    Treat USDT ~= USD for close values.
    """
    import math
    import pandas as pd
    base = "https://api.binance.com/api/v3/klines"
    # Binance returns max 1000 bars per call; loop if needed
    start_ms = int(datetime(start.year, start.month, start.day).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day).timestamp() * 1000)
    limit = 1000
    all_rows = []
    next_start = start_ms
    s = _requests_session()
    while next_start <= end_ms:
        params = {
            "symbol": "BTCUSDT",
            "interval": "1d",
            "startTime": next_start,
            "endTime": end_ms,
            "limit": limit,
        }
        r = s.get(base, params=params, timeout=30)
        if r.status_code != 200:
            break
        arr = r.json()
        if not arr:
            break
        all_rows.extend(arr)
        # Each item: [openTime, open, high, low, close, volume, closeTime, ...]
        last_close_time = arr[-1][6]
        # Advance to last_close_time + 1ms to avoid duplicates
        next_start = int(last_close_time) + 1
        if len(arr) < limit:
            break
    if not all_rows:
        return pd.DataFrame()
    rows = []
    for it in all_rows:
        try:
            rows.append({
                "date": pd.to_datetime(int(it[0]), unit="ms").date(),
                "open": float(it[1]),
                "high": float(it[2]),
                "low": float(it[3]),
                "close": float(it[4]),
                "adj_close": float(it[4]),
                "volume": float(it[5]),
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values("date")
    df = df[(df["date"] >= pd.to_datetime(start.date())) & (df["date"] <= pd.to_datetime(end.date()))]
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

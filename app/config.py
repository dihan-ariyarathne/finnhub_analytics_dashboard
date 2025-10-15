import os
from typing import List


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


PROJECT_ID = get_env("PROJECT_ID")
BQ_DATASET = get_env("BQ_DATASET")
SYMBOLS: List[str] = [s.strip() for s in get_env("SYMBOLS", "AAPL,TSLA,MSFT").split(",") if s.strip()]
FAST_MA = int(get_env("FAST_MA", "20"))
SLOW_MA = int(get_env("SLOW_MA", "50"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

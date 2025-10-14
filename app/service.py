from __future__ import annotations

import logging
from fastapi import FastAPI, Query
from datetime import datetime

from . import config
from .ingest import run_for_symbol, backfill_symbol


logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
logger = logging.getLogger(__name__)


app = FastAPI(title="Finnhub-Style Analytics", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run_pipeline():
    summary = {}
    for sym in config.SYMBOLS:
        try:
            n_prices, n_preds = run_for_symbol(sym, config.FAST_MA, config.SLOW_MA)
            summary[sym] = {"prices": n_prices, "predictions": n_preds}
        except Exception as e:
            logger.exception("Pipeline failed for %s", sym)
            summary[sym] = {"error": str(e)}
    return {"status": "completed", "summary": summary}


@app.post("/backfill")
def backfill(
    symbol: str = Query(..., description="Symbol, e.g., AAPL, TSLA, BTC-USD"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str | None = Query(None, description="End date YYYY-MM-DD (optional)")
):
    try:
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d") if end else None
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}

    try:
        n_prices, n_preds = backfill_symbol(symbol, config.FAST_MA, config.SLOW_MA, start_dt, end_dt)
        return {"status": "completed", "summary": {symbol: {"prices": n_prices, "predictions": n_preds}}}
    except Exception as e:
        logger.exception("Backfill failed for %s", symbol)
        return {"status": "error", "message": str(e)}

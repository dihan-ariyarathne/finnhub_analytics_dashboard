from __future__ import annotations

import logging
from fastapi import FastAPI

from . import config
from .ingest import run_for_symbol


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


# Finnhub-Style Analytics Dashboard (Loooker Studio + BigQuery + Cloud Run) — Requirements

> Goal: Daily end-to-end pipeline that (1) ingests **AAPL**, **TSLA**, and **BTC-USD** daily prices from **Yahoo Finance**, (2) stores them in **BigQuery** (one table per symbol), (3) computes next-day **Moving Average Crossover** predictions into three prediction tables, and (4) powers a **Looker Studio** dashboard via the BigQuery connector. All backend runs on **Python in Cloud Run** with CI/CD via **GitHub Actions**, and credentials handled via **environment files** (plus Cloud-native best practices).

---

## 1) Scope & Success Criteria
... (truncated for brevity in code, but includes full document above) ...

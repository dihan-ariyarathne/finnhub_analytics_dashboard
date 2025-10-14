# Finnhub-Style Analytics Dashboard

FastAPI service on Cloud Run that ingests daily prices from Yahoo Finance into BigQuery, computes Moving Average (MA) crossover signals, and exposes a `/run` endpoint for Cloud Scheduler. CI/CD builds and deploys via GitHub Actions using Workload Identity Federation (no JSON keys).

## Architecture
- Cloud Run service `finnhub-dash` (FastAPI, Python 3.11)
- BigQuery dataset (you created `finnhub_data`) with per‑symbol tables:
  - `prices_<symbol>` partitioned by `date`
  - `pred_ma_crossover_<symbol>` partitioned by `as_of`
- Cloud Scheduler daily POST to `/run` with OIDC auth
- Artifact Registry repo `finnhub-dash` (Docker images)
- GitHub Actions builds with Cloud Build and deploys to Cloud Run

## Service Endpoints
- `GET /health` → simple liveness check
- `POST /run` → runs ingest + prediction for all symbols

## Environment Variables (Cloud Run)
- `PROJECT_ID` e.g., `finnhub-dashboard`
- `BQ_DATASET` e.g., `finnhub_data`
- `SYMBOLS` CSV, e.g., `AAPL,TSLA,BTC-USD`
- `FAST_MA` default `20`
- `SLOW_MA` default `50`
- `LOG_LEVEL` default `INFO`

## Deployment
- On push to `main`, `.github/workflows/deploy.yml`:
  - Authenticates via WIF provider
  - Builds image with Cloud Build and pushes to Artifact Registry
  - Deploys to Cloud Run and sets env vars

If you change the project number or pool/provider names, update in `.github/workflows/deploy.yml:1` the `workload_identity_provider` value.

## Scheduler Setup
- Cloud Scheduler → HTTP target
  - URL: `<Cloud Run Service URL>/run`
  - Method: `POST`
  - Body: `{"trigger":"scheduler"}` (optional)
  - Auth: OIDC, service account with `Cloud Run Invoker`
  - Audience: base Cloud Run URL (no path)

## BigQuery Tables
Created on first run if missing (one set per symbol):
- `prices_<symbol>` columns: `date, open, high, low, close, adj_close, volume, load_ts`
- `pred_ma_crossover_<symbol>` columns: `as_of, fast_ma, slow_ma, signal, next_day_signal, model_version, load_ts`

## Local Development
- Build and run container:
  - `docker build -t finnhub-dash .`
  - `docker run -e PROJECT_ID=... -e BQ_DATASET=finnhub_data -e SYMBOLS=AAPL,TSLA,BTC-USD -e FAST_MA=20 -e SLOW_MA=50 -p 8080:8080 finnhub-dash`
- Call: `curl -X POST localhost:8080/run`
  - Note: Local container needs ADC that can access BigQuery (e.g., `gcloud auth application-default login`).

## Where To Find Your PROJECT_NUMBER
- Console: `IAM & Admin` → `Settings` → Project number is displayed.
- Console: Top bar project picker → hover project → shows number.
- Console: Home → Dashboard card lists Project number.
- CLI: `gcloud projects describe <PROJECT_ID> --format='value(projectNumber)'`
- Cloud Run URL often contains the number as a subdomain (e.g., `...-484774036849.us-central1.run.app`).

## Troubleshooting
- 403 on BigQuery: ensure Cloud Run runtime SA has `BigQuery Data Editor` and `BigQuery Job User`.
- 401 from Cloud Run: call with an ID token or use Scheduler OIDC; service is set to require auth.
- CI auth fails: verify WIF provider resource in workflow and that `Workload Identity User` is granted.
- Empty data: yfinance may lag for today’s bar; check after market close or widen start date.
- Expensive merges: increase batch size or run off-peak; tables are partitioned by date to minimize cost.

## Code Map
- `app/config.py:1` env + defaults
- `app/bq.py:1` BigQuery table creation + MERGE upserts
- `app/ingest.py:1` yfinance fetch + MA computation
- `app/service.py:1` FastAPI app (`/health`, `/run`)
- `Dockerfile:1` container entrypoint (Uvicorn)
- `.github/workflows/deploy.yml:1` CI/CD to Cloud Run


from __future__ import annotations

from datetime import date
from typing import Iterable

from google.cloud import bigquery

from . import config


def client() -> bigquery.Client:
    return bigquery.Client(project=config.PROJECT_ID)


def table_id(base: str) -> str:
    return f"{config.PROJECT_ID}.{config.BQ_DATASET}.{base}"


def ensure_price_table(symbol: str) -> None:
    bq = client()
    tbl = bigquery.Table(
        table_id(f"prices_{symbol.lower().replace('-', '_')}")
    )
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("open", "FLOAT64"),
        bigquery.SchemaField("high", "FLOAT64"),
        bigquery.SchemaField("low", "FLOAT64"),
        bigquery.SchemaField("close", "FLOAT64"),
        bigquery.SchemaField("adj_close", "FLOAT64"),
        bigquery.SchemaField("volume", "INT64"),
        bigquery.SchemaField("load_ts", "TIMESTAMP"),
    ]
    tbl.schema = schema
    tbl.time_partitioning = bigquery.TimePartitioning(field="date")
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)


def ensure_prediction_table(symbol: str) -> None:
    bq = client()
    tbl = bigquery.Table(
        table_id(f"pred_ma_crossover_{symbol.lower().replace('-', '_')}")
    )
    schema = [
        bigquery.SchemaField("as_of", "DATE"),
        bigquery.SchemaField("fast_ma", "FLOAT64"),
        bigquery.SchemaField("slow_ma", "FLOAT64"),
        bigquery.SchemaField("signal", "STRING"),
        bigquery.SchemaField("next_day_signal", "STRING"),
        bigquery.SchemaField("model_version", "STRING"),
        bigquery.SchemaField("load_ts", "TIMESTAMP"),
    ]
    tbl.schema = schema
    tbl.time_partitioning = bigquery.TimePartitioning(field="as_of")
    try:
        bq.get_table(tbl)
    except Exception:
        bq.create_table(tbl)


def max_date(symbol: str) -> date | None:
    bq = client()
    query = f"""
    SELECT MAX(date) AS max_date
    FROM `{table_id('prices_' + symbol.lower().replace('-', '_'))}`
    """
    try:
        rows = list(bq.query(query))
    except Exception:
        return None
    if not rows or rows[0][0] is None:
        return None
    return rows[0][0]


def upsert_prices(symbol: str, df) -> int:
    """Upsert price rows by date using a temp table + MERGE.

    Expects df columns: date, open, high, low, close, adj_close, volume, load_ts
    """
    bq = client()
    target = table_id(f"prices_{symbol.lower().replace('-', '_')}")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    temp_table = table_id(f"_tmp_prices_{symbol.lower().replace('-', '_')}")

    # Load to temporary table
    bq.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{target}` T
    USING `{temp_table}` S
    ON T.date = S.date
    WHEN MATCHED THEN UPDATE SET
      open = S.open,
      high = S.high,
      low = S.low,
      close = S.close,
      adj_close = S.adj_close,
      volume = S.volume,
      load_ts = S.load_ts
    WHEN NOT MATCHED THEN INSERT (date, open, high, low, close, adj_close, volume, load_ts)
    VALUES (S.date, S.open, S.high, S.low, S.close, S.adj_close, S.volume, S.load_ts)
    """
    bq.query(merge_sql).result()
    # Post-merge: ensure next_day_signal is fully populated using LEAD() over the table
    update_sql = f"""
    UPDATE `{target}` T
    SET next_day_signal = COALESCE(S.next_signal, T.signal)
    FROM (
      SELECT as_of, LEAD(signal) OVER (ORDER BY as_of) AS next_signal
      FROM `{target}`
    ) S
    WHERE T.as_of = S.as_of
    """
    bq.query(update_sql).result()
    # Clean up temp table
    try:
        bq.delete_table(temp_table)
    except Exception:
        pass
    return len(df)


def upsert_predictions(symbol: str, df) -> int:
    bq = client()
    target = table_id(f"pred_ma_crossover_{symbol.lower().replace('-', '_')}")

    # Enforce stable dtypes to avoid BQ inference surprises
    df = df.copy()
    if "signal" in df.columns:
        df["signal"] = df["signal"].astype("string")
    if "next_day_signal" in df.columns:
        df["next_day_signal"] = df["next_day_signal"].astype("string")
    if "model_version" in df.columns:
        df["model_version"] = df["model_version"].astype("string")

    temp_table = table_id(f"_tmp_pred_{symbol.lower().replace('-', '_')}")
    temp_schema = [
        bigquery.SchemaField("as_of", "DATE"),
        bigquery.SchemaField("fast_ma", "FLOAT64"),
        bigquery.SchemaField("slow_ma", "FLOAT64"),
        bigquery.SchemaField("signal", "STRING"),
        bigquery.SchemaField("next_day_signal", "STRING"),
        bigquery.SchemaField("model_version", "STRING"),
        bigquery.SchemaField("load_ts", "TIMESTAMP"),
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=temp_schema,
    )
    bq.load_table_from_dataframe(df, temp_table, job_config=job_config).result()

    merge_sql = f"""
    MERGE `{target}` T
    USING `{temp_table}` S
    ON T.as_of = S.as_of
    WHEN MATCHED THEN UPDATE SET
      fast_ma = S.fast_ma,
      slow_ma = S.slow_ma,
      signal = S.signal,
      next_day_signal = S.next_day_signal,
      model_version = S.model_version,
      load_ts = S.load_ts
    WHEN NOT MATCHED THEN INSERT (as_of, fast_ma, slow_ma, signal, next_day_signal, model_version, load_ts)
    VALUES (S.as_of, S.fast_ma, S.slow_ma, S.signal, S.next_day_signal, S.model_version, S.load_ts)
    """
    bq.query(merge_sql).result()
    try:
        bq.delete_table(temp_table)
    except Exception:
        pass
    return len(df)

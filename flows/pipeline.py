import os
import sys
import json
import math
import pandas as pd
import duckdb
from prefect import flow, task, get_run_logger

INPUT_CSV = os.getenv("INPUT_CSV", "./input/df_fraud_credit.csv")

DATA_DIR = "./data"
CURATED_DIR = "./curated"
DUCK_PATH = os.path.join(DATA_DIR, "results.duckdb")
DQ_METRICS_PATH = os.path.join(DATA_DIR, "dq_metrics.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CURATED_DIR, exist_ok=True)

def _detect_timestamp_unit(series: pd.Series) -> str:
    
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return "s"
    m = float(s.astype(float).abs().median())
    if m > 1e17:
        return "ns"
    elif m > 1e14:
        return "us"
    elif m > 1e11:
        return "ms"
    else:
        return "s"

@task
def ingest_local(path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Ingestão local do CSV: {path}")
    df = pd.read_csv(path)
    logger.info(f"Linhas lidas: {len(df)} | Colunas: {list(df.columns)}")
    return df

@task
def clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Iniciando limpeza e padronização")

    # Normaliza nomes de colunas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Padroniza strings básicas
    for col in ["receiving_address", "location_region", "transaction_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"": None, "nan": None, "None": None, "0": None})
    # transaction_type em minúsculas
    if "transaction_type" in df.columns:
        df["transaction_type"] = df["transaction_type"].str.lower()

    # timestamp -> datetime UTC, com detecção de unidade quando numérico
    if "timestamp" in df.columns:
        ts_raw = df["timestamp"]
        # Se numérico, detecta unidade
        if pd.api.types.is_numeric_dtype(ts_raw) or ts_raw.dropna().astype(str).str.match(r"^\d+(\.\d+)?$").all():
            unit = _detect_timestamp_unit(ts_raw)
            logger.info(f"Detectada unidade do timestamp: {unit}")
            df["timestamp"] = pd.to_datetime(pd.to_numeric(ts_raw, errors="coerce"), unit=unit, utc=True)
        else:
            df["timestamp"] = pd.to_datetime(ts_raw, errors="coerce", utc=True)
    else:
        df["timestamp"] = pd.NaT

    # amount -> numérico; remove inválidos/negativos
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    else:
        df["amount"] = pd.NA
    if "risk_score" in df.columns:
        df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["timestamp", "transaction_type", "amount"])
    df = df[df["amount"] >= 0]

    key_cols = [c for c in ["timestamp", "receiving_address", "transaction_type", "amount"] if c in df.columns]
    if key_cols:
        df = df.drop_duplicates(subset=key_cols)

    after = len(df)
    logger.info(f"Limpeza concluída. Linhas antes: {before} | depois: {after} | removidas: {before - after}")
    return df

@task
def dq_checks(df: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("Executando Data Quality checks")
    total = len(df)

    metrics = {
        "total_rows": total,
        "nulls": {},
        "rules": {
            "timestamp_not_null": None,
            "transaction_type_not_null": None,
            "amount_not_null": None,
            "amount_non_negative": None,
        },
        "conformity_rate": None,
        "failed_rows_estimate": None,
    }

    for col in ["timestamp", "transaction_type", "amount", "receiving_address", "location_region", "risk_score"]:
        if col in df.columns:
            metrics["nulls"][col] = int(df[col].isna().sum())

    fails = 0
    for rule, col in [
        ("timestamp_not_null", "timestamp"),
        ("transaction_type_not_null", "transaction_type"),
        ("amount_not_null", "amount"),
    ]:
        if col in df.columns:
            violated = int(df[col].isna().sum())
            metrics["rules"][rule] = {"violations": violated}
            fails += violated

    if "amount" in df.columns:
        violated = int((df["amount"] < 0).sum())
        metrics["rules"]["amount_non_negative"] = {"violations": violated}
        fails += violated

    metrics["failed_rows_estimate"] = fails
    metrics["conformity_rate"] = max(0.0, 1.0 - (fails / (total + 1e-9)))

    with open(DQ_METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    logger.info(f"Métricas de DQ -> {DQ_METRICS_PATH}")
    logger.info(json.dumps(metrics, indent=2, default=str))
    return metrics

@task
def transform_and_publish(df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("Escrevendo stg e resultados no DuckDB")
    con = duckdb.connect(DUCK_PATH)

    con.register("df", df)

    con.execute("""
        create or replace table stg_transactions as
        select * from df
    """)

    con.execute("""
        create or replace table region_risk_avg as
        select location_region,
               avg(risk_score) as avg_risk_score
        from stg_transactions
        where location_region is not null
        group by 1
        order by avg_risk_score desc
    """)

    con.execute("""
        create or replace table last_sale_per_address as
        with ranked as (
            select *,
                   row_number() over (
                     partition by receiving_address
                     order by timestamp desc
                   ) as rn
            from stg_transactions
            where transaction_type = 'sale'
        )
        select receiving_address, amount, timestamp
        from ranked
        where rn = 1
    """)

    con.execute("""
        create or replace table top3_recent_sales_by_receiving as
        select *
        from last_sale_per_address
        order by amount desc
        limit 3
    """)

    con.execute(f"copy (select * from region_risk_avg) to '{CURATED_DIR}/region_risk_avg.csv' (header, delim ',')")

    con.execute(f"copy (select * from top3_recent_sales_by_receiving) to '{CURATED_DIR}/top3_recent_sales_by_receiving.csv' (header, delim ',')")

    logger.info(f"Resultados exportados para {CURATED_DIR}")
    con.close()

@flow(name="local_etl_case")
def main():
    logger = get_run_logger()
    try:
        df = ingest_local(INPUT_CSV)
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado em {INPUT_CSV}. Coloque o CSV na pasta ./input ou ajuste a env var INPUT_CSV.")
        sys.exit(1)

    df_clean = clean_and_standardize(df)
    metrics = dq_checks(df_clean)

    # Política: falhar abaixo de 98% de conformidade
    if metrics["conformity_rate"] is not None and metrics["conformity_rate"] < 0.98:
        transform_and_publish(df_clean)  # ainda publica p/ inspeção
        raise RuntimeError("Conformidade < 98% — falhando o flow por política de qualidade.")

    transform_and_publish(df_clean)
    logger.info("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()

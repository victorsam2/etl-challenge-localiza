import os
import sys
import json
import pandas as pd
import duckdb
from prefect import flow, task, get_run_logger


INPUT_CSV = os.getenv("INPUT_CSV", "./input/df_fraud_credit.csv")

DATA_DIR = "./data"
CURATED_DIR = "./curated"
DUCK_PATH = os.path.join(DATA_DIR, "results.duckdb")
DQ_PRE_PATH = os.path.join(DATA_DIR, "dq_metrics_pre.json")
DQ_POST_PATH = os.path.join(DATA_DIR, "dq_metrics_post.json")

MIN_CONFORMITY_PRE = float(os.getenv("MIN_CONFORMITY_PRE", "0.98"))
MIN_CONFORMITY_POST = float(os.getenv("MIN_CONFORMITY_POST", "0.995"))

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CURATED_DIR, exist_ok=True)


# detectar unidade do timestamp
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


# Ingestão
@task
def ingest_local(path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Ingestão local do CSV: {path}")
    df = pd.read_csv(path)
    logger.info(f"Linhas lidas: {len(df)} | Colunas: {list(df.columns)}")
    return df


# Define o Data Quality profile
def _dq_profile(df: pd.DataFrame) -> dict:
    total = int(len(df))
    metrics = {
        "total_rows": total,
        "nulls": {},
        "rules": {
            "timestamp_not_null": None,
            "transaction_type_not_null": None,
            "amount_not_null": None,
            "amount_non_negative": None,
        },
        "failed_rows_estimate": None,
        "conformity_rate": None,
    }

    for col in ["timestamp", "transaction_type", "amount", "receiving_address", "location_region", "risk_score"]:
        if col in df.columns:
            metrics["nulls"][col] = int(df[col].isna().sum())

    fails = 0
    # Regras de not-null
    for rule, col in [
        ("timestamp_not_null", "timestamp"),
        ("transaction_type_not_null", "transaction_type"),
        ("amount_not_null", "amount"),
    ]:
        if col in df.columns:
            violated = int(df[col].isna().sum())
            metrics["rules"][rule] = {"violations": violated}
            fails += violated

    # amount não-negativo
    if "amount" in df.columns:
        violated = int((pd.to_numeric(df["amount"], errors="coerce") < 0).sum())
        metrics["rules"]["amount_non_negative"] = {"violations": violated}
        fails += violated

    metrics["failed_rows_estimate"] = int(fails)
    metrics["conformity_rate"] = max(0.0, 1.0 - (fails / (total + 1e-9)))
    return metrics


# Data Quality 1
@task
def dq_checks(df: pd.DataFrame, phase: str, out_path: str) -> dict:
    logger = get_run_logger()
    logger.info(f"Executando Data Quality ({phase})")
    # Garantir tipos mínimos para medir
    df_tmp = df.copy()
    if "amount" in df_tmp.columns:
        df_tmp["amount"] = pd.to_numeric(df_tmp["amount"], errors="coerce")
    # timestamp pode ser número (epoch) ou datetime; não forçamos conversão aqui.
    metrics = _dq_profile(df_tmp)
    with open(out_path, "w") as f:
        json.dump({"phase": phase, **metrics}, f, indent=2, default=str)
    logger.info(f"DQ ({phase}) salvo em {out_path}")
    logger.info(json.dumps({"phase": phase, **metrics}, indent=2, default=str))
    return metrics


# Limpeza e padronização
@task
def clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Iniciando limpeza e padronização")

    # Normaliza nomes
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "receiving_address" in df.columns:
        df["receiving_address"] = df["receiving_address"].astype(str).str.strip().replace({"": None, "nan": None, "None": None})
    if "transaction_type" in df.columns:
        df["transaction_type"] = df["transaction_type"].astype(str).str.strip().str.lower().replace({"": None, "nan": None, "None": None})
    if "location_region" in df.columns:
        df["location_region"] = (
            df["location_region"].astype(str).str.strip()
            .replace({"": None, "nan": None, "None": None, "0": None})  
        )

    # timestamp -> datetime UTC (detectando unidade se for numérico)
    if "timestamp" in df.columns:
        ts_raw = df["timestamp"]
        looks_numeric = pd.api.types.is_numeric_dtype(ts_raw) or ts_raw.dropna().astype(str).str.match(r"^\d+(\.\d+)?$").all()
        if looks_numeric:
            unit = _detect_timestamp_unit(ts_raw)
            logger.info(f"Detectada unidade do timestamp: {unit}")
            df["timestamp"] = pd.to_datetime(pd.to_numeric(ts_raw, errors="coerce"), unit=unit, utc=True)
        else:
            df["timestamp"] = pd.to_datetime(ts_raw, errors="coerce", utc=True)
    else:
        df["timestamp"] = pd.NaT

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
        df = df.drop_duplicates(subset=key_cols, keep="first")

    after = len(df)
    logger.info(f"Limpeza concluída. Linhas antes: {before} | depois: {after} | removidas: {before - after}")
    return df


# Transformações de negócio
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

    # 1) média de risk_score por região (desc)
    con.execute("""
        create or replace table region_risk_avg as
        select location_region,
               avg(risk_score) as avg_risk_score
        from stg_transactions
        where location_region is not null
        group by 1
        order by avg_risk_score desc
    """)

    # 2) última venda por receiving_address (timestamp mais recente) e top 3 amounts
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

    # Exports
    con.execute(f"copy (select * from region_risk_avg) to '{CURATED_DIR}/region_risk_avg.csv' (header, delim ',')")
    con.execute(f"copy (select * from top3_recent_sales_by_receiving) to '{CURATED_DIR}/top3_recent_sales_by_receiving.csv' (header, delim ',')")

    logger.info(f"Resultados exportados para {CURATED_DIR}")
    con.close()


# Flow
@flow(name="local_etl_case")
def main():
    logger = get_run_logger()
    try:
        df_raw = ingest_local(INPUT_CSV)
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado em {INPUT_CSV}. Coloque o CSV na pasta ./input ou ajuste a env var INPUT_CSV.")
        sys.exit(1)

    # DQ pré-limpeza (estado real da chegada)
    dq_pre = dq_checks(df_raw, phase="pre_clean", out_path=DQ_PRE_PATH)

    # Política principal: barrar lote ruim NA CHEGADA
    if dq_pre["conformity_rate"] is not None and dq_pre["conformity_rate"] < MIN_CONFORMITY_PRE:
        # Mesmo barrando, publicamos uma visão “staging” mínima para inspeção
        try:
            # Publica mínimo bruto para análise (sem filtros)
            duck = duckdb.connect(DUCK_PATH)
            duck.register("df_raw", df_raw)
            duck.execute("create or replace table raw_snapshot as select * from df_raw")
            duck.close()
        finally:
            pass
        raise RuntimeError(
            f"Conformidade PRE ({dq_pre['conformity_rate']:.4f}) < {MIN_CONFORMITY_PRE} — falhando por política de qualidade."
        )

    # Limpeza/padronização
    df_clean = clean_and_standardize(df_raw)

    # DQ pós-limpeza (qualidade da camada curated)
    dq_post = dq_checks(df_clean, phase="post_clean", out_path=DQ_POST_PATH)

    # Política secundária: barrar lote ruim na saída
    if dq_post["conformity_rate"] is not None and dq_post["conformity_rate"] < MIN_CONFORMITY_POST:
        # Ainda publica para inspeção
        transform_and_publish(df_clean)
        raise RuntimeError(
            f"Conformidade POST ({dq_post['conformity_rate']:.4f}) < {MIN_CONFORMITY_POST} — falhando por política de qualidade na saída."
        )

    # Publicação normal
    transform_and_publish(df_clean)
    logger.info("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()

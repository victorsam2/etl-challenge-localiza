# ETL Local (Desafio Localiza)

Stack mínima: **Python + Pandas + DuckDB** com tarefas em estilo **Prefect** (sem servidor), **Data Quality** automatizado e **conteinerização via Docker**.  
Entrada **local** (sem URL, lendo da pasta `./input`). Saídas em `./curated` e um banco **DuckDB** (`./data/results.duckdb`).

## Estrutura
```
.
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ README.md
├─ flows/
│  └─ pipeline.py
├─ input/
│  └─ df_fraud_credit.csv           # coloque seu CSV aqui
├─ data/
│  ├─ results.duckdb                # gerado pelo pipeline
│  └─ dq_metrics.json               # métricas de qualidadee
└─ curated/
   ├─ region_risk_avg.csv
   └─ top3_recent_sales_by_receiving.csv
```

## Como rodar (Docker)
1. Coloque seu arquivo na pasta `input/` com nome `df_fraud_credit.csv` (ou ajuste a env var abaixo).
2. Construa e execute:
   ```bash
   docker compose up --build
   ```
   O container roda o pipeline e termina. Os artefatos ficam nas pastas montadas.

## Como rodar (local sem Docker)
```bash
pip install -r requirements.txt
python flows/pipeline.py
```

## O que o pipeline faz
1. **Ingestão local**: lê `./input/df_fraud_credit.csv`.
2. **Limpeza e padronização**:
   - `timestamp` → datetime (UTC); inválidos são descartados.
   - `amount` → numérico; negativos são tratados como inválidos (descartados).
   - `transaction_type` → normalizado para minúsculas (e.g., `sale`, `refund`).
   - `receiving_address` e `location_region` → `.str.strip()`, vazios/"0" viram `NaN`.
   - **Dedupe** por (`timestamp`,`receiving_address`,`transaction_type`,`amount`).
3. **Data Quality (automatizado)**: validações mínimas
   - `timestamp`, `transaction_type`, `amount` não nulos após limpeza.
   - `amount >= 0`.
   - conformidade mínima de 98%; abaixo disso o job sai com código 2 (falha).
   - Métricas em `data/dq_metrics.json`.
4. **Transformações (DuckDB)**:
   - Tabela `stg_transactions` (limpa) em `data/results.duckdb`.
   - **Resultado 1**: `region_risk_avg` (média de `risk_score` por `location_region`, desc.).
   - **Resultado 2**: `top3_recent_sales_by_receiving` (para cada `receiving_address`, pega a *última* transação `sale` por `timestamp` e lista o top 3 por `amount` desc.).
   - Exporta resultados para `./curated` em **CSV**.

## Variáveis de ambiente
- `INPUT_CSV` (default: `./input/df_fraud_credit.csv`)


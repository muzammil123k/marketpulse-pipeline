# MarketPulse Pipeline

An end-to-end market data pipeline combining batch and streaming ingestion, dbt transformations, and FinBERT-based ML inference.

## Structure

| Path | Purpose |
|------|---------|
| `.github/workflows/` | GitHub Actions CI/CD pipeline definitions |
| `airflow/dags/` | Apache Airflow DAG definitions |
| `data_ingestion/batch/` | yfinance & historical data scripts |
| `data_ingestion/streaming/` | Alpaca Event Hubs producers/consumers |
| `dbt_marketpulse/` | Full dbt project |
| `infrastructure/terraform/` | IaC configurations |
| `ml_pipeline/notebooks/` | Feature engineering & training notebooks |
| `ml_pipeline/src/` | FinBERT inference and deployment scripts |

## Setup

1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies per sub-project (see each folder's README).
3. Configure Airflow connections to point at your environment.

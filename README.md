# databricks-crypto-dlt-medallion
Crypto Market Medallion Pipeline using DLT, Autoloader &amp; Unity Catalog on Databricks

# 🪙 Crypto Market Medallion Pipeline — Databricks DLT

## Architecture
![Architecture](architecture/crypto_medallion_architecture.png)

## Tech Stack
- Azure Databricks Free Edition
- Delta Live Tables (DLT) with @dlt.expect quality rules
- Autoloader (cloudFiles) for CSV + JSON ingestion
- Unity Catalog — 3-level namespace (catalog.schema.table)
- CoinGecko REST API + Kaggle historical CSV
- Databricks Workflows (2-task orchestrated job)
- PySpark + Spark SQL

## Pipeline Overview
| Layer  | Table               | Description                        |
|--------|--------------------|------------------------------------|
| Bronze | raw_ohlcv_csv      | Historical Kaggle CSV via Autoloader|
| Bronze | raw_ohlcv_api      | Live CoinGecko API via Autoloader  |
| Silver | cleaned_ohlcv      | Union + 5 DQ rules + daily_return  |
| Gold   | daily_market_summary| OHLCV + 7d moving avg per coin    |
| Gold   | coin_volatility    | Stddev + risk_tier per coin        |
| Gold   | market_cap_dominance| Market share rankings             |
| Gold   | top_performers_monthly | Monthly return rankings        |

## How to Run
1. Upload `data/kaggle_crypto_historical.csv` to UC Volume
2. Run `notebooks/00_setup.py` to create catalog/schemas/volumes
3. Run `notebooks/01_api_ingestor.py` to fetch CoinGecko data
4. Create DLT pipeline pointing to `notebooks/02_dlt_pipeline.py`
5. Trigger pipeline — watch lineage graph populate ✅

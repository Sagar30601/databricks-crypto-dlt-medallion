# Imports & Config
import dlt
from pyspark.sql import functions as F
from pyspark.sql import Window

# ── Paths ────────────────────────────────────────────
CATALOG    = "crypto_analytics"
BASE_PATH  = "/Volumes/crypto_analytics/landing/raw"

CSV_PATH        = f"{BASE_PATH}/csv/"
API_PATH        = f"{BASE_PATH}/api/"
SCHEMA_CSV_PATH = f"{BASE_PATH}/schema_csv/"
SCHEMA_API_PATH = f"{BASE_PATH}/schema_api/"
SCHEMA_MKT_PATH = f"{BASE_PATH}/schema_mkt/"


# Bronze — Historical CSV via Autoloader
@dlt.table(
    name = "raw_ohlcv_csv",
    comment = "Raw historical OHLCV data ingested from Kaggle/CryptoDataDownload CSVs via Autoloader",
    table_properties = {
        "quality"                         : "bronze",
        "delta.autoOptimize.optimizeWrite" : "true"
    }
)
def bronze_csv():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", SCHEMA_CSV_PATH)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .option("skipRows", "1")
            .load(CSV_PATH)
            .withColumn("source", F.lit("csv_bckfill"))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
    )
    

# Bronze — Daily CoinGecko API JSON via Autoloader
@dlt.table(
    name = "raw_ohlcv_api",
    comment = "Daily OHLCV data from CoinGecko API — incremental, one folder per day",
    table_properties = {
        "quality"                          : "bronze",
        "delta.autoOptimize.optimizeWrite" : "true"
    }
)
def bronze_api():
    return(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_API_PATH)
            .load(f"{API_PATH}*/ohlcv/")          # wildcard picks up all dated folders
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
    )


# Bronze — Daily Market Cap snapshots via Autoloader
@dlt.table(
    name = "raw_market_cap",
    comment = "Daily market cap snapshots from CoinGecko API",
    table_properties = {
        "quality"                          : "bronze",
        "delta.autoOptimize.optimizeWrite" : "true"
    }
)
def bronze_market_cap():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", SCHEMA_MKT_PATH)
            .load(f"{API_PATH}*/market_caps/")          # wildcard picks up all dated folders
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
    )


# Silver — Clean, union, deduplicate CSV + API bronze tables
@dlt.expect("valid_close", "close > 0")
@dlt.expect("valid_ohlcv",      "high >= low")
@dlt.expect_or_drop("valid_volume",   "volume >= 0")
@dlt.expect_or_drop("no_future_date", "trade_date <= current_date()")
@dlt.expect("valid_return",     "daily_return_pct BETWEEN -99 AND 99")
@dlt.table(
    name    = "cleaned_ohlcv",
    comment = "Cleaned, unioned, deduplicated OHLCV from CSV + API. Daily return % added.",
    table_properties = {
        "quality"                          : "silver",
        "delta.autoOptimize.optimizeWrite" : "true"
    }
)
def silver_ohlcv():
     ── CSV branch ──────────────────────────────────
    # CSV columns: Unix, Date, Symbol, Open, High, Low, Close, Volume XXX, Volume USDT
    csv = (
        dlt.read_stream("raw_ohlcv_csv")
            .select(
                F.lower(
                    F.regexp_replace("Symbol", "USDT", "")
                ).alias("coin_id"),
                F.to_date(F.col("Date"), "yyyy-MM-dd").alias("trade_date"),
                F.col("Open").cast("double").alias("open"),
                F.col("High").cast("double").alias("high"),
                F.col("Low").cast("double").alias("low"),
                F.col("Close").cast("double").alias("close"),
                F.col("Volume USDT").cast("double").alias("volume"),
                F.lit("csv_backfill").alias("data_source")
            )
    )

    # ── API branch ──────────────────────────────────
    # API columns: coin_id, timestamp, open, high, low, close, source, fetched_at
    api = (
        dlt.read_stream("raw_ohlcv_api")
            .select(
                F.lower(F.col("coin_id")).alias("coin_id"),
                F.to_date(
                    F.from_unixtime(F.col("timestamp") / 1000)
                ).alias("trade_date")
                F.col("open").cast("double"),
                F.col("high").cast("double"),
                F.col("low").cast("double"),
                F.col("close").cast("double"),
                F.lit(0.0).alias("volume"),
                F.lit("coingecko_api").alias("data_source")
            )
    )

    # ── Union + enrich + deduplicate ────────────────
    return (
        csv.unionByName(api)
            .withColumn("daily_return_pct",
                F.round(
                    (F.col("close") - F.col("open")) / F.col("open") * 100, 4)
                )
            .withColumn("price_range",
                F.round(F.col("high") - F.col("low"), 6))
            .dropDuplicates(["coin_id", "trade_date"])    
            .filter(F.col("trade_date").isNotNull())    
    )



















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

# Gold — Daily OHLCV summary with 7-day moving average

@dlt.table(
    name = "daily_market_summary",
    comment = "Daily OHLCV per coin with 7d moving average close price",
    table_properties = {"qaulity": "gold"}
)
def gold_daily_summary():
    window_7d = (
        Window.partitionBy("coin_id")
              .orderBy("trade_date")
              .rowsBetween(-6, 0)
    )
    return (
        dlt.read("cleaned_ohlcv")
           .withColumn("moving_avg_7d", F.round(F.avg("close").over(window_7d), 6))
           .select(
               "coin_id",
                "trade_date",
                "open", "high", "low", "close",
                "volume",
                "daily_return_pct",
                "price_range",
                "moving_avg_7d",
                "data_source"
           )
           .orderBy("coin_id", "trade_date")
    )

# Gold — Volatility and risk classification per coin
@dlt.table(
    name = "coin_volatility",
    comment = "Volatility metrics and risk tier per coin based on daily return stddev",
    table_properties = {"quality": "gold"}
)

def gold_volatility():
    return (
        dlt.read("cleaned_ohlcv")
           .groupBy("coin_id")
           .agg(
                F.count("*").alias("total_trading_days"), 
                F.round(F.avg("close"), 6).alias("avg_close_price"),
                F.round(F.avg("daily_return_pct"), 4).alias("avg_daily_return_pct"),
                F.round(F.stddev("daily_return_pct"), 4).alias("volatility_stddev"),
                F.round(F.max("daily_return_pct"), 4).alias("best_day_return_pct"),
                F.round(F.min("daily_return_pct"), 4).alias("worst_day_return_pct"),
                F.min("trade_date").alias("data_from"),
                F.max("trade_date").alias("data_to")
           )
           .withColumn("risk_tier",
                F.when(F.col("volatility_stddev") > 5,  "🔴 High")
                 .when(F.col("volatility_stddev") > 2,  "🟡 Medium")
                 .otherwise(                             "🟢 Low")
            )
    )


# Gold — Monthly return rankings across all coins
@dlt.table(
    name = "top_performers_monthly",
    comment = "Monthly return % per coin ranked best to worst",
    table_properties = {"quality": "gold"}
)
def gold_top_performers():
    window_rank - (
        Window.partitionBy("month")
              .orderBy(F.desc("monthly_return_pct"))
    )
    return(
        dlt.read("cleaned_ohlcv")
           .withColumn("month", F.date_format("trade_date", "yyyy-MM"))
           .groupBy("coin_id", "month")
           .agg(
               F.round(F.sum("daily_return_pct"), 2).alias("monthly_return_pct"),
                F.round(F.max("daily_return_pct"), 2).alias("best_single_day_pct"),
                F.round(F.min("daily_return_pct"), 2).alias("worst_single_day_pct"),
                F.round(F.avg("close"), 6).alias("avg_monthly_close"),
                F.count("*").alias("trading_days")
            )
            .withColumn("rank", F.rank().over(window_rank))
    )


































# Imports & Config
import requests
import json
import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Config ──────────────────────────────────────────
CATALOG = "crypto_analytics"
SCHEMA = "landing"
VOLUME = "raw"
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

TODAY = datetime.date.today().strftime("%Y-%m-%d")
API_OUTPUT_PATH = f"{BASE_PATH}/api/{TODAY}"

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_KEY  = "CG-aBebkX2fm16jo92nL6a3kDnF"


COINS = [
  "bitcoin",
  "ethereum", 
  "solana",
  "ripple",
  "binancecoin",
  "cardano",
  "dogecoin",
  "avalanche-2",
  "chainlink",
  "polkadot"
]

print(f" Config loaded")
print(f" Run date : {TODAY}")
print(f" Output path : {API_OUTPUT_PATH}")
print(f" Coins to fetch : {len(COINS)}")

# Quick connectivity test
test = requests.get(
    f"{COINGECKO_BASE}/ping",
    params={"x_cg_demo_api_key": COINGECKO_KEY}
)
print(f"🏓 Ping: {test.json()}")   # should print: {'gecko_says': '(V3) To the Moon!'}


# Helper Function
def fetch_ohlcv(coin_id: str, days: int = 1) -> list:
  """
    Fetch OHLCV data from CoinGecko for a given coin.
    Returns list of records or empty list on failure.
  """
  url = f"{COINGECKO_BASE}/coins/{coin_id}/ohlc"
  params = {"vs_currency": "usd", "days": days, "x_cg_demo_api_key": COINGECKO_KEY}

  try:
    response = requests.get(url, params=params, timeout=10)

    if response.status_code == 200:
      raw = response.json()
      records = []
      for row in raw:
        records.append({
          "coin_id"   : coin_id,
          "timestamp" : row[0],
          "open"      : float(row[1]),
          "high"      : float(row[2]),
          "low"       : float(row[3]),
          "close"     : float(row[4]),
          "source"    : "coingecko_api",
          "fetched_at": TODAY
        })
      return records
    
    elif response.status_code == 429:
      print(f"  ⚠️  Rate limited on {coin_id} — waiting 60s")
      time.sleep(60)
      return fetch_ohlcv(coin_id, days) # retry once
    
    else:
      print(f"  ❌ Failed {coin_id}: HTTP {response.status_code} — {response.text}")
      return []

  except Exception as e:
    print(f"  ❌ Exception for {coin_id}: {str(e)}")
    return []


def fetch_market_caps(coin_ids: list) -> list:
  """
    Fetch current market cap & dominance data for all coins in one call.
  """
  url = f"{COINGECKO_BASE}/coins/markets"
  params = {
    "vs_currency" : "usd",
    "ids"         : ",".join(coin_ids),
    "per_page"    : 50,
    "page"        : 1,
    "x_cg_demo_api_key": COINGECKO_KEY
  }

  try:
    response = requests.get(url, params=params, timeout=10)
    if response.status_code == 200:
      records = []
      for coin in response.json():
        records.append({
          "coin_id"       : coin["id"],
          "symbol"        : coin["symbol"].upper(),
          "snapshot_date" : TODAY,
          "market_cap_usd": float(coin["market_cap"] or 0),
          "current_price" : float(coin["current_price"] or 0),
          "total_volume"  : float(coin["total_volume"] or 0),
          "price_change_24h_pct": float(coin["price_change_percentage_24h"] or 0),
          "source"        : "coingecko_api",
          "fetched_at"    : TODAY
        })
      return records
    
    else:
      print(f"❌ Market cap fetch failed: HTTP {response.status_code} — {response.text}")
      return []
    
  except Exception as e:
    print(f"❌ Market cap exception: {str(e)}")
    return []
      
print("✅ Helper functions defined")



# Fetch OHLCV for all coins
print("🚀 Starting OHLCV fetch...\n")

all_ohlcv_records = []

for coin in COINS:
  print(f" 📡 Fetching: {coin}")
  records = fetch_ohlcv(coin_id=coin, days=1)

  all_ohlcv_records.extend(records)
  print(f"    ✅ {len(records)} records fetched")
  time.sleep(2) 

print(f"\n📊 Total OHLCV records fetched: {len(all_ohlcv_records)}")


# Fetch Market Cap Data
print("🚀 Fetching market cap data...\n")

market_cap_records = fetch_market_caps(COINS)

print(f"📊 Market cap records fetched: {len(market_cap_records)}")

# Preview
for r in market_cap_records[:3]:
    print(f"  {r['coin_id']:15} | ${r['market_cap_usd']:>20,.0f} | ${r['current_price']:>10,.2f}")


# Write OHLCV records to Volume as JSON

if len(all_ohlcv_records) == 0:
  raise Exception("❌ No OHLCV records fetched — aborting write")

# dated output folder
dbutils.fs.mkdirs(API_OUTPUT_PATH)

# Convert to Spark DataFrame and write
ohlcv_df = spark.createDataFrame(all_ohlcv_records)

ohlcv_df.write \
  .mode("overwrite") \
  .json(f"{API_OUTPUT_PATH}/ohlcv/")

print(f"✅ OHLCV data written to  : {API_OUTPUT_PATH}/ohlcv/")
print(f"   Rows written           : {ohlcv_df.count()}")
print(f"   Schema:")
ohlcv_df.printSchema()


# Write Market Cap records to Volume as JSON

if len(market_cap_records) == 0:
  print("⚠️  No market cap records — skipping write")

else:
  mcap_df = spark.createDataFrame(market_cap_records)

  mcap_df.write \
    .mode("overwrite") \
    .json(f"{API_OUTPUT_PATH}/market_cap/")

  print(f"✅ Market cap data written to: {API_OUTPUT_PATH}/market_cap/")
  print(f"   Rows written              : {mcap_df.count()}")
  print(f"   Schema:")
  mcap_df.printSchema()


# Verifying files landed correctly in Volume

print("📁 Files in today's API output folder:\n")

try:
    ohlcv_files = dbutils.fs.ls(f"{API_OUTPUT_PATH}/ohlcv/")
    print(f"  /ohlcv/ → {len(ohlcv_files)} files")
    for f in ohlcv_files:
        size_kb = round(f.size / 1024, 2)
        print(f"    📄 {f.name} ({size_kb} KB)")
except:
    print("  ❌ OHLCV folder missing")

print()

try:
    mcap_files = dbutils.fs.ls(f"{API_OUTPUT_PATH}/market_cap/")
    print(f"  /market_cap/ → {len(mcap_files)} files")
    for f in mcap_files:
        size_kb = round(f.size / 1024, 2)
        print(f"    📄 {f.name} ({size_kb} KB)")
except:
    print("  ❌ Market cap folder missing")

print(f"\n✅ Ingestor run complete for {TODAY}")


# Cell 8: Quick preview of what was written

print("👀 OHLCV Sample (what Autoloader will pick up):\n")
preview_df = spark.read.json(f"{API_OUTPUT_PATH}/ohlcv/")
display(preview_df)

print("\n👀 Market Cap Sample:\n")
mcap_preview = spark.read.json(f"{API_OUTPUT_PATH}/market_cap/")
display(mcap_preview)




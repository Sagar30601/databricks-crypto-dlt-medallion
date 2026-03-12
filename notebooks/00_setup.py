# Manully Setup: Create Unity Catalog, Schemas, Volumes
# Cell 5: Verify full structure
print("=" * 50)
print("📋 CATALOG")
spark.sql("SHOW CATALOGS").filter("catalog = 'crypto_analytics'").show()

print("📋 SCHEMAS")
spark.sql("SHOW SCHEMAS IN crypto_analytics").show()

print("📋 VOLUME")
spark.sql("SHOW VOLUMES IN crypto_analytics.landing").show()

print("📋 FOLDERS IN VOLUME")
display(dbutils.fs.ls("/Volumes/crypto_analytics/landing/raw/"))

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("").getOrCreate()


catalogs = ["citibike_dev", "citibike_test", "citibike_prod"]

schemas = ["00_landing", "01_bronze", "02_silver", "03_gold"]

for catalog in catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{table};")

for catalog in catalogs:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.00_landing.source_citibike_data")

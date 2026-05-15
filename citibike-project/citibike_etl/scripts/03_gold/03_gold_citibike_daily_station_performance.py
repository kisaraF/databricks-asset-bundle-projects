# To get the project root dir to import custom modules
from pathlib import Path
import sys
from pyspark.sql.functions import avg, count, round

project_root = Path().resolve().parents[2]
sys.path.append(str(project_root))

# To retrieve the parameters at the job run time for metadata
catalog = sys.argv[1]

df = spark.read.table(f"{catalog}.02_silver.jc_citibike")

df = df.groupBy("trip_start_date", "start_station_name").agg(
    round(avg("get_trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
    count("ride_id").alias("total_trips"),
)

df.write.mode("overwrite").options(overwriteSchema=True).saveAsTable(
    f"{catalog}.03_gold.daily_station_performance"
)

# To get the project root dir to import custom modules
from pathlib import Path
import sys

project_root = Path().resolve().parents[2]
sys.path.append(str(project_root))

from citibike_project.citibike.citibike_utils import get_trip_duration_mins
from citibike_project.utils.datetime_utils import timestamp_to_date_col
from pyspark.sql.functions import create_map, lit

# To retrieve the parameters at the job run time for metadata
pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
catalog = sys.argv[5]

df = spark.read.table(f"{catalog}.01_bronze.jc_citibike")
df = get_trip_duration_mins(
    spark, df, "started_at", "ended_at", "get_trip_duration_mins"
)
df = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")

df = df.withColumn(
    "metadata",
    create_map(
        lit("pipeline_id"),
        lit(pipeline_id),
        lit("run_id"),
        lit(run_id),
        lit("task_id"),
        lit(task_id),
        lit("processed_timestamp"),
        lit(processed_timestamp),
    ),
)

df = df.select(
    "ride_id",
    "trip_start_date",
    "started_at",
    "ended_at",
    "start_station_name",
    "end_station_name",
    "get_trip_duration_mins",
    "metadata",
)

df.write.mode("overwrite").options(overwriteSchema=True).saveAsTable(
    f"{catalog}.02_silver.jc_citibike"
)

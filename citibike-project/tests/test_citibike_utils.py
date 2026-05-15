import sys
import os
from datetime import datetime

sys.path.append(os.getcwd())

from citibike_project.citibike.citibike_utils import get_trip_duration_mins
from src.citibike_project.utils.get_spark_session import create_spark_session


def test_get_trip_duration_mins(spark):
    data = [
        (
            datetime.strptime("2025-12-01 13:45:02", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-12-01 14:15:02", "%Y-%m-%d %H:%M:%S"),
        ),
        (
            datetime.strptime("2025-12-05 10:55:45", "%Y-%m-%d %H:%M:%S"),
            datetime.strptime("2025-12-05 14:55:45", "%Y-%m-%d %H:%M:%S"),
        ),
    ]

    custom_schema = "start_timestamp timestamp, end_timestamp timestamp"

    df = spark.createDataFrame(data, schema=custom_schema)

    result_df = get_trip_duration_mins(
        spark, df, "start_timestamp", "end_timestamp", "trip_duration_min"
    )

    results = list(result_df.select("trip_duration_min").collect())

    assert results[0]["trip_duration_min"] == 30
    assert results[1]["trip_duration_min"] == 240

import sys
from datetime import datetime, date, timezone
from pathlib import Path

print(sys.path)
project_root = Path().resolve().parents[2]
print(sys.path)
sys.path.append(project_root)
print(sys.path)

from citibike_project.utils.datetime_utils import timestamp_to_date_col
from citibike_project.utils.get_spark_session import create_spark_session


def test_timestamp_to_date_col(spark):
    data = [
        (datetime(2025, 12, 1, 10, 44, 56, tzinfo=timezone.utc),),
        (datetime(2025, 11, 19, 10, 44, 56, tzinfo=timezone.utc),),
        (datetime(2025, 5, 30, 10, 44, 56, tzinfo=timezone.utc),),
        (datetime(2025, 8, 23, 10, 44, 56, tzinfo=timezone.utc),),
    ]

    custom_schema = "datetime_column timestamp"

    df = spark.createDataFrame(data, schema=custom_schema)

    df = timestamp_to_date_col(spark, df, "datetime_column", "date_column")

    results = df.select("date_column").collect()

    assert results[0]["date_column"] == date(2025, 12, 1)
    assert results[1]["date_column"] == date(2025, 11, 19)
    assert results[2]["date_column"] == date(2025, 5, 30)
    assert results[3]["date_column"] == date(2025, 8, 23)

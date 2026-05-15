from databricks.connect import DatabricksSession


def create_spark_session():
    """
    Create a spark session which uses serverless compute on demand
    """
    spark = DatabricksSession.builder.serverless().profile("fe_kfs").getOrCreate()
    return spark

__SparkSession vs DatabricksSession__

When developing locally with `auth_type=databricks-cli`, the databricks connect was imported and used like below 

```python
from databricks.connect import DatabricksSession spark = DatabricksSession.builder.serverless().profile("profile_name").getOrCreate()
```
But you don't need to get the spark session like this once the code is deployed to the databricks and it will default to use `SparkSession` instead of DatabricksSession.
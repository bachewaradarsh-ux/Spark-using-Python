import boto3
import json
from pyspark.sql import SparkSession

# -------- Load Snowflake params from S3 --------
def load_params(bucket="project-data-adarshpractice",
                key="telekom_project/param_store/snowflake_param.json",
                region="ap-south-2"):
    """
    Fetch Snowflake connection parameters from S3.
    Bucket is in ap-south-2, so force boto3 to use that region.
    """
    s3 = boto3.client("s3", region_name=region)
    obj = s3.get_object(Bucket=bucket, Key=key)
    params = json.loads(obj['Body'].read().decode('utf-8'))
    return params

params = load_params()

# -------- Spark session --------
spark = SparkSession.builder.appName("snowflake_test").getOrCreate()

# -------- Snowflake connector options --------
sfOptions = {
    "sfURL": params["sfURL"],
    "sfUser": params["sfUser"],
    "sfPassword": params["sfPassword"],
    "sfDatabase": params["sfDatabase"],
    "sfSchema": params["sfSchema"],     # STAGING schema by default
    "sfWarehouse": params["sfWarehouse"],
    "sfRole": params["sfRole"]
}

# -------- Read dummy data from Snowflake --------
df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CUSTOMER_MASTER") \
    .load()

df.show()
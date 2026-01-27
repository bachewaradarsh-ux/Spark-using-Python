import boto3, json
from pyspark.sql import SparkSession

# Load Snowflake params from S3
def load_params(bucket="telecom-project", key="param_store/snowflake_params.json"):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    params = json.loads(obj['Body'].read().decode('utf-8'))
    return params

params = load_params()

# Spark session
spark = SparkSession.builder.appName("snowflake_test").getOrCreate()

# Snowflake connector options
sfOptions = {
    "sfURL": params["sfURL"],
    "sfUser": params["sfUser"],
    "sfPassword": params["sfPassword"],
    "sfDatabase": params["sfDatabase"],
    "sfSchema": params["sfSchema"],   # STAGING
    "sfWarehouse": params["sfWarehouse"],
    "sfRole": params["sfRole"]
}

# Read dummy data
df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "CUSTOMER_MASTER") \
    .load()

df.show()
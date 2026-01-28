import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Get ENV argument (dev/prod) from Glue job parameters
args = getResolvedOptions(sys.argv, ['ENV'])
ENV = args['ENV']

# Initialize Spark
spark = SparkSession.builder.appName("SnowflakeReader").getOrCreate()

# Fetch Snowflake config JSON from Parameter Store
ssm = boto3.client('ssm', region_name='ap-south-2')
param = ssm.get_parameter(Name=f"/snowflake/{ENV}/config", WithDecryption=True)
sfOptions = json.loads(param['Parameter']['Value'])

# Example: Read from staging table
df = spark.read.format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "STG_CUSTOMER_MASTER") \
    .load()

# Show results
df.show(10, truncate=False)
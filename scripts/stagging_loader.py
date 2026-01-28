import sys
import os
import json
import boto3
import logging
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import ProjectLogger from your s3_logger package
from s3_logger import ProjectLogger

# -------------------------------
# Glue Job Parameters
# -------------------------------
args = getResolvedOptions(sys.argv, ['ENV', 'INPUT_FILE'])
ENV = args['ENV']
INPUT_FILE = args['INPUT_FILE']   # e.g. s3://.../customer_master_20260127.csv.gz

# Initialize Spark
spark = SparkSession.builder.appName("TelekomStageLoader").getOrCreate()

# Initialize Logger
logger = ProjectLogger("TelekomStageLoader")

logger.log("INFO", "Job started", {"env": ENV})
logger.capture_spark_metadata(spark)

# -------------------------------
# Fetch Snowflake Config from SSM
# -------------------------------
ssm = boto3.client('ssm', region_name='ap-south-2')
param = ssm.get_parameter(Name=f"/snowflake/{ENV}/config", WithDecryption=True)
sfOptions = json.loads(param['Parameter']['Value'])

# -------------------------------
# Fetch Batch ID from Snowflake CONFIG.BATCH
# -------------------------------
try:
    batch_df = spark.read.format("snowflake") \
        .options(**sfOptions) \
        .option("sfSchema", "CONFIG") \
        .option("query", "select coalesce(max(batch_id),0) as BATCH_ID from batch_control where batch_name = 'STG_LOAD'") \
        .load()
    batch_id = batch_df.collect()[0]["BATCH_ID"]
    logger.log("INFO", "Fetched batch_id from CONFIG.BATCH", {"batch_id": batch_id})
except Exception as e:
    logger.log("ERROR", "Failed to fetch batch_id", {"error": str(e)})
    batch_id = 0

# -------------------------------
# Process Input File
# -------------------------------
filename = INPUT_FILE
parts = filename.replace(".csv.gz", "").split("_")
table_name = parts[0] + "_" + parts[1]   # e.g. customer_master
lieferdatum = parts[-1]                  # e.g. 20260127

logger.log("INFO", "Processing file", {
    "file": filename,
    "table": table_name,
    "lieferdatum": lieferdatum,
    "batch_id": batch_id
})

# Read gzipped CSV into Spark DataFrame
df = spark.read.option("header", "true").csv(INPUT_FILE)

# Add metadata columns
df = df.withColumn("lieferdatum", F.lit(substr(lieferdatum,1,8))) \
       .withColumn("batch_id", F.lit(batch_id))

# -------------------------------
# Load into Snowflake Staging Table
# -------------------------------
try:
    df.write.format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", f"STG_{table_name.upper()}") \
        .mode("append") \
        .save()

    logger.log("INFO", "File loaded successfully", {
        "table": f"STG_{table_name.upper()}",
        "rows": df.count()
    })
except Exception as e:
    logger.log("ERROR", "Failed to load file", {
        "file": filename,
        "error": str(e)
    })

# -------------------------------
# Finalize
# -------------------------------
logger.log("INFO", "Job completed", {"status": "success"})
logger.flush_to_s3()
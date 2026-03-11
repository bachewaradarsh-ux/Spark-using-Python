import sys
import re
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import lit
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom logger
from s3_logger import S3Logger

# ==========================================================
# 1. READ GLUE JOB PARAMETERS
# ==========================================================
args = getResolvedOptions(
    sys.argv,
    [
        'S3_BUCKET',
        'S3_PREFIX',
        'PARAMETER_PREFIX',     
        'LOG_BUCKET'
    ]
)

s3_bucket = args['S3_BUCKET']
s3_prefix = args['S3_PREFIX']
parameter_prefix = args['PARAMETER_PREFIX']

# ==========================================================
# 2. INITIALIZE SPARK
# ==========================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ==========================================================
# 3. INITIALIZE LOGGER
# ==========================================================
logger = S3Logger(bucket=args['LOG_BUCKET'], log_prefix="stage_load_logs/")
logger.log("JOB_STARTED")

# ==========================================================
# 4. FETCH SNOWFLAKE CREDS FROM PARAMETER STORE
# ==========================================================
def get_ssm_parameter(name):
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response["Parameter"]["Value"]

try:
    sf_user = get_ssm_parameter(f"{parameter_prefix}/user")
    sf_password = get_ssm_parameter(f"{parameter_prefix}/password")
    sf_account = get_ssm_parameter(f"{parameter_prefix}/account")
    sf_warehouse = get_ssm_parameter(f"{parameter_prefix}/warehouse")
    sf_database = get_ssm_parameter(f"{parameter_prefix}/database")
    sf_schema = get_ssm_parameter(f"{parameter_prefix}/schema")
    logger.log("Successfully fetched Snowflake credentials")
except Exception as e:
    logger.log(f"Failed to fetch credentials: {str(e)}")
    sys.exit(1)

# ==========================================================
# 5. CONNECT TO SNOWFLAKE
# ==========================================================
try:
    sf_conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    sf_cursor = sf_conn.cursor()
    logger.log("Connected to Snowflake")
except Exception as e:
    logger.log(f"Snowflake Connection Failed: {str(e)}")
    sys.exit(1)

# ==========================================================
# 6. GENERATE NEW BATCH ID
# ==========================================================
sf_cursor.execute("SELECT COALESCE(MAX(BATCH_ID),0) FROM A_BATCH")
max_batch_id = sf_cursor.fetchone()[0]
new_batch_id = max_batch_id + 1
logger.log(f"Generated New Batch ID: {new_batch_id}")

# ==========================================================
# 7. FETCH METADATA FROM A_BATCH_INPUT
# ==========================================================
sf_cursor.execute("""
    SELECT SOURCE_FILE_PREFIX, TARGET_TABLE
    FROM A_BATCH_INPUT
    WHERE ACTIVE_FLAG = 'Y'
""")
rows = sf_cursor.fetchall()
mapping_dict = {row[0]: row[1] for row in rows}
logger.log(f"Loaded metadata for {len(mapping_dict)} tables")

# ==========================================================
# 8. LIST FILES FROM S3
# ==========================================================
s3_client = boto3.client("s3")
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv.gz")]
logger.log(f"Total files detected: {len(files)}")

# ==========================================================
# 9. SNOWFLAKE SPARK CONNECTOR CONFIG
# ==========================================================
sfOptions = {
    "sfURL": f"{sf_account}.snowflakecomputing.com",
    "sfUser": sf_user,
    "sfPassword": sf_password,
    "sfDatabase": sf_database,
    "sfSchema": sf_schema,
    "sfWarehouse": sf_warehouse
}

# ==========================================================
# 10. DEFINE FILE PROCESSING FUNCTION
# ==========================================================
def process_file(file_key):
    file_name = file_key.split("/")[-1]
    prefix = file_name.split("_")[0]

    if prefix not in mapping_dict:
        logger.log(f"Skipping (No mapping found): {file_name}")
        return False

    target_table = mapping_dict[prefix]

    match = re.search(r'_(\d{8})\d{6}\.csv\.gz$', file_name)
    if not match:
        logger.log(f"Invalid filename format: {file_name}")
        return False

    processing_date = datetime.strptime(match.group(1), "%Y%m%d").date()

    try:
        logger.log(f"Processing file: {file_name}")

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(f"s3://{s3_bucket}/{file_key}")

        df = df.withColumn("BATCH_ID", lit(new_batch_id)) \
               .withColumn("PROCESSING_DATE", lit(str(processing_date)))

        sf_cursor.execute(f"TRUNCATE TABLE {target_table}")
        sf_conn.commit()

        df.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("dbtable", target_table) \
            .mode("append") \
            .save()

        df.unpersist()  # free memory after write

        logger.log(f"Successfully loaded: {file_name} → {target_table}")
        return True

    except Exception as e:
        logger.log(f"FAILED: {file_name} | Error: {str(e)}")
        return False

# ==========================================================
# 11. PROCESS FILES WITH THREADPOOLEXECUTOR
# ==========================================================
job_failed = False
files_processed = 0
MAX_THREADS = 4  # number of parallel tables

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {executor.submit(process_file, f): f for f in files}

    for future in as_completed(futures):
        result = future.result()
        if result:
            files_processed += 1
        else:
            job_failed = True
            # Optionally: break here if you want to fail-fast
            # break

# ==========================================================
# 12. INSERT INTO A_BATCH IF SUCCESS
# ==========================================================
if not job_failed:
    sf_cursor.execute(f"""
        INSERT INTO A_BATCH
        (BATCH_ID, BATCH_DATE, STATUS, CREATED_TS)
        VALUES
        ({new_batch_id}, CURRENT_DATE(), 'SUCCESS', CURRENT_TIMESTAMP())
    """)
    sf_conn.commit()
    logger.log(f"Batch {new_batch_id} committed successfully")
    logger.log(f"Files processed: {files_processed}")
    logger.log("JOB_COMPLETED_SUCCESS")
else:
    logger.log("JOB_FAILED - Batch not committed")
    sys.exit(1)

# ==========================================================
# 13. CLOSE CONNECTIONS
# ==========================================================
sf_cursor.close()
sf_conn.close()
logger.log("Snowflake connection closed")

import sys
import json
import boto3
import snowflake.connector
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Project Logger
from s3_logger import ProjectLogger


# -------------------------------------------------------
# Read Glue Arguments
# -------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'file_prefix',
        'file_date',
        'SNOWFLAKE_SECRET_NAME',
        'WAREHOUSE',
        'METADATA_DB',
        'METADATA_SCHEMA',
        'STAGE_DB',
        'STAGE_SCHEMA'
    ]
)

JOB_NAME = args['JOB_NAME']
FILE_PREFIX = args['file_prefix']
FILE_DATE = args['file_date']

SECRET_NAME = args['SNOWFLAKE_SECRET_NAME']
WAREHOUSE = args['WAREHOUSE']

METADATA_DB = args['METADATA_DB']
METADATA_SCHEMA = args['METADATA_SCHEMA']

STAGE_DB = args['STAGE_DB']
STAGE_SCHEMA = args['STAGE_SCHEMA']


# -------------------------------------------------------
# Spark Session
# -------------------------------------------------------

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# -------------------------------------------------------
# Initialize Logger
# -------------------------------------------------------

logger = ProjectLogger(JOB_NAME)

logger.log("INFO", "Glue Stage Loader Started", {
    "job_name": JOB_NAME,
    "file_prefix": FILE_PREFIX,
    "file_date": FILE_DATE
})

logger.capture_spark_metadata(spark)


# -------------------------------------------------------
# AWS Client
# -------------------------------------------------------

ssm = boto3.client('ssm')


# -------------------------------------------------------
# Snowflake Connection
# -------------------------------------------------------

def get_snowflake_connection():

    logger.log("INFO", "Fetching Snowflake credentials")

    param = ssm.get_parameter(
        Name=SECRET_NAME,
        WithDecryption=True
    )

    creds = json.loads(param['Parameter']['Value'])

    conn = snowflake.connector.connect(
        user=creds['sfUser'],
        password=creds['sfPassword'],
        account=creds['sfURL'],
        warehouse=WAREHOUSE
    )

    logger.log("INFO", "Snowflake connection established")

    return conn


# -------------------------------------------------------
# Generate Batch ID (use sequence in production)
# -------------------------------------------------------

def get_batch_id(cursor):

    cursor.execute(f"""
    SELECT COALESCE(MAX(BATCH_ID),0)+1
    FROM {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    """)

    batch_id = cursor.fetchone()[0]

    logger.log("INFO", "Generated Batch ID", {
        "batch_id": batch_id
    })

    return batch_id


# -------------------------------------------------------
# Get File from Manifest
# -------------------------------------------------------

def get_file_from_manifest(cursor, prefix):

    logger.log("INFO", "Fetching file from FILE_ARRIVAL_MANIFEST", {
        "prefix": prefix,
        "file_date": FILE_DATE
    })

    cursor.execute(f"""
        SELECT file_name, s3_path
        FROM {METADATA_DB}.{METADATA_SCHEMA}.FILE_ARRIVAL_MANIFEST
        WHERE file_prefix=%s
        AND file_date=TO_DATE(%s,'YYYYMMDD')
    """, (prefix, FILE_DATE))

    result = cursor.fetchone()

    if not result:
        raise Exception(f"No file found in manifest for prefix {prefix}")

    return result[0], result[1]


# -------------------------------------------------------
# Get Stage Table Mapping
# -------------------------------------------------------

def get_stage_table(cursor, prefix):

    logger.log("INFO", "Fetching stage table mapping", {
        "prefix": prefix
    })

    cursor.execute(f"""
        SELECT stage_table
        FROM {METADATA_DB}.{METADATA_SCHEMA}.PIPELINE_CONFIG
        WHERE file_prefix=%s
    """, (prefix,))

    result = cursor.fetchone()

    if not result:
        raise Exception(f"No stage table mapping found for prefix {prefix}")

    return result[0]


# -------------------------------------------------------
# Insert Batch Log
# -------------------------------------------------------

def insert_batch_log(cursor,batch_id):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    (
        BATCH_ID,
        BATCH_DATE,
        BATCH_STATUS,
        TOTAL_FILES_EXPECTED,
        TOTAL_FILES_RECEIVED,
        STAGE_STATUS,
        IS_ACTIVE_BATCH,
        BATCH_START_TS
    )
    VALUES
    (
        %s,
        TO_DATE(%s,'YYYYMMDD'),
        'IN_PROGRESS',
        1,
        0,
        'IN_PROGRESS',
        'Y',
        CURRENT_TIMESTAMP
    )
    """,
    (batch_id,FILE_DATE)
    )


# -------------------------------------------------------
# Log File Status
# -------------------------------------------------------

def log_file_status(cursor,batch_id,file_name,table_name,status,rows,error=None):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{METADATA_SCHEMA}.FILE_PROCESS_LOG
    (
        BATCH_ID,
        FILE_NAME,
        STAGE_TABLE,
        ROWS_LOADED,
        STATUS,
        ERROR_MESSAGE,
        START_TS,
        END_TS
    )
    VALUES
    (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    """,
    (batch_id,file_name,table_name,rows,status,error)
    )


# -------------------------------------------------------
# Load Stage Table
# -------------------------------------------------------

def load_stage_table(cursor,batch_id,prefix):

    file_name, s3_path = get_file_from_manifest(cursor,prefix)

    stage_table = get_stage_table(cursor,prefix)

    logger.log("INFO", "Starting file load", {
        "file": file_name,
        "stage_table": stage_table
    })

    try:

        cursor.execute(f"""
        TRUNCATE TABLE {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        """)

        copy_sql = f"""
        COPY INTO {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        FROM
        (
            SELECT
            t.$1,
            t.$2,
            t.$3,
            t.$4,
            {batch_id} AS BATCH_ID,
            TO_DATE('{FILE_DATE}','YYYYMMDD') AS FILE_DATE
            FROM @{s3_path}/{file_name} t
        )
        FILE_FORMAT=(
            TYPE=CSV
            COMPRESSION=GZIP
            SKIP_HEADER=1
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
        )
        """

        cursor.execute(copy_sql)

        cursor.execute(f"""
        SELECT COUNT(*)
        FROM {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        """)

        rows = cursor.fetchone()[0]

        log_file_status(
            cursor,
            batch_id,
            file_name,
            stage_table,
            "SUCCESS",
            rows
        )

        logger.log("INFO", "File loaded successfully", {
            "rows_loaded": rows
        })

        return True

    except Exception as e:

        logger.log("ERROR", "File load failed", {
            "error": str(e)
        })

        log_file_status(
            cursor,
            batch_id,
            file_name,
            stage_table,
            "FAILED",
            0,
            str(e)
        )

        return False


# -------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------

conn = get_snowflake_connection()
cursor = conn.cursor()

try:

    batch_id = get_batch_id(cursor)

    insert_batch_log(cursor,batch_id)

    conn.commit()

    success = load_stage_table(
        cursor,
        batch_id,
        FILE_PREFIX
    )

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    SET
        TOTAL_FILES_RECEIVED=1,
        STAGE_STATUS='COMPLETED',
        BATCH_STATUS='COMPLETED',
        BATCH_END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    """,(batch_id,))

    conn.commit()

    logger.log("INFO", "Batch Completed", {
        "batch_id": batch_id
    })


except Exception as e:

    logger.log("ERROR", "Batch Failed", {
        "error": str(e)
    })

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    SET
        STAGE_STATUS='FAILED',
        BATCH_STATUS='FAILED',
        BATCH_END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    """,(batch_id,))

    conn.commit()

    raise e

finally:

    cursor.close()
    conn.close()

    logger.log("INFO", "Glue Job Finished")

    logger.flush_summary({
        "batch_id": batch_id,
        "file_prefix": FILE_PREFIX,
        "file_date": FILE_DATE,
        "status": "COMPLETED"
    })

print("Stage Loader Completed")
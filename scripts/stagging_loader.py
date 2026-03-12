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
        'file_date',
        'config',
        'SNOWFLAKE_SECRET_NAME',
        'WAREHOUSE',
        'METADATA_DB',
        'METADATA_SCHEMA',
        'STAGE_DB',
        'STAGE_SCHEMA'
    ]
)

JOB_NAME = args['JOB_NAME']
FILE_DATE = args['file_date']
CONFIG = json.loads(args['config'])

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

    logger.log("INFO", "Fetching Snowflake credentials from SSM")

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
# Get Batch ID
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
# Insert Batch Log
# -------------------------------------------------------

def insert_batch_log(cursor,batch_id,total_files):

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
        %s,
        0,
        'IN_PROGRESS',
        'Y',
        CURRENT_TIMESTAMP
    )
    """,
    (batch_id,FILE_DATE,total_files)
    )

    logger.log("INFO", "Batch Run Log Inserted", {
        "batch_id": batch_id,
        "total_files_expected": total_files
    })


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

    logger.log("INFO", "File Process Logged", {
        "file": file_name,
        "table": table_name,
        "status": status,
        "rows": rows
    })


# -------------------------------------------------------
# Load Stage Table
# -------------------------------------------------------

def load_stage_table(cursor,batch_id,prefix,stage_table):

    file_name = f"{prefix}_{FILE_DATE}.csv.gz"

    logger.log("INFO", "Starting File Load", {
        "file": file_name,
        "stage_table": stage_table
    })

    try:

        cursor.execute(f"""
        TRUNCATE TABLE
        {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
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
            FROM @TELECOM_STAGE/{file_name} t
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

        logger.log("INFO", "File Loaded Successfully", {
            "file": file_name,
            "rows_loaded": rows
        })

        return True

    except Exception as e:

        logger.log("ERROR", "File Load Failed", {
            "file": file_name,
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

    total_files = sum(len(g["files"]) for g in CONFIG["groups"])

    batch_id = get_batch_id(cursor)

    logger.log("INFO", "Starting Batch Processing", {
        "batch_id": batch_id,
        "file_date": FILE_DATE
    })

    insert_batch_log(cursor,batch_id,total_files)

    conn.commit()

    loaded_files = 0

    for group in CONFIG["groups"]:

        logger.log("INFO", "Processing Group", {
            "group_id": group["group"]
        })

        for file in group["files"]:

            prefix = file["prefix"]
            stage_table = file["stage_table"]

            success = load_stage_table(
                cursor,
                batch_id,
                prefix,
                stage_table
            )

            if success:
                loaded_files += 1

            conn.commit()

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    SET
        TOTAL_FILES_RECEIVED=%s,
        STAGE_STATUS='COMPLETED',
        BATCH_STATUS='COMPLETED',
        BATCH_END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    """,
    (loaded_files,batch_id)
    )

    conn.commit()

    logger.log("INFO", "Batch Completed Successfully", {
        "batch_id": batch_id,
        "files_loaded": loaded_files
    })


except Exception as e:

    logger.log("ERROR", "Batch Failed", {
        "batch_id": batch_id,
        "error": str(e)
    })

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    SET
        STAGE_STATUS='FAILED',
        BATCH_STATUS='FAILED',
        BATCH_END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    """,(batch_id,)
    )

    conn.commit()

    raise e

finally:

    cursor.close()
    conn.close()

    logger.log("INFO", "Glue Job Finished")

    logger.flush_summary({
        "batch_id": batch_id,
        "file_date": FILE_DATE,
        "status": "COMPLETED"
    })

print("Stage Loader Completed")
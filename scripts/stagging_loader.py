import sys
import json
import boto3
import snowflake.connector
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from s3_logger import ProjectLogger

# -------------------------------------------------------
# Read Glue Arguments
# -------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
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
# Logger
# -------------------------------------------------------

logger = ProjectLogger(JOB_NAME)

logger.log("INFO","Glue Job Started",{
    "file_date": FILE_DATE
})

logger.capture_spark_metadata(spark)


# -------------------------------------------------------
# AWS Client
# -------------------------------------------------------

ssm = boto3.client("ssm")


# -------------------------------------------------------
# Snowflake Connection
# -------------------------------------------------------

def get_snowflake_connection():

    param = ssm.get_parameter(
        Name=SECRET_NAME,
        WithDecryption=True
    )

    creds = json.loads(param['Parameter']['Value'])

    conn = snowflake.connector.connect(
        user=creds['sfUser'],
        password=creds['sfPassword'],
        account=creds['sfURL'],
        warehouse=WAREHOUSE,
        database=METADATA_DB,
        schema=METADATA_SCHEMA
    )

    logger.log("INFO","Snowflake connection established")

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

    logger.log("INFO","Generated Batch ID",{
        "batch_id":batch_id
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
        TOTAL_FILES_EXPECTED,
        TOTAL_FILES_RECEIVED,
        BATCH_STATUS,
        BATCH_START_TS
    )
    VALUES
    (%s,TO_DATE(%s,'YYYYMMDD'),%s,0,'IN_PROGRESS',CURRENT_TIMESTAMP)
    """,(batch_id,FILE_DATE,total_files))

    logger.log("INFO","Batch Run Inserted",{
        "batch_id":batch_id,
        "files_expected":total_files
    })


# -------------------------------------------------------
# Fetch Files From Metadata
# -------------------------------------------------------

def get_files_to_process(cursor):

    cursor.execute(f"""
    SELECT
        m.file_name,
        m.s3_path,
        c.stage_table
    FROM {METADATA_DB}.{METADATA_SCHEMA}.FILE_ARRIVAL_MANIFEST m
    JOIN {METADATA_DB}.{METADATA_SCHEMA}.PIPELINE_CONFIG c
      ON m.file_prefix = c.file_prefix
    WHERE m.file_date = TO_DATE(%s,'YYYYMMDD')
    """,(FILE_DATE,))

    files = cursor.fetchall()

    logger.log("INFO","Files fetched for processing",{
        "total_files":len(files)
    })

    return files


# -------------------------------------------------------
# Log File Status
# -------------------------------------------------------

def log_file_status(cursor,batch_id,file_name,stage_table,status,rows,error=None):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{METADATA_SCHEMA}.FILE_PROCESS_LOG
    (
        BATCH_ID,
        FILE_NAME,
        STAGE_TABLE,
        STATUS,
        ROWS_LOADED,
        ERROR_MESSAGE,
        START_TS,
        END_TS
    )
    VALUES
    (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    """,(batch_id,file_name,stage_table,status,rows,error))

    logger.log("INFO","File log inserted",{
        "file":file_name,
        "table":stage_table,
        "status":status,
        "rows":rows
    })


# -------------------------------------------------------
# Load Stage Table
# -------------------------------------------------------

def load_stage_table(cursor,batch_id,file_name,stage_table):

    try:

        logger.log("INFO","Starting file load",{
            "file":file_name,
            "stage_table":stage_table
        })

        cursor.execute(f"""
        TRUNCATE TABLE {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        """)

        copy_sql=f"""
        COPY INTO {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        FROM
        (
            SELECT
            t.$1,
            t.$2,
            t.$3,
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

        cursor.execute("""
        SELECT rows_loaded
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
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

        return True

    except Exception as e:

        logger.log("ERROR","File Load Failed",{
            "file":file_name,
            "error":str(e)
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

    files = get_files_to_process(cursor)

    total_files = len(files)

    batch_id = get_batch_id(cursor)

    insert_batch_log(cursor,batch_id,total_files)

    conn.commit()

    loaded_files = 0

    for file_name,s3_path,stage_table in files:

        success = load_stage_table(
            cursor,
            batch_id,
            file_name,
            stage_table
        )

        if success:
            loaded_files += 1

        conn.commit()

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    SET
        TOTAL_FILES_RECEIVED=%s,
        BATCH_STATUS='COMPLETED',
        BATCH_END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    """,(loaded_files,batch_id))

    conn.commit()

    logger.log("INFO","Batch Completed",{
        "batch_id":batch_id,
        "files_loaded":loaded_files
    })


except Exception as e:

    logger.log("ERROR","Batch Failed",{
        "error":str(e)
    })

    raise e


finally:

    cursor.close()
    conn.close()

logger.log("INFO","Glue Job Finished")

print("Stage Loader Completed")
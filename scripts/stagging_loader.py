import sys
import json
import boto3
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from s3_logger import ProjectLogger

# ---------------------------------------------------
# Glue Arguments
# ---------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'file_date',
    'SNOWFLAKE_SECRET_NAME',
    'WAREHOUSE'
])

JOB_NAME = args['JOB_NAME']
FILE_DATE = args['file_date']
SECRET_NAME = args['SNOWFLAKE_SECRET_NAME']
WAREHOUSE = args['WAREHOUSE']
METADATA_DB = args['METADATA_DB']
CONFIG_SCHEMA = args['METADATA_SCHEMA']
STAGE_DB = args['STAGE_DB']
STAGE_SCHEMA = args['STAGE_SCHEMA']

MAX_THREADS = 10

# ---------------------------------------------------
# Spark Session
# ---------------------------------------------------

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------
# Logger
# ---------------------------------------------------

logger = ProjectLogger(JOB_NAME)
logger.log("INFO","Stage Loader Started",{"file_date":FILE_DATE})

# ---------------------------------------------------
# AWS Clients
# ---------------------------------------------------

ssm = boto3.client("ssm")

# ---------------------------------------------------
# Snowflake Connection
# ---------------------------------------------------

def get_snowflake_connection():

    param = ssm.get_parameter(Name=SECRET_NAME, WithDecryption=True)
    creds = json.loads(param['Parameter']['Value'])

    conn = snowflake.connector.connect(
        user = creds['sfUser'],
        password = creds['sfPassword'],
        account = creds['sfURL'],
        warehouse = WAREHOUSE
    )

    return conn

# ---------------------------------------------------
# Get Files From Manifest
# ---------------------------------------------------

def get_files(cursor):

    cursor.execute(f"""
    SELECT
        m.file_name,
        c.stage_table
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.FILE_ARRIVAL_MANIFEST m
    JOIN {METADATA_DB}.{CONFIG_SCHEMA}.PIPELINE_CONFIG c
        ON m.file_prefix = c.file_prefix
    WHERE m.file_date = TO_DATE(%s,'YYYYMMDD')
    AND c.active_flag = 'Y'
    """,(FILE_DATE,))

    return cursor.fetchall()

# ---------------------------------------------------
# Start Batch
# ---------------------------------------------------

def start_batch(cursor,total_files):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
    (
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
        TO_DATE('{FILE_DATE}','YYYYMMDD'),
        'RUNNING',
        {total_files},
        {total_files},
        'RUNNING',
        'Y',
        CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("SELECT LAST_INSERT_ID()")

    return cursor.fetchone()[0]

# ---------------------------------------------------
# Start Job Run
# ---------------------------------------------------

def start_job_run(cursor,batch_id):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    (
        BATCH_ID,
        JOB_NAME,
        JOB_LAYER,
        JOB_STATUS,
        START_TS
    )
    VALUES
    (
        %s,
        %s,
        'STAGE',
        'RUNNING',
        CURRENT_TIMESTAMP
    )
    """,(batch_id,JOB_NAME))

    cursor.execute("SELECT LAST_INSERT_ID()")

    return cursor.fetchone()[0]

# ---------------------------------------------------
# Insert File Log
# ---------------------------------------------------

def insert_file_log(cursor,batch_id,file_name,stage_table):

    cursor.execute(f"""
    INSERT INTO {METADATA_DB}.{CONFIG_SCHEMA}.FILE_PROCESS_LOG
    (
        BATCH_ID,
        FILE_NAME,
        STAGE_TABLE,
        START_TS,
        STATUS
    )
    VALUES
    (
        %s,%s,%s,CURRENT_TIMESTAMP,'RUNNING'
    )
    """,(batch_id,file_name,stage_table))

# ---------------------------------------------------
# Update File Log
# ---------------------------------------------------

def update_file_status(cursor,batch_id,file_name,status,rows,error):

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.FILE_PROCESS_LOG
    SET
        STATUS=%s,
        ROWS_LOADED=%s,
        ERROR_MESSAGE=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
    AND FILE_NAME=%s
    """,(status,rows,error,batch_id,file_name))

# ---------------------------------------------------
# Update Batch Status
# ---------------------------------------------------

def end_batch(cursor,batch_id,status):

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
    SET
        BATCH_STATUS=%s,
        STAGE_STATUS=%s,
        BATCH_END_TS=CURRENT_TIMESTAMP,
        IS_ACTIVE_BATCH='N'
    WHERE BATCH_ID=%s
    """,(status,status,batch_id))

# ---------------------------------------------------
# Update Job Run
# ---------------------------------------------------

def end_job_success(cursor,job_run_id,records):

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    SET
        JOB_STATUS='SUCCESS',
        RECORDS_PROCESSED=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE JOB_RUN_ID=%s
    """,(records,job_run_id))


def end_job_failed(cursor,job_run_id,error):

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    SET
        JOB_STATUS='FAILED',
        ERROR_MESSAGE=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE JOB_RUN_ID=%s
    """,(error,job_run_id))

# ---------------------------------------------------
# Stage Load Function
# ---------------------------------------------------

def load_stage(batch_id,file_name,stage_table):

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:

        cursor.execute(f"TRUNCATE TABLE {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}")

        copy_sql = f"""
        COPY INTO {STAGE_DB}.{STAGE_SCHEMA}.{stage_table}
        FROM (
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
        )
        """

        cursor.execute(copy_sql)

        cursor.execute("""
        SELECT rows_loaded
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        """)

        rows = cursor.fetchone()[0]

        conn.commit()

        return (file_name,stage_table,"SUCCESS",rows,None)

    except Exception as e:

        return (file_name,stage_table,"FAILED",0,str(e))

    finally:

        cursor.close()
        conn.close()

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

conn = get_snowflake_connection()
cursor = conn.cursor()

job_run_id = None
batch_id = None

try:

    files = get_files(cursor)

    total_files = len(files)

    if total_files == 0:
        raise Exception("No files found in manifest")

    batch_id = start_batch(cursor,total_files)

    job_run_id = start_job_run(cursor,batch_id)

    conn.commit()

    logger.log("INFO","Batch Started",{"batch_id":batch_id})

    for file_name,stage_table in files:
        insert_file_log(cursor,batch_id,file_name,stage_table)

    conn.commit()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures = [
            executor.submit(load_stage,batch_id,file_name,stage_table)
            for file_name,stage_table in files
        ]

        for future in as_completed(futures):
            results.append(future.result())

    success = 0
    total_records = 0

    for file_name,stage_table,status,rows,error in results:

        update_file_status(cursor,batch_id,file_name,status,rows,error)

        if status == "SUCCESS":
            success += 1
            total_records += rows

    batch_status = "SUCCESS" if success == total_files else "FAILED"

    end_batch(cursor,batch_id,batch_status)

    end_job_success(cursor,job_run_id,total_records)

    conn.commit()

    logger.log("INFO","Batch Completed",{
        "batch_id":batch_id,
        "files_loaded":success,
        "records":total_records
    })

except Exception as e:

    logger.log("ERROR","Pipeline Failed",{"error":str(e)})

    if job_run_id:
        end_job_failed(cursor,job_run_id,str(e))

    if batch_id:
        end_batch(cursor,batch_id,"FAILED")

    conn.commit()

    raise e

finally:

    cursor.close()
    conn.close()

logger.log("INFO","Glue Job Finished")
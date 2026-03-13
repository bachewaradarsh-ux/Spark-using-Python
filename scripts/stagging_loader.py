import sys
import json
import boto3
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from s3_logger import ProjectLogger

# ------------------------------------------------
# Glue Arguments
# ------------------------------------------------

args = getResolvedOptions(sys.argv,[
    "JOB_NAME",
    "file_date",
    "SNOWFLAKE_SECRET_NAME",
    "WAREHOUSE",
    "METADATA_DB",
    "CONFIG_SCHEMA",
    "STAGE_DB",
    "STAGE_SCHEMA"
])

JOB_NAME = args['JOB_NAME']
FILE_DATE = args['file_date']
SECRET_NAME = args['SNOWFLAKE_SECRET_NAME']
WAREHOUSE = args['WAREHOUSE']

METADATA_DB = args['METADATA_DB']
CONFIG_SCHEMA = args['CONFIG_SCHEMA']
STAGE_DB = args['STAGE_DB']
STAGE_SCHEMA = args['STAGE_SCHEMA']

MAX_THREADS = 10

# ------------------------------------------------
# Spark
# ------------------------------------------------

spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger = ProjectLogger(JOB_NAME)

# ------------------------------------------------
# AWS
# ------------------------------------------------

ssm = boto3.client("ssm")

# ------------------------------------------------
# Snowflake Connection
# ------------------------------------------------

def get_conn():
    secret = ssm.get_parameter(Name=SECRET_NAME,WithDecryption=True)
    creds = json.loads(secret['Parameter']['Value'])

    conn = snowflake.connector.connect(
        user=creds['sfUser'],
        password=creds['sfPassword'],
        account=creds['sfURL'],
        warehouse=WAREHOUSE
    )

    return conn

def use_context(cursor, db, schema):
    cursor.execute(f"USE DATABASE {db}")
    cursor.execute(f"USE SCHEMA {schema}")

# ------------------------------------------------
# Idempotent Batch Handling
# ------------------------------------------------

def get_or_create_batch(cursor,total_files):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

    cursor.execute(f"""
    SELECT BATCH_ID,BATCH_STATUS
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
    WHERE BATCH_DATE = TO_DATE(%s,'YYYYMMDD')
    ORDER BY BATCH_ID DESC
    LIMIT 1
    """,(FILE_DATE,))

    row = cursor.fetchone()

    if row:
        batch_id,status = row
        if status == "SUCCESS":
            raise Exception("Batch already completed")
        cursor.execute(f"""
        UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
        SET BATCH_STATUS='RUNNING',
            STAGE_STATUS='RUNNING',
            IS_ACTIVE_BATCH='Y'
        WHERE BATCH_ID=%s
        """,(batch_id,))
        return batch_id

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
    TO_DATE(%s,'YYYYMMDD'),
    'RUNNING',
    %s,
    %s,
    'RUNNING',
    'Y',
    CURRENT_TIMESTAMP
    )
    """,(FILE_DATE,total_files,total_files))

    cursor.execute(f"""
    SELECT MAX(BATCH_ID)
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
    WHERE BATCH_DATE=TO_DATE(%s,'YYYYMMDD')
    """,(FILE_DATE,))

    return cursor.fetchone()[0]

# ------------------------------------------------
# Job Run Log
# ------------------------------------------------

def start_job(cursor,batch_id):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

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
    (%s,%s,'STAGE','RUNNING',CURRENT_TIMESTAMP)
    """,(batch_id,JOB_NAME))

    cursor.execute(f"""
    SELECT MAX(JOB_RUN_ID)
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    """)

    return cursor.fetchone()[0]

# ------------------------------------------------
# Fetch Files
# ------------------------------------------------

def get_files(cursor):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

    cursor.execute(f"""
    SELECT
    m.FILE_NAME,
    c.STAGE_TABLE,
    m.S3_PATH
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.FILE_ARRIVAL_MANIFEST m
    JOIN {METADATA_DB}.{CONFIG_SCHEMA}.PIPELINE_CONFIG c
      ON m.FILE_PREFIX=c.FILE_PREFIX
    WHERE m.FILE_DATE=TO_DATE(%s,'YYYYMMDD')
      AND c.ACTIVE_FLAG='Y'
    """,(FILE_DATE,))

    return cursor.fetchall()

# ------------------------------------------------
# Resume Failed Tables
# ------------------------------------------------

def filter_files_for_resume(cursor,batch_id,files):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

    cursor.execute(f"""
    SELECT FILE_NAME,STATUS
    FROM {METADATA_DB}.{CONFIG_SCHEMA}.FILE_PROCESS_LOG
    WHERE BATCH_ID=%s
    """,(batch_id,))

    status_map = {r[0]:r[1] for r in cursor.fetchall()}

    filtered=[]
    for f,t,p in files:
        if f not in status_map or status_map[f] != "SUCCESS":
            filtered.append((f,t,p))

    return filtered

# ------------------------------------------------
# File Log
# ------------------------------------------------

def insert_file_log(cursor,batch_id,file,table):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

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
    (%s,%s,%s,CURRENT_TIMESTAMP,'RUNNING')
    """,(batch_id,file,table))

# ------------------------------------------------
# Update File Status
# ------------------------------------------------

def update_file(cursor,batch_id,file,status,rows,error):
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.FILE_PROCESS_LOG
    SET STATUS=%s,
        ROWS_LOADED=%s,
        ERROR_MESSAGE=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE BATCH_ID=%s
      AND FILE_NAME=%s
    """,(status,rows,error,batch_id,file))

# ------------------------------------------------
# Load Stage
# ------------------------------------------------

def load_stage(batch_id,file,table,s3_path):
    conn=get_conn()
    cur=conn.cursor()
    try:
        use_context(cur, STAGE_DB, STAGE_SCHEMA)

        cur.execute(f"TRUNCATE TABLE {STAGE_DB}.{STAGE_SCHEMA}.{table}")

        from_path = f"'{s3_path}/{file}'"

        cur.execute(f"""
        COPY INTO {STAGE_DB}.{STAGE_SCHEMA}.{table}
        FROM (
        SELECT
        t.$1,
        t.$2,
        t.$3,
        {batch_id},
        TO_DATE('{FILE_DATE}','YYYYMMDD')
        FROM {from_path} t
        )
        FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP SKIP_HEADER=1)
        """)

        cur.execute("SELECT rows_loaded FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
        rows=cur.fetchone()[0]
        conn.commit()
        return(file,table,"SUCCESS",rows,None)

    except Exception as e:
        return(file,table,"FAILED",0,str(e))
    finally:
        cur.close()
        conn.close()

# ------------------------------------------------
# MAIN
# ------------------------------------------------

conn=get_conn()
cursor=conn.cursor()

try:
    files=get_files(cursor)
    total=len(files)

    batch_id=get_or_create_batch(cursor,total)

    job_run_id=start_job(cursor,batch_id)

    conn.commit()

    files=filter_files_for_resume(cursor,batch_id,files)

    for f,t,p in files:
        insert_file_log(cursor,batch_id,f,t)

    conn.commit()

    results=[]
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as exe:
        futures=[exe.submit(load_stage,batch_id,f,t,p) for f,t,p in files]
        for future in as_completed(futures):
            results.append(future.result())

    success=0
    records=0
    for f,t,s,r,e in results:
        update_file(cursor,batch_id,f,s,r,e)
        if s=="SUCCESS":
            success+=1
            records+=r

    batch_status="SUCCESS" if success==len(files) else "FAILED"

    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)
    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.BATCH_RUN_LOG
    SET BATCH_STATUS=%s,
        STAGE_STATUS=%s,
        BATCH_END_TS=CURRENT_TIMESTAMP,
        IS_ACTIVE_BATCH='N'
    WHERE BATCH_ID=%s
    """,(batch_status,batch_status,batch_id))

    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    SET JOB_STATUS=%s,
        RECORDS_PROCESSED=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE JOB_RUN_ID=%s
    """,(batch_status,records,job_run_id))

    conn.commit()

except Exception as e:
    use_context(cursor, METADATA_DB, CONFIG_SCHEMA)
    cursor.execute(f"""
    UPDATE {METADATA_DB}.{CONFIG_SCHEMA}.JOB_RUN_LOG
    SET JOB_STATUS='FAILED',
        ERROR_MESSAGE=%s,
        END_TS=CURRENT_TIMESTAMP
    WHERE JOB_RUN_ID=%s
    """,(str(e),job_run_id))
    conn.commit()
    raise

finally:
    cursor.close()
    conn.close()
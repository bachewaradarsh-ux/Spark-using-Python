import sys
import json
import boto3
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from s3_logger import ProjectLogger

# -------------------------------------------------------
# Glue Arguments
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

    return conn

# -------------------------------------------------------
# Fetch Files To Process
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

    return cursor.fetchall()

# -------------------------------------------------------
# Load Stage Table (Thread Safe)
# -------------------------------------------------------

def load_stage_table(batch_id,file_name,stage_table):

    conn = get_snowflake_connection()
    cursor = conn.cursor()

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

        logger.log("INFO","Load completed",{
            "table":stage_table,
            "rows_loaded":rows
        })

        conn.commit()

        return True

    except Exception as e:

        logger.log("ERROR","Load failed",{
            "table":stage_table,
            "error":str(e)
        })

        return False

    finally:

        cursor.close()
        conn.close()

# -------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------

conn = get_snowflake_connection()
cursor = conn.cursor()

try:

    files = get_files_to_process(cursor)

    total_files=len(files)

    cursor.execute(f"""
    SELECT COALESCE(MAX(BATCH_ID),0)+1
    FROM {METADATA_DB}.{METADATA_SCHEMA}.BATCH_RUN_LOG
    """)

    batch_id=cursor.fetchone()[0]

    logger.log("INFO","Batch Started",{
        "batch_id":batch_id,
        "files":total_files
    })

    conn.commit()

    # -------------------------------------------------------
    # Parallel Execution
    # -------------------------------------------------------

    MAX_THREADS=10

    loaded_files=0

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futures=[]

        for file_name,s3_path,stage_table in files:

            futures.append(
                executor.submit(
                    load_stage_table,
                    batch_id,
                    file_name,
                    stage_table
                )
            )

        for future in as_completed(futures):

            result=future.result()

            if result:
                loaded_files+=1

    logger.log("INFO","All Loads Completed",{
        "loaded_files":loaded_files
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
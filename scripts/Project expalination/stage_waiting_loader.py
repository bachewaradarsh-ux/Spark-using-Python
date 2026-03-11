import sys
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import snowflake.connector
import uuid

# ==========================
# PARAMETERS
# ==========================

args = getResolvedOptions(sys.argv, [
    'BATCH_ID',
    'SNOWFLAKE_USER',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_WAREHOUSE',
    'SNOWFLAKE_DATABASE'
])

BATCH_ID = args['BATCH_ID']

sfOptions = {
    "sfURL": args['SNOWFLAKE_ACCOUNT'],
    "sfUser": args['SNOWFLAKE_USER'],
    "sfPassword": args['SNOWFLAKE_PASSWORD'],
    "sfDatabase": args['SNOWFLAKE_DATABASE'],
    "sfWarehouse": args['SNOWFLAKE_WAREHOUSE'],
    "sfSchema": "PUBLIC"
}

STAGE_SCHEMA = "STAGE"
BAD_SCHEMA = "BAD"
WAITING_SCHEMA = "WAITING"

MAX_PARALLEL_TABLES = 6  # Tune as needed

# ==========================
# SPARK INIT
# ==========================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ==========================
# GET TABLE CONFIGURATION
# ==========================

def get_table_config():
    """
    Fetch stage, bad, waiting table mappings
    along with primary keys and merge condition.
    """

    conn = snowflake.connector.connect(
        user=args['SNOWFLAKE_USER'],
        password=args['SNOWFLAKE_PASSWORD'],
        account=args['SNOWFLAKE_ACCOUNT'],
        warehouse=args['SNOWFLAKE_WAREHOUSE'],
        database=args['SNOWFLAKE_DATABASE'],
        schema="CONFIG"
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT STAGE_TABLE,
               BAD_TABLE,
               WAITING_TABLE,
               PRIMARY_KEYS
        FROM TABLE_MAPPING_CONFIG
        WHERE ACTIVE_FLAG = 'Y'
    """)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return results


# ==========================
# PROCESS SINGLE TABLE
# ==========================

def process_table(config):

    stage_table, bad_table, waiting_table, primary_keys = config
    print(f"\n--- Processing {stage_table} ---")

    try:
        # -----------------------------------
        # 1️⃣ Read Stage Data (Pushdown Filter)
        # -----------------------------------

        query = f"""
            SELECT *
            FROM {STAGE_SCHEMA}.{stage_table}
            WHERE BATCH_ID = '{BATCH_ID}'
        """

        df_stage = spark.read \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("query", query) \
            .load()

        if df_stage.rdd.isEmpty():
            print(f"No data found for {stage_table}")
            return f"{stage_table} - No Data"

        # -----------------------------------
        # 2️⃣ Split BAD / GOOD
        # -----------------------------------

        df_bad = df_stage.filter(col("ISBAD") == "Y")
        df_good = df_stage.filter(col("ISBAD") == "N")

        # -----------------------------------
        # 3️⃣ Write BAD (Append)
        # -----------------------------------

        if not df_bad.rdd.isEmpty():
            df_bad.write \
                .format("snowflake") \
                .options(**sfOptions) \
                .option("dbtable", f"{BAD_SCHEMA}.{bad_table}") \
                .mode("append") \
                .save()

        # -----------------------------------
        # 4️⃣ Write GOOD to Temp Table
        # -----------------------------------

        temp_table = f"TEMP_{waiting_table}_{uuid.uuid4().hex[:8]}"

        df_good.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("dbtable", temp_table) \
            .mode("overwrite") \
            .save()

        # -----------------------------------
        # 5️⃣ Execute MERGE
        # -----------------------------------

        pk_list = [pk.strip() for pk in primary_keys.split(",")]
        merge_condition = " AND ".join(
            [f"t.{pk} = s.{pk}" for pk in pk_list]
        )

        update_clause = ", ".join(
            [f"t.{c} = s.{c}" for c in df_good.columns]
        )

        insert_columns = ", ".join(df_good.columns)
        insert_values = ", ".join([f"s.{c}" for c in df_good.columns])

        merge_sql = f"""
            MERGE INTO {WAITING_SCHEMA}.{waiting_table} t
            USING {temp_table} s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        conn = snowflake.connector.connect(
            user=args['SNOWFLAKE_USER'],
            password=args['SNOWFLAKE_PASSWORD'],
            account=args['SNOWFLAKE_ACCOUNT'],
            warehouse=args['SNOWFLAKE_WAREHOUSE'],
            database=args['SNOWFLAKE_DATABASE']
        )

        cursor = conn.cursor()
        cursor.execute(merge_sql)

        # Drop temp table
        cursor.execute(f"DROP TABLE {temp_table}")

        cursor.close()
        conn.close()

        print(f"{stage_table} completed successfully.")
        return f"{stage_table} - Success"

    except Exception as e:
        print(f"Error processing {stage_table}: {str(e)}")
        return f"{stage_table} - Failed"


# ==========================
# MAIN EXECUTION
# ==========================

def main():

    table_configs = get_table_config()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_TABLES) as executor:
        future_to_table = {
            executor.submit(process_table, config): config[0]
            for config in table_configs
        }

        for future in as_completed(future_to_table):
            result = future.result()
            results.append(result)

    print("\n==== FINAL SUMMARY ====")
    for r in results:
        print(r)


if __name__ == "__main__":
    main()

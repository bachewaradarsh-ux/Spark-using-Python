import uuid
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_PARALLEL_CORE = 4

# ----------------------------------
# Read SQL From S3
# ----------------------------------

def read_sql_from_s3(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


# ----------------------------------
# Process Single CORE Table
# ----------------------------------

def process_core_table(config):

    core_table, sql_path, merge_condition, load_type = config

    print(f"Processing {core_table}")

    bucket = sql_path.split("/")[2]
    key = "/".join(sql_path.split("/")[3:])

    sql_query = read_sql_from_s3(bucket, key)

    # Execute SQL in Spark
    core_df = spark.sql(sql_query)

    core_df = core_df.repartition(200)

    if load_type == "OVERWRITE":

        core_df.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("dbtable", f"CORE.{core_table}") \
            .mode("overwrite") \
            .save()

    else:  # MERGE

        temp_table = f"TEMP_{core_table}_{uuid.uuid4().hex[:6]}"

        core_df.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("dbtable", temp_table) \
            .mode("overwrite") \
            .save()

        merge_sql = f"""
        MERGE INTO CORE.{core_table} t
        USING {temp_table} s
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        cursor.execute(merge_sql)
        cursor.execute(f"DROP TABLE {temp_table}")

    print(f"{core_table} completed.")


# ----------------------------------
# Main
# ----------------------------------

def main():

    configs = get_core_table_config()  # fetch metadata table

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CORE) as executor:
        futures = [executor.submit(process_core_table, c) for c in configs]

        for future in as_completed(futures):
            print(future.result())


if __name__ == "__main__":
    main()

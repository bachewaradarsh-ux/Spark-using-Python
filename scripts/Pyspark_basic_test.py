from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("AirBnb Processing").getOrCreate()

# Read sample data from S3 (replace with your bucket later)
df_listings = spark.read.format('csv')\
          .option('header', True)\
          .option('inferSchema', True)\
          .load("s3://project-data-adarshpractice/AirBnb_Data/Listings.csv")

df_superhost = df_listings.filter(col("host_is_superhost") == 't')\
                          .groupBy(col("city"))\
                          .agg(count(col("listing_id")).alias("host_count"))

df_result = df_listings.join(broadcast(df_superhost),"city", 'left')\
                       .groupBy(col("city"))\
                       .agg(count("listing_id").alias("total_count"),max("host_count").alias("superhost_count"))\
                       .fillna("No Super Host", subset = ['city'])\
                       .select(col("city"),(col("superhost_count") / col("total_count")) * 100)\

df_result.write.format('parquet')\
               .mode("overwrite")\
               .save("s3://project-data-adarshpractice/AirBnb_Data/processed_data")
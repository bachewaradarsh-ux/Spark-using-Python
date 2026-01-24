from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# Read sample data from S3 (replace with your bucket later)
df_listings = spark.read.format('csv')\
          .option('header', True)\
          .option('inferSchema', True)\
          .load("s3://project-data-adarshpractice/AirBnb_Data/Listings.csv")

# Show first few rows
df_listings.show()
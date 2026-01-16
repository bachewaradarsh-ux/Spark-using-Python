from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# Read sample data from S3 (replace with your bucket later)
df = spark.read.option("header", "true").csv("s3://my-bucket/raw/sample.csv")

# Show first few rows
df.show()

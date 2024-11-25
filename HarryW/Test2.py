from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LastFM Analysis") \
    .getOrCreate()


print("Hello")
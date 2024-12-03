from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc

spark = SparkSession.builder.appName("TopDriversByRating").getOrCreate()

input_file = str(Path("data", "trips_data.csv").absolute())
output_file = str(Path("data", "top_100_drivers") .absolute())

print("Reading input CSV file...")
df = spark.read.csv(input_file, header=True, inferSchema=True)

df = df.withColumn("driver_rating", col("driver_rating").cast("float"))

print("Calculating average driver ratings...")
driver_ratings = (
    df.groupBy("driver")
    .agg(avg("driver_rating").alias("average_rating"))
    .orderBy(desc("average_rating"))
)

print("Selecting top 100 drivers by rating...")
top_100_drivers = driver_ratings.limit(100)

print("Writing results to output CSV file...")
top_100_drivers.write.csv(output_file, header=True, mode="overwrite")

print(f"Top 100 drivers by rating saved to: {output_file}")

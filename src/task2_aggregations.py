from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, sum, avg, to_timestamp

# Build Spark Session
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingAggregation") \
    .getOrCreate()

# Read Stream from Socket
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Define Schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", IntegerType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

json_data = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

def aggregate(df: DataFrame) -> DataFrame:  
    return df.withWatermark("Timestamp", "1 minutes").groupBy("driver_id", "Timestamp").agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

aggregated = aggregate(json_data)

query = (
    aggregated
    .writeStream
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .format("csv")
    .option("path", "/opt/bitnami/spark/Rideshare/output")
    .option("header", True)
    .option("checkpointLocation", "/opt/bitnami/spark/Rideshare/checkpoint")
    .start()
)

query.awaitTermination()

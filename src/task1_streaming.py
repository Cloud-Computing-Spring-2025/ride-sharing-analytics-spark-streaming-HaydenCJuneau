from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import explode, split, from_json, col

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", IntegerType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

json_data = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")

json_data.writeStream.outputMode("append").format("console").start().awaitTermination()

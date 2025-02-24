from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "iot")
  .option("includeHeaders", "true")
  .load()
)

event_schema = StructType([
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

query = (
  df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "timestamp", "headers")
  .withColumn("event", F.from_json(F.col("value"), schema=event_schema))
  # .withWatermark("timestamp", "10 minutes")
  .groupBy(
      F.window(F.col("timestamp"), "10 seconds", "10 seconds")
  )
  .agg(F.avg("event.temperature").alias("temperature"), F.avg("event.humidity").alias("humidity"))
  .writeStream
  .outputMode("complete") #append, complete
  .format("console")
  .start()
)

query.awaitTermination()

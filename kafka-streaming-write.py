from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "topic1")
  .option("includeHeaders", "true")
  .load()
)

query = (
  df
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("topic", "topic2")
  .option("checkpointLocation", ".checkpoint")
  .start()
)

query.awaitTermination()

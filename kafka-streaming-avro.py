from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from avro.schema import Parse
from avro.io import DatumReader, BinaryDecoder
import io

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

df = (
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "avro")
  .option("includeHeaders", "true")
  .load()
)

schema_str = """
{
  "type": "record",
  "name": "SensorData",
  "fields": [
    {"name": "temperature", "type": "int"},
    {"name": "humidity", "type": "int"}
  ]
}
"""
schema = Parse(schema_str)

def avro_deserializer(avro_bytes):
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    return reader.read(decoder)

@F.udf(returnType=StructType([
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
]))
def deserialize_avro(value):
    return avro_deserializer(value)

query = (
  df
  .selectExpr("CAST(key AS STRING)", "value", "partition", "offset", "timestamp", "headers")
  .withColumn("event", deserialize_avro(F.col("value")))
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
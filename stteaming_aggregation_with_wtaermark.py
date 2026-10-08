from pyspark.sql import SparkSession
from pyspark.sql.functions import col,window
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DoubleType
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Q9")
    .getOrCreate()
)

schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("temperature", DoubleType(), True)
])

input_path = "C:/demand/events"

df = (
    spark.readStream
    .schema(schema)
    .json(input_path)
)

# 15-minute watermark
result_df = (
    df
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        window(
            col("event_time"),
            "10 minutes",   # window duration
            "5 minutes"     # sliding interval
        ),
        col("device_id")
    )
    .count()
    .select(
        col("device_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("count").alias("event_count")
    )
)

query = (
    result_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "C:/demand/checkpoint_q9")
    .start()
)

query.awaitTermination()
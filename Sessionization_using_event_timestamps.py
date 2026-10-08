# Problem: Group events into sessions per user. A new session begins when the gap between consecutive events is greater than 30 minutes. Return each session's start time, end time, and event count.
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_diff,col,lag,timestamp_diff,lit,coalesce,when,sum,min,max,count
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("Q7").getOrCreate()

data = [
    (1, "2026-01-01 10:00:00"),
    (1, "2026-01-01 10:10:00"),
    (1, "2026-01-01 10:40:00"),
    (1, "2026-01-01 11:20:00"),
    (2, "2026-01-01 09:00:00"),
    (2, "2026-01-01 09:20:00"),
    (2, "2026-01-01 10:00:00")
]

df = spark.createDataFrame(data, ["user_id", "event_time"]).withColumn(
    "event_time", to_timestamp("event_time")
)

window_spec=Window.partitionBy("user_id").orderBy(col("event_time"))
prev_df=df.withColumn("prev_event_time",lag(col("event_time"),1).over(window_spec))

result_df=prev_df.withColumn(
    "new_session",
    when(((
        timestamp_diff("MINUTE",col("prev_event_time"),col("event_time"))
        ) > 30) | (col("prev_event_time").isNull()),1).otherwise(0))
# result_df.show()

window_spec1=Window.partitionBy("user_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding,Window.currentRow)


final_df=result_df.withColumn("session_id",sum(col("new_session")).over(window_spec1))
# final_df.show()

grouped_df=final_df.groupBy("user_id","session_id").agg(
    min(col("event_time")).alias("start_time"),
    max(col("event_time")).alias("end_time"),
    count("*").alias("event_count")
)

grouped_df.show()
# filtered_df=result_df.filter((col("date_diff")==0)|(col("date_diff")>=30))
# filtered_df.show()
# Write your solution here
# Problem: Identify users who logged in on at least three consecutive calendar days. Return each qualifying user only once.
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,lag,date_diff
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Q4").getOrCreate()

data = [
    (1, "2026-01-01"),
    (1, "2026-01-02"),
    (1, "2026-01-03"),
    (1, "2026-01-05"),
    (2, "2026-01-01"),
    (2, "2026-01-03"),
    (2, "2026-01-04"),
    (2, "2026-01-05"),
    (3, "2026-01-01"),
    (3, "2026-01-02")
]

df = spark.createDataFrame(data, ["user_id", "login_date"]).withColumn(
    "login_date", to_date("login_date")
)
window_spec=Window.partitionBy("user_id").orderBy( "login_date")
result_df=df.withColumn("active",date_diff("login_date",lag("login_date",2).over(window_spec))).filter(col("active")==2).select("user_id").distinct()

result_df.show()

# Write your solution here
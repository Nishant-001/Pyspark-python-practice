# Q3. Detect gaps in customer activity

# You have daily customer activity.

# Find customers who had a gap of more than 3 days between two consecutive activity dates.
# Return:

# customer_id
# previous_activity_date
# current_activity_date
# gap_days

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,lag,datediff
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q3") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "2026-01-01"),
    (1, "2026-01-02"),
    (1, "2026-01-10"),
    (1, "2026-01-11"),

    (2, "2026-01-01"),
    (2, "2026-01-05"),
    (2, "2026-01-06"),

    (3, "2026-01-01"),
    (3, "2026-01-02"),
    (3, "2026-01-03")
]

df = spark.createDataFrame(
    data,
    ["customer_id", "activity_date"]
).withColumn(
    "activity_date",
    to_date("activity_date")
)
window_spec=Window.partitionBy("customer_id").orderBy("activity_date")

ordered_df = df.withColumn(
    "previous_activity_date",
    lag("activity_date").over(window_spec)
).withColumn(
    "current_activity_date",
    col("activity_date")
).withColumn(
    "gap",
    datediff(
        col("current_activity_date"),
        col("previous_activity_date")
    )
)

result_df=ordered_df.filter(col("gap")>3).drop("activity_date")
df.show()
ordered_df.show()
result_df.show()

input("press_enter_to_exit")
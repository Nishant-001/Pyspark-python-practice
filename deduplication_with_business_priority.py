# Q1. Deduplication with business priority

# You receive employee records multiple times.

# Requirement:

# Remove duplicates based on emp_id.
# Keep the record with the highest salary.
# If salary is equal, keep the record with the latest updated_at.
# If both are equal, keep the record with the highest record_id.
# Ignore records where emp_id is null.

# Driver code:
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,col,first_value,row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q1") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "A", 50000, "2026-01-01 10:00:00", 101),
    (1, "A", 60000, "2026-01-02 10:00:00", 102),
    (1, "A", 60000, "2026-01-03 10:00:00", 103),
    (2, "B", 70000, "2026-01-01 10:00:00", 201),
    (2, "B", 70000, "2026-01-01 10:00:00", 202),
    (3, "C", 80000, "2026-01-01 10:00:00", 301),
    (None, "X", 90000, "2026-01-01 10:00:00", 401)
]

df = spark.createDataFrame(
    data,
    ["emp_id", "name", "salary", "updated_at", "record_id"]
).withColumn(
    "updated_at",
    to_timestamp("updated_at")
)
# ordered_df=df.orderBy(col("salary").desc(),col("updated_at").desc(),col("record_id").desc()).groupBy("emp_id").agg(
#     first_value("name").alias("name"),
#     first_value("salary").alias("salary"),
#     first_value("updated_at").alias("updated_at"),
#     first_value("record_id").alias("record_id")
#     ).filter(col("emp_id").isNotNull())

window_spec=Window.partitionBy("emp_id").orderBy(
    col("salary").desc_nulls_last(),
    col("updated_at").desc_nulls_last(),
    col("record_id").desc_nulls_last()
)
ordered_df=(
    df.filter(col("emp_id").isNotNull())
    .withColumn("rn",row_number().over(window_spec))
    .filter(col("rn")==1)
    .drop(col("rn")))
df.show()
ordered_df.show()
# Q6. Incremental processing

# You have a target table containing the latest processed record for each customer.

# New data arrives every day.

# Requirement:

# For each customer_id:

# Keep the record with the highest event_id.
# Insert new customers.
# Update existing customers.
# Ignore older events.
# The final output should contain the latest state of every customer.

# Simulate the target table and today's incoming data.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q6") \
    .master("local[*]") \
    .getOrCreate()

target_data = [
    (1, "Nishant", "Delhi", 100),
    (2, "Rahul", "Mumbai", 200),
    (3, "Amit", "Pune", 300)
]

incoming_data = [
    (1, "Nishant", "Noida", 101),
    (1, "Nishant", "Delhi", 99),
    (2, "Rahul", "Bangalore", 201),
    (4, "Vikas", "Chennai", 400)
]

target = spark.createDataFrame(
    target_data,
    ["customer_id", "name", "city", "event_id"]
)

incoming = spark.createDataFrame(
    incoming_data,
    ["customer_id", "name", "city", "event_id"]
)
window_spec=Window.partitionBy("customer_id").orderBy(col("event_id").desc())
deduplicated_df=incoming.withColumn("rn",row_number().over(window_spec)).filter(col("rn")==1).drop("rn")

union_df=target.union(deduplicated_df)
final_window_spec=Window.partitionBy("customer_id").orderBy(col("event_id").desc())
final_df=union_df.withColumn("rn",row_number().over(final_window_spec)).filter(col("rn")==1).drop("rn")
final_df.show()

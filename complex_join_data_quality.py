# Q7. Complex join + data quality

# You have orders and customers.

# Find:

# Total order value by city.
# Number of unique customers by city.
# Number of orders with invalid customer IDs.  city number_customers total_order_value
# Total order value from valid customers only. delhi 2 1900  null 3 null

# Do this while ensuring that orders with null customer IDs are also counted as invalid.

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col,count,count_distinct,coalesce,lit,when

spark = SparkSession.builder \
    .appName("Q7") \
    .master("local[*]") \
    .getOrCreate()

customers_data = [
    (1, "A", "Delhi"),
    (2, "B", "Mumbai"),
    (3, "C", "Pune"),
    (4, "D", "Delhi")
]

orders_data = [
    (101, 1, 1000),
    (102, 1, 500),
    (103, 2, 700),
    (104, 99, 900),
    (105, None, 300),
    (106, 3, 800),
    (107, 99, 200),
    (108, 4, 400)
]

customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "name", "city"]
)

orders = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "amount"]
)

joined_df=orders.join(customers,"customer_id","left")
grouped_df=joined_df.groupBy("city").agg(
    count_distinct((when(col("city").isNotNull(),col("customer_id")))).alias("number_of_customers"),
    sum(col("amount")).alias("total_order_value"),
    count( when(col("city").isNull(), col("order_id"))).alias("invalid_orders"),
    sum(when(col("city").isNotNull(),col("amount"))).alias("valid_order_value")
)
grouped_df.show()
# orders.show()
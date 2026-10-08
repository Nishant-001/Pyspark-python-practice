# Problem: Join a large orders DataFrame to a small customers DataFrame and calculate revenue by customer region. Assume customer_id = 1 accounts for 60% of orders. Optimize the join to reduce skew and avoid unnecessary shuffles where possible.
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast,sum

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Q10")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# Simulated orders
orders = [
    (1, 1001, 500),
    (1, 1002, 700),
    (1, 1003, 200),
    (2, 1004, 300),
    (3, 1005, 900),
    (4, 1006, 400)
]

customers = [
    (1, "North"),
    (2, "South"),
    (3, "East"),
    (4, "West")
]

orders_df = spark.createDataFrame(
    orders, ["customer_id", "order_id", "amount"]
)

customers_df = spark.createDataFrame(
    customers, ["customer_id", "region"]
)
joined_df=orders_df.join(
    broadcast(customers_df),
    "customer_id",
    "inner"
).groupBy("region").agg(
    sum("amount").alias("total_revenue")
).orderBy("region")

joined_df.show()
input("Press ENTER to stop Spark and exit...")
# Write your solution here
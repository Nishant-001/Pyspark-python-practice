# Q2. Top 2 products per category

# Given product sales, find the top 2 products by total revenue within each category.

# Requirements:

# Calculate total revenue = quantity * price.
# Rank products within each category.
# If two products have the same revenue, they should receive the same rank.
# Return all products having rank <= 2.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,dense_rank,sum
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q2") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("Electronics", "P1", 10, 100),
    ("Electronics", "P1", 5, 100),
    ("Electronics", "P2", 20, 50),
    ("Electronics", "P3", 10, 100),
    ("Electronics", "P4", 2, 1000),
    ("Clothing", "P5", 10, 50),
    ("Clothing", "P6", 5, 100),
    ("Clothing", "P7", 20, 25),
    ("Clothing", "P8", 1, 1000)
]

df = spark.createDataFrame(
    data,
    ["category", "product_id", "quantity", "price"]
)
grouped_df=df.groupBy(
    "category",
    "product_id",
).agg(
    sum(col("quantity")*col("price")).alias("revenue")
)
window_spec=Window.partitionBy("category").orderBy((col("revenue")).desc())

resultdf=(
    grouped_df.withColumn("rn",dense_rank().over(window_spec))
    .filter(col("rn")<=2)
)

df.show()
resultdf.show()
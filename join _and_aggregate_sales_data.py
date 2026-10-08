# Join and aggregate sales data
# Medium

# Problem: Calculate total sales and total quantity for each product category. Include categories with zero sales, and handle products with no matching sales records.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,coalesce,lit

spark = SparkSession.builder.master("local[*]").appName("Q6").getOrCreate()

products = [
    (1, "Laptop", "Electronics"),
    (2, "Phone", "Electronics"),
    (3, "Chair", "Furniture"),
    (4, "Desk", "Furniture"),
    (5, "Milk", "Grocery")
]

sales = [
    (101, 1, 2, 50000),
    (102, 1, 1, 50000),
    (103, 2, 3, 20000),
    (104, 3, 4, 3000),
    (105, 2, 1, 20000)
]

products_df = spark.createDataFrame(
    products, ["product_id", "product_name", "category"]
)

sales_df = spark.createDataFrame(
    sales, ["sale_id", "product_id", "quantity", "unit_price"]
)

total_sales_df=sales_df.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity"),
    sum(col("quantity")*col("unit_price")).alias("total_sales")
)
# result_df=products_df.join(total_sales_df,on="product_id",how="left")
result_df = (
    products_df
    .join(total_sales_df, "product_id", "left")
    .groupBy("category")
    .agg(
        coalesce(sum("total_quantity"), lit(0)).alias("total_quantity"),
        coalesce(sum("total_sales"), lit(0)).alias("total_sales")
    )
)
result_df.show()
# Write your solution here
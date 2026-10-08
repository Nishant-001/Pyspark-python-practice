# Flatten nested JSON
# Problem: Parse the JSON string, explode the orders array, and produce one row per order containing customer_id, name, order_id, and amount. Handle null or empty order arrays.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,explode_outer,explode
from pyspark.sql.types import  StructType, StructField, StringType,IntegerType, ArrayType, DoubleType

spark = SparkSession.builder.master("local[*]").appName("Q5").getOrCreate()

data = [
    (1, '{"name":"Amit","orders":[{"order_id":101,"amount":500},{"order_id":102,"amount":700}]}'),
    (2, '{"name":"Neha","orders":[{"order_id":201,"amount":300}]}'),
    (3, '{"name":"Raj","orders":[]}'),
    (4, '{"name":"Sara","orders":null}')
]

df = spark.createDataFrame(data, ["customer_id", "json_data"])
schema=StructType([
    StructField("name",StringType(),True),
    
    StructField("orders",ArrayType(
        StructType([
            StructField("order_id",IntegerType(),True),
            StructField("amount",DoubleType(),True)
        ])
            ),True)
])
flattened_df=df.withColumn("data",from_json("json_data",schema))
result_df=flattened_df.select(
    col("customer_id").alias("customer_id"),
    col("data.name").alias("name"),
    col("data.orders").alias("orders")
)
result_df1=result_df.select(
    col("customer_id"),
    col("name"),
    explode_outer("orders").alias("orders")
).select(
    col("customer_id"),
    col("name"),
    col("orders.order_id").alias("order_id"),
    col("orders.amount").alias("amount")
)
# final_df=result_df.
result_df1.show()
# Write your solution here
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("NestedJSONParsing")
    .master("local[*]")
    .getOrCreate()
)

# 1. Define the nested schema
schema = StructType([
    StructField("event_id", IntegerType(), True),

    StructField("customer", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),

        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)
    ]), True),

    StructField("orders", ArrayType(
        StructType([
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
    ), True)
])

# 2. Sample JSON string
data = [
    ('''{
        "event_id": 101,
        "customer": {
            "id": 10,
            "name": "Nishant",
            "address": {
                "city": "Meerut",
                "country": "India"
            }
        },
        "orders": [
            {"product": "Laptop", "amount": 50000},
            {"product": "Mouse", "amount": 1000}
        ]
    }''',)
]

df = spark.createDataFrame(data, ["json_string"])

# 3. Parse JSON string into a structured column
parsed_df = df.withColumn(
    "data",
    from_json(col("json_string"), schema)
)
print (parsed_df)
# 4. Extract nested fields
result = parsed_df.select(
    col("data.event_id").alias("event_id"),
    col("data.customer.id").alias("customer_id"),
    col("data.customer.name").alias("customer_name"),
    col("data.customer.address.city").alias("city"),
    col("data.customer.address.country").alias("country"),
    col("data.orders").alias("orders")
)

result.show(truncate=False)

flattened_df = (
    parsed_df
    .select(
        col("data.event_id").alias("event_id"),
        col("data.customer.name").alias("customer_name"),
        explode(col("data.orders")).alias("order")
    )
    .select(
        "event_id",
        "customer_name",
        col("order.product").alias("product"),
        col("order.amount").alias("amount")
    )
)

flattened_df.show()
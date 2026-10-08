from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    explode_outer,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("FlattenJson")
    .master("local[*]")
    .getOrCreate()
)

# -----------------------------
# Schema
# -----------------------------

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

# -----------------------------
# Input data
# -----------------------------

data = [

    # Normal
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
            {"product": "Mouse", "amount": 1000},
            {"product": "Display", "amount": 60000},
            {"product": "Phone", "amount": 10000}
        ]
    }''',),

    # Empty orders
    ('''{
        "event_id": 102,
        "customer": {
            "id": 11,
            "name": "Rahul",
            "address": {
                "city": "Delhi",
                "country": "India"
            }
        },
        "orders": []
    }''',),

    # Null orders
    ('''{
        "event_id": 103,
        "customer": {
            "id": 12,
            "name": "Amit",
            "address": {
                "city": "Noida",
                "country": "India"
            }
        },
        "orders": null
    }''',),

    # Missing customer fields
    ('''{
        "event_id": 104,
        "customer": {
            "id": 13,
            "name": null,
            "address": null
        },
        "orders": [
            {"product": null, "amount": null}
        ]
    }''',),

    # Missing orders completely
    ('''{
        "event_id": 105,
        "customer": {
            "id": 14,
            "name": "Vikas"
        }
    }''',)
]

df = spark.createDataFrame(
    data,
    ["json_string"]
)

# -----------------------------
# Parse JSON
# -----------------------------

parsed_df = df.withColumn(
    "data",
    from_json(col("json_string"), schema)
)

# -----------------------------
# Extract fields
# -----------------------------

result = parsed_df.select(
    col("data.event_id").alias("event_id"),

    col("data.customer.id").alias("customer_id"),

    col("data.customer.name").alias("name"),

    col("data.customer.address.city").alias("city"),

    col("data.customer.address.country").alias("country"),

    col("data.orders").alias("orders")
)

result.show(truncate=False)

# -----------------------------
# Flatten orders
# -----------------------------

flattened_df = (
    result

    # Keep parent row even when orders is NULL or []
    .select(
        col("event_id"),
        col("customer_id"),
        col("name"),
        col("city"),
        col("country"),
        explode_outer("orders").alias("order")
    )

    .select(
        col("event_id"),
        col("customer_id"),
        col("name"),
        col("city"),
        col("country"),

        col("order.product").alias("product"),
        col("order.amount").alias("amount")
    )
)

flattened_df.show(truncate=False)
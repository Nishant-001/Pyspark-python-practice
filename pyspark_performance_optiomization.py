# Q8. PySpark performance problem

# You have a huge transactions DataFrame and a small customers DataFrame.

from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col,rand,lit,concat,broadcast,sum

spark = SparkSession.builder \
    .appName("Q8") \
    .master("local[*]") \
    .option("spark.sql.adaptive.enabled", True)\
    .getOrCreate()

transactions = spark.range(0, 10000000) \
    .withColumn(
        "customer_id",
        when(col("id") < 8000000, lit(1))
        .otherwise((col("id") % 100000).cast("long"))
    ) \
    .withColumn(
        "amount",
        (rand() * 1000)
    )

customers = spark.range(0, 100000) \
    .withColumnRenamed("id", "customer_id") \
    .withColumn(
        "region",
        concat(lit("Region_"), col("customer_id") % 10)
    )
result_df=transactions.join(broadcast(customers),"customer_id").groupBy("region").agg(
    sum("amount").alias("total_amount")
)
transactions.show(5)
customers.show(5)

input("press_enter_to_exit")
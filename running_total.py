from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,sum
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Q3").getOrCreate()

data = [
    (1, 101, "2026-01-01", 100),
    (1, 102, "2026-01-03", 200),
    (1, 103, "2026-01-03", 50),
    (2, 201, "2026-01-02", 300),
    (2, 202, "2026-01-05", 100),
    (1, 104, "2026-01-07", 150)
]

df = spark.createDataFrame(
    data, ["customer_id", "transaction_id", "transaction_date", "amount"]
).withColumn("transaction_date", to_date("transaction_date"))

window_spec=Window.partitionBy("customer_id").orderBy("transaction_date","transaction_id").rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow
)
result_df=df.withColumn("running_total",sum("amount").over(window_spec))

result_df.show()
# Write your solution here
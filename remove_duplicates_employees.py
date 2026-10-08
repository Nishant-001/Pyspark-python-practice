from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,row_number
from pyspark.sql.window import Window as window

spark = SparkSession.builder.master("local[*]").appName("Q1").getOrCreate()

data = [
    (1, "Amit", 50000, "2026-01-01"),
    (2, "Neha", 60000, "2026-01-02"),
    (1, "Amit", 55000, "2026-02-01"),
    (3, "Raj", 45000, "2026-01-10"),
    (None, "Unknown", 30000, "2026-01-15"),
    (2, "Neha", 65000, "2026-03-01")
]

df=spark.createDataFrame(
    data,
    ["emp_id","name","salary","updated_at"]
).withColumn("updated_at",to_date("updated_at"))

window_spec=window.partitionBy("emp_id").orderBy(col("updated_at").desc())

grouped_df= df.withColumn("rn",
    row_number().over(window_spec)
    ).filter(col("rn")==1).filter(col("emp_id").isNotNull()).drop("rn")

grouped_df.show()

# Write your solution here
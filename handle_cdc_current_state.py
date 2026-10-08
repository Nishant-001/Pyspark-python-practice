# Problem: Given change data capture records, build the latest state of each customer. Apply inserts and updates, remove records whose latest operation is a delete, and handle out-of-order events using
from pyspark.sql import SparkSession
from pyspark.sql.functions import max,col,row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("Q8").getOrCreate()

data = [
    (1, "Amit", "Agra", "I", 1),
    (2, "Neha", "Delhi", "I", 2),
    (1, "Amit", "Noida", "U", 3),
    (2, "Neha", "Delhi", "D", 4),
    (3, "Raj", "Mumbai", "I", 5),
    (1, "Amit", "Delhi", "U", 2),
    (3, "Raj", "Pune", "U", 6)
]

df = spark.createDataFrame(
    data, ["customer_id", "name", "city", "operation", "sequence_no"]
)
window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("sequence_no").desc())
)
filtered_df=df.withColumn("rn",
                          row_number().over(window_spec)).filter(col("rn")==1).drop("rn").filter(col("operation")!="D").drop("operation","sequence_no")

filtered_df.show()

# Write your solution here
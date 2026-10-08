from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank,col,when,max
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("Q2").getOrCreate()

data = [
    (1, "Amit", "IT", 80000),
    (2, "Neha", "IT", 90000),
    (3, "Raj", "IT", 90000),
    (4, "Simran", "IT", 70000),
    (5, "John", "HR", 60000),
    (6, "Sara", "HR", 50000),
    (7, "Tom", "HR", 60000),
    (8, "Maya", "Finance", 75000)
]

df = spark.createDataFrame(
    data, ["emp_id", "name", "department", "salary"]
)
window_spec=Window.partitionBy("department").orderBy(col("salary").desc())
grouped_df=df.withColumn("rn",dense_rank().over(window_spec))
resultdf=grouped_df.groupBy("department").agg(
    max(when(col("rn")==2,col("name"))).alias("name")
)

resultdf.show()
# Write your solution here
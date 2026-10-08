# Q5. Find overlap_projectping intervals

# Each employee can have multiple project assignments.

# Find employees whose project assignments overlap_project in time.

# For example:

# Project A: Jan 1 → Jan 10
# Project B: Jan 5 → Jan 15

# is an overlap_project.

# Return:

# emp_id
# project_1
# project_2
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,when,lag,lead
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("Q5") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "P1", "2026-01-01", "2026-01-10"),
    (1, "P2", "2026-01-05", "2026-01-15"),
    (1, "P3", "2026-01-20", "2026-01-30"),

    (2, "P4", "2026-02-01", "2026-02-10"),
    (2, "P5", "2026-02-11", "2026-02-20"),

    (3, "P6", "2026-03-01", "2026-03-15"),
    (3, "P7", "2026-03-10", "2026-03-12")
]

df = spark.createDataFrame(
    data,
    ["emp_id", "project", "start_date", "end_date"]
).withColumn(
    "start_date", to_date("start_date")
).withColumn(
    "end_date", to_date("end_date")
)
window_spec=Window.partitionBy("emp_id").orderBy("start_date")

# overlap_project_df=df.withColumn("overlap_project",(when(
#     ((lag("end_date",1).over(window_spec)) >= col("start_date"))
#     | 
#     ((lead("start_date",1).over(window_spec)) <= col("end_date"))
#     ,1)
#                                     .otherwise(0)))
# overlap_project_df=df.withColumn("overlap_project",(when(
#     ((lag("end_date",1).over(window_spec)) >= col("start_date"))
#     ,lag("project",1).over(window_spec))))
# result_df=overlap_project_df.filter(col("overlap_project").isNotNull()).select(
#     col("emp_id"),
#     col("overlap_project").alias("project_1"),
#     col("project").alias("project_2"))
df1 = df.alias("p1")
df2 = df.alias("p2")

result_df = df1.join(
    df2,
    (col("p1.emp_id") == col("p2.emp_id")) &
    (col("p1.project") < col("p2.project")) &
    (col("p1.start_date") <= col("p2.end_date")) &
    (col("p2.start_date") <= col("p1.end_date"))
).select(
    col("p1.emp_id").alias("emp_id"),
    col("p1.project").alias("project_1"),
    col("p2.project").alias("project_2")
)
df.show()
# overlap_project_df.show()
result_df.show()



        #                           P1  start_date--------------------------------- P1 end_date
        #                           P2 start_date-----------------------------------P2 end_date
        
        




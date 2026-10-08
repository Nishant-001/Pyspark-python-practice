# Q4. Slowly Changing Dimension Type 2

# You have customer changes arriving as snapshots.

# For every customer, generate an SCD Type-2 history.

# Rules:

# effective_from = when that version became active.
# effective_to = next version's effective_from.
# Current record should have effective_to = null.
# Add is_current = true/false.
# Ignore duplicate records having the same customer and effective date.

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,lead,coalesce,when,lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Q4") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Nishant", "Delhi", "2026-01-01"),
    (1, "Nishant", "Noida", "2026-02-01"),
    (1, "Nishant", "Noida", "2026-02-01"),
    (1, "Nishant", "Pune", "2026-04-01"),

    (2, "Rahul", "Mumbai", "2026-01-01"),
    (2, "Rahul", "Bangalore", "2026-03-01")
]

df = spark.createDataFrame(
    data,
    ["customer_id", "name", "city", "effective_from"]
).withColumn(
    "effective_from",
    to_date("effective_from")
)
clean_df=df.dropDuplicates()
window_spec=Window.partitionBy("customer_id").orderBy("effective_from")
result_df=clean_df.withColumn(
    "effective_to",
    (lead(col("effective_from")).over(window_spec))
    ).withColumn(
        "is_current",
        when(col("effective_to").isNotNull(),False)
        .otherwise(True)
    )

clean_df.show()
result_df.show()
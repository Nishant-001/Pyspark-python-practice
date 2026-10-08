import os
import sys

JAVA_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"

PYTHON = sys.executable

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PYSPARK_PYTHON"] = PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("CleanPySparkTest")

    .config("spark.pyspark.python", PYTHON)
    .config("spark.pyspark.driver.python", PYTHON)

    .config(
        "spark.python.worker.faulthandler.enabled",
        "true"
    )

    .config(
        "spark.sql.execution.pyspark.udf.faulthandler.enabled",
        "true"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print()
print("=" * 60)
print("PYSPARK TEST")
print("=" * 60)

print("Python:")
print(sys.executable)

print("Python version:")
print(sys.version)

print("Spark:")
print(spark.version)

print("Spark Python:")
print(spark.conf.get("spark.pyspark.python"))

print("Driver Python:")
print(spark.conf.get("spark.pyspark.driver.python"))

print()
print("TEST 1: JVM / Spark")
print("=" * 60)

spark.range(10).show()

print()
print("TEST 2: Python worker")
print("=" * 60)

result = (
    spark.range(10)
    .rdd
    .map(lambda x: x.id * 2)
    .collect()
)

print(result)

print()
print("TEST 3: Python UDF")
print("=" * 60)

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

def square(x):
    return x * x

square_udf = udf(square, LongType())

(
    spark.range(10)
    .withColumn("square", square_udf("id"))
    .show()
)

print()
print("=" * 60)
print("ALL PYSPARK TESTS PASSED")
print("=" * 60)

spark.stop()

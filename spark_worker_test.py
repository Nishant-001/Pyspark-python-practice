import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PythonWorkerTest")
    .master("local[2]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.python.worker.reuse", "false")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)

print("Python:", sys.executable)
print("Spark:", spark.version)

print("=== JVM TEST ===")
spark.range(10).show()

print("=== PYTHON WORKER TEST ===")
result = (
    spark.range(10)
    .rdd
    .map(lambda x: x.id * 2)
    .collect()
)

print("RESULT:", result)

spark.stop()

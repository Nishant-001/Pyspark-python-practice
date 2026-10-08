import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

spark = (
    SparkSession.builder
    .appName("PythonWorkerDebug")
    .master("local[2]")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .getOrCreate()
)

def square(x):
    return x * x

square_udf = udf(square, LongType())

df = spark.range(10)

df.withColumn(
    "square",
    square_udf("id")
).show()

spark.stop()
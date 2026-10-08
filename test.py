from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("JSONExample") \
    .master("local[*]") \
    .getOrCreate()

# JSON string
value = '{"id":101,"name":"Nishant","amount":500}'

# Create DataFrame
df = spark.createDataFrame(
    [(value,)],
    ["value"]
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Parse JSON string
df = df.withColumn(
    "json_data",
    from_json(col("value"), schema)
)

# Expand STRUCT into columns
df = df.select("json_data.*")

df.show()


#json={name:xyz,amount:9090
# name:xyz,amount:9090
# name:xyz,amount:9090
# name:xyz,amount:9090
#name:xyz,amount:9090}
dict={}

for item in json:
    dict.put(item["name"],dict.getOrDefault(item["name"],0)+item["amount"])
    
  2,3 - l-2 d -3  
    2 + 3
    
1,1,0
3,2,1
6,3,1
10,4,1

0 0
0 0

2,3
1,1 -2
1,2 -


- - - - -

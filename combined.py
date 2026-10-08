# consumer.py
import json
import os
import signal
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "high-volume-consumers")
MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "1000"))
FETCH_MIN_BYTES = int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1048576"))
FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "100"))
MAX_POLL_INTERVAL_MS = int(
    os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000")
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def process_batch(records):
    """
    Replace with bulk database writes, Spark ingestion, or another
    idempotent sink. Avoid one network/database call per record.
    """
    if not records:
        return

    # Example: parse records and process them in bulk.
    events = [record.value for record in records]

    # TODO: perform a durable, idempotent batch write here.
    logger.info(
        "Processed %d records from %d partitions",
        len(events),
        len({(r.topic, r.partition) for r in records}),
    )


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=f"consumer-{os.getpid()}",
        group_id=GROUP_ID,
        group_instance_id=os.getenv("KAFKA_GROUP_INSTANCE_ID"),
        enable_auto_commit=False,
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        isolation_level="read_committed",
        max_poll_records=MAX_POLL_RECORDS,
        fetch_min_bytes=FETCH_MIN_BYTES,
        fetch_max_wait_ms=FETCH_MAX_WAIT_MS,
        max_partition_fetch_bytes=4 * 1024 * 1024,
        request_timeout_ms=40000,
        session_timeout_ms=15000,
        heartbeat_interval_ms=5000,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        check_crcs=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )

    consumer.subscribe([TOPIC_NAME])

    try:
        while running:
            batches = consumer.poll(
                timeout_ms=1000,
                max_records=MAX_POLL_RECORDS,
            )

            for _, records in batches.items():
                if not records:
                    continue

                # Commit offsets only after the entire batch is durable.
                process_batch(records)

            if batches:
                consumer.commit()

    except Exception:
        logger.exception("Consumer failed")
        raise

    finally:
        consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
import pandas as pd
dataFrame = pd.DataFrame({'Name': [' RACHEL  ', ' MONICA  ', ' PHOEBE  ',
                                   '  ROSS    ', 'CHANDLER', ' JOEY    '],
                          
                          'Age': [30, 35, 37, 33, 34, 30],
                          
                          'Salary': [100000, 93000, 88000, 120000, 94000, 95000],
                          
                          'JOB': ['DESIGNER', 'CHEF', 'MASUS', 'PALENTOLOGY',
                                  'IT', 'ARTIST']})
# filter dataframe
print(dataFrame.loc[(dataFrame['Salary']>=100000) & (dataFrame['Age']< 40) & (dataFrame['JOB'].str.startswith('D')),
                    ['Name','JOB']])
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    explode_outer,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("FlattenJson")
    .master("local[*]")
    .getOrCreate()
)

# -----------------------------
# Schema
# -----------------------------

schema = StructType([
    StructField("event_id", IntegerType(), True),

    StructField("customer", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),

        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)

    ]), True),

    StructField("orders", ArrayType(
        StructType([
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
    ), True)
])

# -----------------------------
# Input data
# -----------------------------

data = [

    # Normal
    ('''{
        "event_id": 101,
        "customer": {
            "id": 10,
            "name": "Nishant",
            "address": {
                "city": "Meerut",
                "country": "India"
            }
        },
        "orders": [
            {"product": "Laptop", "amount": 50000},
            {"product": "Mouse", "amount": 1000},
            {"product": "Display", "amount": 60000},
            {"product": "Phone", "amount": 10000}
        ]
    }''',),

    # Empty orders
    ('''{
        "event_id": 102,
        "customer": {
            "id": 11,
            "name": "Rahul",
            "address": {
                "city": "Delhi",
                "country": "India"
            }
        },
        "orders": []
    }''',),

    # Null orders
    ('''{
        "event_id": 103,
        "customer": {
            "id": 12,
            "name": "Amit",
            "address": {
                "city": "Noida",
                "country": "India"
            }
        },
        "orders": null
    }''',),

    # Missing customer fields
    ('''{
        "event_id": 104,
        "customer": {
            "id": 13,
            "name": null,
            "address": null
        },
        "orders": [
            {"product": null, "amount": null}
        ]
    }''',),

    # Missing orders completely
    ('''{
        "event_id": 105,
        "customer": {
            "id": 14,
            "name": "Vikas"
        }
    }''',)
]

df = spark.createDataFrame(
    data,
    ["json_string"]
)

# -----------------------------
# Parse JSON
# -----------------------------

parsed_df = df.withColumn(
    "data",
    from_json(col("json_string"), schema)
)

# -----------------------------
# Extract fields
# -----------------------------

result = parsed_df.select(
    col("data.event_id").alias("event_id"),

    col("data.customer.id").alias("customer_id"),

    col("data.customer.name").alias("name"),

    col("data.customer.address.city").alias("city"),

    col("data.customer.address.country").alias("country"),

    col("data.orders").alias("orders")
)

result.show(truncate=False)

# -----------------------------
# Flatten orders
# -----------------------------

flattened_df = (
    result

    # Keep parent row even when orders is NULL or []
    .select(
        col("event_id"),
        col("customer_id"),
        col("name"),
        col("city"),
        col("country"),
        explode_outer("orders").alias("order")
    )

    .select(
        col("event_id"),
        col("customer_id"),
        col("name"),
        col("city"),
        col("country"),

        col("order.product").alias("product"),
        col("order.amount").alias("amount")
    )
)

flattened_df.show(truncate=False)
import json
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,explode
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, DoubleType
)
spark=SparkSession.builder.appName("FlattenJson").master("local[*]").getOrCreate()
file_path = r"C:\Users\chnis\Downloads\Vodafone\NEW Fields & Adaptation\VFRO\CMTS STRUCT\ro_casa_cmts_struct.json"

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

jq_filter = r'''
[({datetime: .metadata.jsonGenerationTime}) + (.data |{cmts: .hostname,  cmts_ip: .ipAddress,  tenant_id: .tenant,  model: .model,  vendor: .vendor} + ((.macDomains[] | {mac_interface_index: .interfaceIndex} +({alias_downstream: .downstreamServiceGroup.ofdms[0].alias}//{alias_downstream: null}) + ((.upstreamServiceGroups[] | {upstream_service_group_id: .upstreamServiceGroupId} + ((.ofdmas[] | {alias: .alias,bandwidth_mhz:	.bandwidthMhz,bitrate_mbps:	.bitrateMbps,description:	.description,  channel_id: .channelId,  end_frequency_mhz: .endFrequencyMhz,  interface_index: .interfaceIndex,  start_frequency_mhz: .startFrequencyMhz, cer_port_connector_id: .cerPortConnectorId} + ((.ofdmaProfiles[] | {modulation: .modulation, profile_id: .profileId})//({modulation: null, profile_id: null}))) //({alias: null,bandwidth_mhz:	null, bitrate_mbps: null,description:	null, channel_id: null, end_frequency_mhz: null, interface_index:	null, start_frequency_mhz:	null, cer_port_connector_id: null, modulation: null, profile_id:	null})))//({upstream_service_group_id: null,alias: null,bandwidth_mhz:	null, bitrate_mbps: null,description:	null, channel_id: null, end_frequency_mhz: null, interface_index:	null, start_frequency_mhz:	null, cer_port_connector_id: null, modulation: null, profile_id:	null}) ))//({mac_interface_index: null,alias_downstream: null,upstream_service_group_id: null,alias: null,bandwidth_mhz:	null, bitrate_mbps: null,description:	null, channel_id: null, end_frequency_mhz: null, interface_index:	null, start_frequency_mhz:	null, cer_port_connector_id: null, modulation: null, profile_id:	null})))]
'''

result = subprocess.run(
    ["jq", "-c", jq_filter],
    input=json.dumps(data),
    text=True,
    capture_output=True
)

# Check jq
if result.returncode != 0:
    print("jq error:")
    print(result.stderr)
    spark.stop()
    raise SystemExit

# jq -> NDJSON
json_rdd = spark.sparkContext.parallelize(
    result.stdout.splitlines()
)

# NDJSON -> Spark DataFrame
df = spark.read.json(json_rdd)

df.show(10, truncate=False)
df.printSchema()
# Flatten nested JSON
# Problem: Parse the JSON string, explode the orders array, and produce one row per order containing customer_id, name, order_id, and amount. Handle null or empty order arrays.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,explode_outer,explode
from pyspark.sql.types import  StructType, StructField, StringType,IntegerType, ArrayType, DoubleType

spark = SparkSession.builder.master("local[*]").appName("Q5").getOrCreate()

data = [
    (1, '{"name":"Amit","orders":[{"order_id":101,"amount":500},{"order_id":102,"amount":700}]}'),
    (2, '{"name":"Neha","orders":[{"order_id":201,"amount":300}]}'),
    (3, '{"name":"Raj","orders":[]}'),
    (4, '{"name":"Sara","orders":null}')
]

df = spark.createDataFrame(data, ["customer_id", "json_data"])
schema=StructType([
    StructField("name",StringType(),True),
    
    StructField("orders",ArrayType(
        StructType([
            StructField("order_id",IntegerType(),True),
            StructField("amount",DoubleType(),True)
        ])
            ),True)
])
flattened_df=df.withColumn("data",from_json("json_data",schema))
result_df=flattened_df.select(
    col("customer_id").alias("customer_id"),
    col("data.name").alias("name"),
    col("data.orders").alias("orders")
)
result_df1=result_df.select(
    col("customer_id"),
    col("name"),
    explode_outer("orders").alias("orders")
).select(
    col("customer_id"),
    col("name"),
    col("orders.order_id").alias("order_id"),
    col("orders.amount").alias("amount")
)
# final_df=result_df.
result_df1.show()
# Write your solution here
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
# Join and aggregate sales data
# Medium

# Problem: Calculate total sales and total quantity for each product category. Include categories with zero sales, and handle products with no matching sales records.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum

spark = SparkSession.builder.master("local[*]").appName("Q6").getOrCreate()

products = [
    (1, "Laptop", "Electronics"),
    (2, "Phone", "Electronics"),
    (3, "Chair", "Furniture"),
    (4, "Desk", "Furniture"),
    (5, "Milk", "Grocery")
]

sales = [
    (101, 1, 2, 50000),
    (102, 1, 1, 50000),
    (103, 2, 3, 20000),
    (104, 3, 4, 3000),
    (105, 2, 1, 20000)
]

products_df = spark.createDataFrame(
    products, ["product_id", "product_name", "category"]
)

sales_df = spark.createDataFrame(
    sales, ["sale_id", "product_id", "quantity", "unit_price"]
)

total_sales_df=sales_df.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity"),
    sum(col("quantity")*col("unit_price")).alias("total_sales")
)
result_df=products_df.join(total_sales_df,on="product_id",how="left")
result_df.show()
# Write your solution here
# Driver code — solve the logic inside count_funnel()

import pandas as pd

# Sample data from the question
data = [
    (1, "page_view",   "2023-01-01 10:00:00"),
    (2, "page_view",   "2023-01-01 10:05:00"),
    (3, "page_view",   "2023-01-01 10:10:00"),
    (4, "page_view",   "2023-01-01 10:15:00"),
    (5, "page_view",   "2023-01-01 10:20:00"),

    (1, "add_to_cart", "2023-01-01 10:30:00"),
    (2, "add_to_cart", "2023-01-01 10:35:00"),
    (3, "add_to_cart", "2023-01-01 10:40:00"),
    (4, "add_to_cart", "2023-01-01 10:45:00"),

    (1, "checkout",    "2023-01-01 11:00:00"),
    
    (1, "purchase",    "2023-01-01 11:30:00"),
    (2, "purchase",    "2023-01-01 11:35:00"),
]

user_events = pd.DataFrame(
    data,
    columns=["user_id", "event_name", "event_time"]
)

user_events["event_time"] = pd.to_datetime(user_events["event_time"])

# ============================================================
# YOUR SOLUTION
# ============================================================

def count_funnel(user_events):

    steps = ["page_view", "add_to_cart", "checkout", "purchase"]

    # Write your logic here
    #
    # Requirements:
    # 1. A user must complete every previous step.
    # 2. Steps must occur in chronological order.
    # 3. Count distinct users reaching each step.
    # 4. Calculate dropoff_rate from previous step.
    # 5. Return:
    #
    #    step | users_reached | dropoff_rate
    #
    # sorted in funnel order.
    df1=user_events.groupby("event_name")["user_id"].count()
    print(df1)
    pass


# ============================================================
# TEST
# ============================================================

result = count_funnel(user_events)

print(result)
import multiprocessing
import sys

def child():
    print("CHILD OK")
    print("Child Python:", sys.executable)

if __name__ == "__main__":
    print("Parent Python:", sys.executable)

    p = multiprocessing.Process(target=child)
    p.start()
    p.join()

    print("Child exit code:", p.exitcode)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("NestedJSONParsing")
    .master("local[*]")
    .getOrCreate()
)

# 1. Define the nested schema
schema = StructType([
    StructField("event_id", IntegerType(), True),

    StructField("customer", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),

        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)
    ]), True),

    StructField("orders", ArrayType(
        StructType([
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
    ), True)
])

# 2. Sample JSON string
data = [
    ('''{
        "event_id": 101,
        "customer": {
            "id": 10,
            "name": "Nishant",
            "address": {
                "city": "Meerut",
                "country": "India"
            }
        },
        "orders": [
            {"product": "Laptop", "amount": 50000},
            {"product": "Mouse", "amount": 1000}
        ]
    }''',)
]

df = spark.createDataFrame(data, ["json_string"])

# 3. Parse JSON string into a structured column
parsed_df = df.withColumn(
    "data",
    from_json(col("json_string"), schema)
)
print (parsed_df)
# 4. Extract nested fields
result = parsed_df.select(
    col("data.event_id").alias("event_id"),
    col("data.customer.id").alias("customer_id"),
    col("data.customer.name").alias("customer_name"),
    col("data.customer.address.city").alias("city"),
    col("data.customer.address.country").alias("country"),
    col("data.orders").alias("orders")
)

result.show(truncate=False)

flattened_df = (
    parsed_df
    .select(
        col("data.event_id").alias("event_id"),
        col("data.customer.name").alias("customer_name"),
        explode(col("data.orders")).alias("order")
    )
    .select(
        "event_id",
        "customer_name",
        col("order.product").alias("product"),
        col("order.amount").alias("amount")
    )
)

flattened_df.show()
# producer.py
import json
import os
import signal
import time
import logging
from concurrent.futures import Future
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
).split(",")

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "65536"))
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
BUFFER_MEMORY = int(os.getenv("KAFKA_BUFFER_MEMORY", str(128 * 1024 * 1024)))
MAX_IN_FLIGHT = int(os.getenv("KAFKA_MAX_IN_FLIGHT", "5"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

running = True

def stop(*_):
    global running
    running = False

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def on_success(metadata):
    pass


def on_error(exc):
    logger.error("Kafka delivery failed: %s", exc)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="high-throughput-producer",
        acks="all",
        enable_idempotence=True,
        compression_type="lz4",
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE,
        buffer_memory=BUFFER_MEMORY,
        max_in_flight_requests_per_connection=MAX_IN_FLIGHT,
        retries=2147483647,
        retry_backoff_ms=100,
        request_timeout_ms=30000,
        max_block_ms=60000,
        delivery_timeout_ms=120000,
        connections_max_idle_ms=540000,
        key_serializer=lambda key: (
            key.encode("utf-8") if isinstance(key, str) else key
        ),
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8"),
    )


def generate_events():
    """Replace with your database, API, file, or streaming source."""
    while running:
        yield {
            "event_id": os.urandom(16).hex(),
            "event_time": time.time_ns(),
            "event_type": "measurement",
            "value": 42,
        }


def main():
    producer = create_producer()
    pending = set()
    max_pending = 10000
    sent = 0

    try:
        for event in generate_events():
            if not running:
                break

            key = event["event_id"]

            while len(pending) >= max_pending:
                done = {f for f in pending if f.done()}
                for future in done:
                    pending.remove(future)
                    future.get(timeout=0)

                if len(pending) >= max_pending:
                    pending.pop().get(timeout=60)

            future: Future = producer.send(
                TOPIC_NAME,
                key=key,
                value=event,
            )
            future.add_callback(on_success)
            future.add_errback(on_error)
            pending.add(future)
            sent += 1

            if sent % 100000 == 0:
                logger.info("Submitted %d events", sent)

    except Exception:
        logger.exception("Producer failed")
        raise

    finally:
        producer.flush(timeout=120)
        producer.close(timeout=120)


if __name__ == "__main__":
    main()
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
# Problem: Group events into sessions per user. A new session begins when the gap between consecutive events is greater than 30 minutes. Return each session's start time, end time, and event count.
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_diff,col,lag,timestamp_diff,lit,coalesce,when,sum,min,max,count
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[*]").appName("Q7").getOrCreate()

data = [
    (1, "2026-01-01 10:00:00"),
    (1, "2026-01-01 10:10:00"),
    (1, "2026-01-01 10:40:00"),
    (1, "2026-01-01 11:20:00"),
    (2, "2026-01-01 09:00:00"),
    (2, "2026-01-01 09:20:00"),
    (2, "2026-01-01 10:00:00")
]

df = spark.createDataFrame(data, ["user_id", "event_time"]).withColumn(
    "event_time", to_timestamp("event_time")
)

window_spec=Window.partitionBy("user_id").orderBy(col("event_time"))
prev_df=df.withColumn("prev_event_time",lag(col("event_time"),1).over(window_spec))

result_df=prev_df.withColumn(
    "new_session",
    when(((
        timestamp_diff("MINUTE",col("prev_event_time"),col("event_time"))
        ) > 30) | (col("prev_event_time").isNull()),1).otherwise(0))
# result_df.show()

window_spec1=Window.partitionBy("user_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding,Window.currentRow)


final_df=result_df.withColumn("session_id",sum(col("new_session")).over(window_spec1))
# final_df.show()

grouped_df=final_df.groupBy("user_id","session_id").agg(
    min(col("event_time")).alias("start_time"),
    max(col("event_time")).alias("end_time"),
    count("*").alias("event_count")
)

grouped_df.show()
# filtered_df=result_df.filter((col("date_diff")==0)|(col("date_diff")>=30))
# filtered_df.show()
# Write your solution here
# Problem: Join a large orders DataFrame to a small customers DataFrame and calculate revenue by customer region. Assume customer_id = 1 accounts for 60% of orders. Optimize the join to reduce skew and avoid unnecessary shuffles where possible.
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast,sum

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Q10")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# Simulated orders
orders = [
    (1, 1001, 500),
    (1, 1002, 700),
    (1, 1003, 200),
    (2, 1004, 300),
    (3, 1005, 900),
    (4, 1006, 400)
]

customers = [
    (1, "North"),
    (2, "South"),
    (3, "East"),
    (4, "West")
]

orders_df = spark.createDataFrame(
    orders, ["customer_id", "order_id", "amount"]
)

customers_df = spark.createDataFrame(
    customers, ["customer_id", "region"]
)
joined_df=orders_df.join(
    broadcast(customers_df),
    "customer_id",
    "inner"
).groupBy("region").agg(
    sum("amount").alias("total_revenue")
).orderBy("region")

joined_df.show()
input("Press ENTER to stop Spark and exit...")
# Write your solution here
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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,window
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DoubleType
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Q9")
    .getOrCreate()
)

schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("temperature", DoubleType(), True)
])

input_path = "C:/demand/events"

df = (
    spark.readStream
    .schema(schema)
    .json(input_path)
)

# 15-minute watermark
result_df = (
    df
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        window(
            col("event_time"),
            "10 minutes",   # window duration
            "5 minutes"     # sliding interval
        ),
        col("device_id")
    )
    .count()
    .select(
        col("device_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("count").alias("event_count")
    )
)

query = (
    result_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "C:/demand/checkpoint_q9")
    .start()
)

query.awaitTermination()
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
# create_topic.py
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "high-volume-events")

PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "24"))
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "3"))

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "compression.type": "producer",
    "retention.ms": str(7 * 24 * 60 * 60 * 1000),
    "segment.bytes": str(1024 * 1024 * 1024),
    "min.insync.replicas": "2",
}

def create_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        client_id="topic-admin",
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )

    try:
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs=TOPIC_CONFIG,
        )

        admin.create_topics(
            new_topics=[topic],
            validate_only=False,
            timeout_ms=30000,
        )
        print(f"Created topic: {TOPIC_NAME}")

    except TopicAlreadyExistsError:
        print(f"Topic already exists: {TOPIC_NAME}")

    finally:
        admin.close()

if __name__ == "__main__":
    create_topic()
# Problem: Identify users who logged in on at least three consecutive calendar days. Return each qualifying user only once.
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col,lag,date_diff
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("Q4").getOrCreate()

data = [
    (1, "2026-01-01"),
    (1, "2026-01-02"),
    (1, "2026-01-03"),
    (1, "2026-01-05"),
    (2, "2026-01-01"),
    (2, "2026-01-03"),
    (2, "2026-01-04"),
    (2, "2026-01-05"),
    (3, "2026-01-01"),
    (3, "2026-01-02")
]

df = spark.createDataFrame(data, ["user_id", "login_date"]).withColumn(
    "login_date", to_date("login_date")
)
window_spec=Window.partitionBy("user_id").orderBy( "login_date")
result_df=df.withColumn("active",date_diff("login_date",lag("login_date",2).over(window_spec))).filter(col("active")==2).select("user_id").distinct()

result_df.show()

# Write your solution here

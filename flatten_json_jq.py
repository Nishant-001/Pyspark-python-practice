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
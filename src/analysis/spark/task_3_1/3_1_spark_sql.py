from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-c", dest="cluster", action="store_true")
parser.add_argument("-s", dest="size", type=str)
args = parser.parse_args()

# CONFIGURAZIONE
BUCKET = "big-data-2026-project"

if args.size:
    SIZE = args.size
else:
    SIZE = "100"

if args.cluster:
    BASE = f"s3a://{BUCKET}"
    print(BASE)
else:
    BASE = "hdfs:///big_data"

INPUT_PATH  = f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
OUTPUT_PATH = f"{BASE}/results/task_3_1/spark_sql/task_3_1_spark_sql_{SIZE}"

os.makedirs("/tmp/spark-events", exist_ok=True)
spark = SparkSession.builder.appName(f"3_1_spark_sql_{SIZE}").config("spark.eventLog.enabled", "true").getOrCreate()

start_time = time.perf_counter()

df = spark.read.parquet(INPUT_PATH)

# ROUTE
df = df.withColumn("route", F.concat_ws("-", "origin", "dest"))

# ROUTE-LEVEL STATS
result = df.groupBy("op_unique_carrier", "route").agg(
    F.count("*").alias("num_flights"),
    F.min("arr_delay").alias("min_arr_delay"),
    F.max("arr_delay").alias("max_arr_delay"),
    F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
    F.round(F.sum("cancelled") / F.count("*"), 4).alias("cancel_rate"),
    F.sort_array(F.collect_set("month")).alias("months")
)

result = result.orderBy("op_unique_carrier", "route")

# OUTPUT
result.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"\nDataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f} seconds")

spark.stop()
from pyspark.sql import SparkSession
from collections import defaultdict
import time
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-c", dest="cluster", action="store_true")
parser.add_argument("-s", dest="size", type=str)
args = parser.parse_args()
# =============================================================
# CONFIGURAZIONE
# =============================================================
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

os.makedirs("/tmp/spark-events", exist_ok=True)
spark = SparkSession.builder.appName(f"3_3_spark_core_{SIZE}").config("spark.eventLog.enabled", "true").getOrCreate()

start_time = time.perf_counter()

INPUT_PATH = f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
OUTPUT_PATH = f"{BASE}/results/task_3_3/spark_core/task_3_3_spark_core_{SIZE}"

df = spark.read.parquet(INPUT_PATH)
rdd = df.rdd

# =========================================================
# UTILS
# =========================================================
def is_valid(x):
    return x is not None

# =========================================================
# (1) AIRLINE-AIRPORT STATS
# =========================================================
def map_to_key(row):
    dep = row["dep_delay"]
    arr = row["arr_delay"]

    return (
        (row["origin"], row["op_unique_carrier"]),
        (
            1,
            dep if dep is not None else 0.0,
            arr if arr is not None else 0.0,
            1 if dep is not None else 0,
            1 if arr is not None else 0,
            float(row["cancelled"]) if row["cancelled"] is not None else 0.0
        )
    )

def reduce_func(a, b):
    return (
        a[0] + b[0],
        a[1] + b[1],
        a[2] + b[2],
        a[3] + b[3],
        a[4] + b[4],
        a[5] + b[5]
    )

def finalize(record):
    (origin, airline), (count, dep_sum, arr_sum, dep_cnt, arr_cnt, cancel_sum) = record

    avg_dep = dep_sum / dep_cnt if dep_cnt > 0 else 0.0
    avg_arr = arr_sum / arr_cnt if arr_cnt > 0 else 0.0
    cancel_rate = cancel_sum / count if count > 0 else 0.0

    return (origin, airline, count, avg_dep, avg_arr, cancel_rate)

stats = rdd.map(map_to_key).reduceByKey(reduce_func).map(finalize)

# =========================================================
# (2) AIRPORT BASELINE — IDENTICO A SPARK SQL
# =========================================================
airport_avg = (
    rdd.map(lambda r: (
        r["origin"],
        (r["dep_delay"], 1)
    ))
    .filter(lambda x: x[1][0] is not None)
    .mapValues(lambda x: (x[0], x[1]))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda x: x[0] / x[1] if x[1] > 0 else 0.0)
    .collectAsMap()
)

# =========================================================
# (3) GROUP BY AIRPORT
# =========================================================
grouped = defaultdict(list)

for origin, airline, cnt, avg_dep, avg_arr, cancel_rate in stats.collect():
    grouped[origin].append({
        "airline": airline,
        "num_flights": cnt,
        "avg_dep": avg_dep,
        "avg_arr": avg_arr,
        "cancel_rate": cancel_rate
    })

# =========================================================
# (4) RANKING
# =========================================================
result = []

for origin in sorted(grouped.keys()):

    base = airport_avg.get(origin, 0.0)

    airlines_sorted = sorted(
        grouped[origin],
        key=lambda x: x["avg_dep"]
    )

    for rank, a in enumerate(airlines_sorted, start=1):

        result.append((
            origin,
            a["airline"],
            a["num_flights"],
            round(a["avg_dep"], 2),
            round(a["avg_arr"], 2),
            round(a["cancel_rate"], 4),
            round(a["avg_dep"] - base, 2),
            rank
        ))

# =========================================================
# OUTPUT
# =========================================================
columns = [
    "origin",
    "op_unique_carrier",
    "num_flights",
    "avg_dep_delay",
    "avg_arr_delay",
    "cancel_rate",
    "dep_delay_diff",
    "rank"
]

out_df = spark.createDataFrame(result, columns)

out_df = out_df.orderBy("origin", "rank")

out_df.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"\nDataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f} seconds")

spark.stop()
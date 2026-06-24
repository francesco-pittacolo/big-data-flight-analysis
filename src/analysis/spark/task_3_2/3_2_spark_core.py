from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, ArrayType,
    LongType
)
import time
import argparse
import os


parser = argparse.ArgumentParser()
parser.add_argument("-c", dest="cluster", action="store_true")
parser.add_argument("-s", dest="size", type=str)
args = parser.parse_args()


BUCKET = "big-data-2026-project"
SIZE = args.size if args.size else "100"

BASE = f"s3a://{BUCKET}" if args.cluster else "hdfs:///big_data"

os.makedirs("/tmp/spark-events", exist_ok=True)

spark = (
    SparkSession.builder
    .appName(f"3_2_spark_core_{SIZE}")
    .config("spark.eventLog.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

start_time = time.perf_counter()


# LOAD DATASET
df = spark.read.parquet(
    f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
)

rdd = df.rdd


CAUSE_COLS = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
]

CANCEL_MAP = {
    "A": "carrier",
    "B": "weather",
    "C": "nas",
    "D": "security"
}


def get_band(delay):
    if delay < 15:
        return "low"
    elif delay <= 60:
        return "medium"
    return "high"


# MAP
def mapper(row):

    origin = row["origin"]
    month = row["month"]

    if row["cancelled"] == 1:

        cause = CANCEL_MAP.get(row["cancellation_code"])

        return [(
            (origin, month, "cancelled"),
            (
                0, 0,
                0, 0,
                1,
                {cause: 1} if cause else {}
            )
        )]


    dep = row["dep_delay"]

    if dep is None or dep <= 0:
        return []


    causes = {}

    for c in CAUSE_COLS:
        if row[c] is not None and row[c] > 0:
            causes[c] = 1


    arr = row["arr_delay"]

    return [(
        (origin, month, get_band(dep)),
        (
            dep,
            1,
            arr if arr is not None else 0,
            1 if arr is not None else 0,
            1,
            causes
        )
    )]


# REDUCE
def reduce_func(a, b):

    causes = dict(a[5])

    for k, v in b[5].items():
        causes[k] = causes.get(k, 0) + v

    return (
        a[0] + b[0],   # dep sum
        a[1] + b[1],   # dep count
        a[2] + b[2],   # arr sum
        a[3] + b[3],   # arr count
        a[4] + b[4],   # flights
        causes
    )


# FINAL RESULT
def finalize(x):

    (origin, month, band), values = x

    dep_sum, dep_count, arr_sum, arr_count, flights, causes = values

    avg_dep = dep_sum / dep_count if dep_count else None
    avg_arr = arr_sum / arr_count if arr_count else None

    top_causes = sorted(
        causes.items(),
        key=lambda x: (-x[1], x[0])
    )[:3]

    return (
        origin,
        month,
        band,
        flights,
        round(avg_dep, 2) if avg_dep else None,
        round(avg_arr, 2) if avg_arr else None,
        top_causes
    )


result_rdd = (
    rdd
    .flatMap(mapper)
    .reduceByKey(reduce_func)
    .map(finalize)
)


schema = StructType([
    StructField("origin", StringType()),
    StructField("month", IntegerType()),
    StructField("band", StringType()),
    StructField("num_flights", LongType()),
    StructField("avg_dep_delay", DoubleType()),
    StructField("avg_arr_delay", DoubleType()),
    StructField(
        "top_causes",
        ArrayType(
            StructType([
                StructField("cause", StringType()),
                StructField("cause_count", LongType())
            ])
        )
    )
])


out_df = spark.createDataFrame(
    result_rdd,
    schema
)

out_df = out_df.orderBy(
    "origin",
    "month",
    F.when(F.col("band") == "low", 1)
     .when(F.col("band") == "medium", 2)
     .when(F.col("band") == "high", 3)
     .otherwise(4)
)


out_df.write.mode("overwrite").parquet(
    f"{BASE}/results/task_3_2/spark_core/task_3_2_spark_core_{SIZE}"
)


print(f"Dataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f}s")

spark.stop()
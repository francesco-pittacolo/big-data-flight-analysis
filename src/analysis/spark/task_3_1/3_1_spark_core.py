from pyspark.sql import SparkSession, Row
from collections import namedtuple
import time
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("-c", dest="cluster", action="store_true")
parser.add_argument("-s", dest="size", type=str)
args = parser.parse_args()

# CONFIGURAZIONE
BUCKET = "big-data-2026-project"
SIZE = args.size if args.size else "100"


if args.cluster:
    BASE = f"s3a://{BUCKET}"
else:
    BASE = "hdfs:///big_data"

os.makedirs("/tmp/spark-events", exist_ok=True)

spark = (SparkSession.builder
         .appName(f"3_1_spark_core_{SIZE}")
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", "file:///tmp/spark-events")
         .getOrCreate())


NUM_PARTITIONS = int(spark.sparkContext.defaultParallelism)

INPUT_PATH = f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
OUTPUT_PATH = f"{BASE}/results/task_3_1/spark_core/task_3_1_spark_core_{SIZE}"


start_time = time.perf_counter()


# LETTURA DATI
rdd = spark.read.parquet(INPUT_PATH).rdd


# ACCUMULATORE
FlightAcc = namedtuple(
    "FlightAcc",
    [
        "count",
        "min_d",
        "max_d",
        "sum_d",
        "count_valid",
        "cancel_sum",
        "months"
    ]
)

ZERO = FlightAcc(
    0,
    None,
    None,
    0.0,
    0,
    0.0,
    set()
)


# CREAZIONE CHIAVE
def extract_key(row):

    return (
        (row["op_unique_carrier"],
         f"{row['origin']}-{row['dest']}"),
        row
    )


# AGGREGAZIONE LOCALE
def seq_func(acc, row):

    arr_delay = (
        float(row["arr_delay"])
        if row["arr_delay"] is not None
        else None
    )

    cancelled = (
        float(row["cancelled"])
        if row["cancelled"] is not None
        else 0.0
    )

    # aggiornamento minimo
    if acc.min_d is None:
        min_d = arr_delay

    elif arr_delay is None:
        min_d = acc.min_d

    else:
        min_d = min(acc.min_d, arr_delay)


    # aggiornamento massimo
    if acc.max_d is None:
        max_d = arr_delay
    elif arr_delay is None:
        max_d = acc.max_d
    else:
        max_d = max(acc.max_d, arr_delay)


    return FlightAcc(
        acc.count + 1,
        min_d,
        max_d,
        acc.sum_d + (arr_delay if arr_delay is not None else 0),
        acc.count_valid + (1 if arr_delay is not None else 0),
        acc.cancel_sum + cancelled,
        acc.months | {int(row["month"])}
    )


# COMBINAZIONE DOPO SHUFFLE
def comb_func(a, b):

    if a.min_d is None:
        min_d = b.min_d
    elif b.min_d is None:
        min_d = a.min_d
    else:
        min_d = min(a.min_d, b.min_d)


    if a.max_d is None:
        max_d = b.max_d
    elif b.max_d is None:
        max_d = a.max_d
    else:
        max_d = max(a.max_d, b.max_d)


    return FlightAcc(
        a.count + b.count,
        min_d,
        max_d,
        a.sum_d + b.sum_d,
        a.count_valid + b.count_valid,
        a.cancel_sum + b.cancel_sum,
        a.months | b.months
    )


# CALCOLO METRICHE FINALI
def finalize(record):

    (carrier, route), acc = record


    avg_delay = (
        round(acc.sum_d / acc.count_valid, 2)
        if acc.count_valid > 0
        else None
    )


    return Row(
        op_unique_carrier=carrier,
        route=route,
        num_flights=acc.count,
        min_arr_delay=acc.min_d,
        max_arr_delay=acc.max_d,
        avg_arr_delay=avg_delay,
        cancel_rate=round(acc.cancel_sum / acc.count, 4),
        months=sorted(list(acc.months))
    )


# PIPELINE SPARK CORE
result_rdd = (
    rdd
    .map(extract_key)
    .aggregateByKey(
        ZERO,
        seq_func,
        comb_func,
        numPartitions=NUM_PARTITIONS
    )
    .map(finalize)
)


# OUTPUT
result_df = spark.createDataFrame(result_rdd)

result_df = result_df.orderBy(
    "op_unique_carrier",
    "route"
)


result_df.write.mode("overwrite").parquet(OUTPUT_PATH)


print(f"Dataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f}s")


spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
spark = SparkSession.builder.appName(f"3_3_spark_sql_{SIZE}").config("spark.eventLog.enabled", "true").getOrCreate()

start_time = time.perf_counter()
# =========================================================
# INPUT
# =========================================================
df = spark.read.parquet(
    f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
).select(
    "origin",
    "op_unique_carrier",
    "dep_delay",
    "arr_delay",
    "cancelled"
)

# =========================================================
# 1. METRICHE PER (AEROPORTO, COMPAGNIA)
# =========================================================
airline_airport = df.groupBy(
    "origin", "op_unique_carrier"
).agg(
    F.count("*").alias("num_flights"),
    F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
    F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
    F.round(F.avg("cancelled"), 4).alias("cancel_rate")
)

# =========================================================
# 2. MEDIA AEROPORTO (BASELINE)
# =========================================================
airport_avg = df.groupBy("origin").agg(
    F.avg("dep_delay").alias("airport_avg_dep_delay")
)

# =========================================================
# 3. JOIN
# =========================================================
df_join = airline_airport.join(airport_avg, "origin")

# =========================================================
# 4. DIFFERENZA RISPETTO MEDIA AEROPORTO
# =========================================================
df_join = df_join.withColumn(
    "dep_delay_diff",
    F.round(
        F.col("avg_dep_delay") - F.col("airport_avg_dep_delay"),
        2
    )
)

# =========================================================
# 5. RANKING (migliore = minor ritardo medio)
# =========================================================
w = Window.partitionBy("origin").orderBy(F.asc("avg_dep_delay"))

df_final = df_join.withColumn(
    "rank",
    F.row_number().over(w)
).drop("airport_avg_dep_delay")


# =========================================================
# 7. ORDINAMENTO FINALE
# =========================================================
df_final = df_final.orderBy("origin", "rank")

# =========================================================
# OUTPUT
# =========================================================
df_final.write.mode("overwrite").parquet(
    f"{BASE}/results/task_3_3/spark_sql/task_3_3_spark_sql_{SIZE}"
)

print(f"\nDataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f} seconds")

spark.stop()
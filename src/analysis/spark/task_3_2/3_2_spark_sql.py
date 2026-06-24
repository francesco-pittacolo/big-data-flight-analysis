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


os.makedirs("/tmp/spark-events", exist_ok=True)
spark = SparkSession.builder.appName(f"3_2_spark_sql_{SIZE}").config("spark.eventLog.enabled", "true").getOrCreate()

start_time = time.perf_counter()


# INPUT
df_raw = spark.read.parquet(
    f"{BASE}/data/processed/flights_cleaned_{SIZE}.parquet"
)

df = df_raw.select(
    "origin",
    "month",
    "dep_delay",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
)

# STEP 1: NON CANCELLED + BAND
df_banded = df.filter(
    (F.col("cancelled") == 0) &
    (F.col("dep_delay").isNotNull()) &
    (F.col("dep_delay") > 0)
).withColumn(
    "band",
    F.when(F.col("dep_delay") < 15, "low")
     .when(F.col("dep_delay") <= 60, "medium")
     .otherwise("high")
)

# STEP 2: CANCELLED CAUSES
df_cancelled_causes = df.filter(F.col("cancelled") == 1).withColumn(
    "band", F.lit("cancelled")
).withColumn(
    "cause",
    F.when(F.col("cancellation_code") == "A", "carrier")
     .when(F.col("cancellation_code") == "B", "weather")
     .when(F.col("cancellation_code") == "C", "nas")
     .when(F.col("cancellation_code") == "D", "security")
).select("origin", "month", "band", "cause") \
 .filter(F.col("cause").isNotNull())

# STEP 3: CAUSE EXPLOSION (NON CANCELLED)
df_exploded = df_banded.select(
    "origin", "month", "band",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay"
).select(
    "origin",
    "month",
    "band",
    F.explode(
        F.array(
            F.when(F.col("carrier_delay") > 0, F.lit("carrier_delay")),
            F.when(F.col("weather_delay") > 0, F.lit("weather_delay")),
            F.when(F.col("nas_delay") > 0, F.lit("nas_delay")),
            F.when(F.col("security_delay") > 0, F.lit("security_delay")),
            F.when(F.col("late_aircraft_delay") > 0, F.lit("late_aircraft_delay"))
        )
    ).alias("cause")
).filter(F.col("cause").isNotNull())

# STEP 4: UNION CAUSES
df_all_causes = df_exploded.unionByName(df_cancelled_causes)

# STEP 5: STATS NON CANCELLED
band_stats = df_banded.groupBy(
    "origin", "month", "band"
).agg(
    F.count("*").alias("num_flights"),
    F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
    F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay")
)

# STEP 5B: STATS CANCELLED
cancel_stats = df.filter(F.col("cancelled") == 1).groupBy(
    "origin", "month"
).agg(
    F.count("*").alias("num_flights")
).withColumn("avg_dep_delay", F.lit(None).cast("double")) \
 .withColumn("avg_arr_delay", F.lit(None).cast("double")) \
 .withColumn("band", F.lit("cancelled"))

# STEP 6: CAUSE COUNTS
cause_counts = df_all_causes.groupBy(
    "origin", "month", "band", "cause"
).agg(
    F.count("*").alias("cause_count")
)

# STEP 7: TOP 3 CAUSES
w = Window.partitionBy(
    "origin", "month", "band"
).orderBy(
    F.desc("cause_count"),
    F.asc("cause")
)

top3 = cause_counts.withColumn(
    "rn",
    F.row_number().over(w)
).filter(F.col("rn") <= 3).groupBy(
    "origin", "month", "band"
).agg(
    F.collect_list(
        F.struct("cause", "cause_count")
    ).alias("top_causes")
)

# STEP 8: UNION STATS
all_stats = band_stats.unionByName(cancel_stats)

# STEP 9: JOIN
final_df = all_stats.join(
    top3,
    on=["origin", "month", "band"],
    how="left"
)

# STEP 10: ORDERING
final_df = final_df.orderBy(
    "origin",
    "month",
    F.when(F.col("band") == "low", 1)
     .when(F.col("band") == "medium", 2)
     .when(F.col("band") == "high", 3)
     .otherwise(4)
)

# OUTPUT
final_df.write.mode("overwrite").parquet(
    f"{BASE}/results/task_3_2/spark_sql/task_3_2_spark_sql_{SIZE}"
)

print(f"\nDataset size: {SIZE}%")
print(f"Execution time: {time.perf_counter() - start_time:.3f} seconds")

spark.stop()
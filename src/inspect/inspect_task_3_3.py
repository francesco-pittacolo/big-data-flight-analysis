from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# CONFIG
MODES = "Spark_core"   # "spark_sql" | "spark_core" | "hive" | "all"
SIZE = "100"

# AIRPORT CONFIG
# - "all" → usa AIRLINES_LIMIT
# - "ABE" → singolo aeroporto
# - ["ABE","JFK"] → lista fissa (IGNORA AIRLINES_LIMIT)
TARGET_AIRPORTS = "all"

AIRLINES_LIMIT = 3
TOP_ROWS = 10

HDFS_BASE = "hdfs:///big_data"

PATHS = {
    "spark_sql":  f"{HDFS_BASE}/results/task_3_3/spark_sql/task_3_3_spark_sql_{SIZE}",
    "spark_core": f"{HDFS_BASE}/results/task_3_3/spark_core/task_3_3_spark_core_{SIZE}",
    "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/ranked_stats_{SIZE}"
}

# INIT
spark = SparkSession.builder.appName("Inspect_3_3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# LOADER
def load(mode):
    return spark.read.parquet(PATHS[mode])

# AIRPORT RESOLUTION
def resolve_airports(df):

    # caso 1: lista o singolo aeroporto
    if TARGET_AIRPORTS != "all":
        if isinstance(TARGET_AIRPORTS, str):
            return [TARGET_AIRPORTS]
        return TARGET_AIRPORTS

    # caso 2: all → top N aeroporti
    airports = (
        df.select("origin")
        .distinct()
        .orderBy("origin")
        .rdd.map(lambda r: r["origin"])
        .collect()
    )

    return airports[:AIRLINES_LIMIT]

# INSPECT
def inspect(mode, df):

    print("\n" + "#" * 80)
    print(f"MODE: {mode.upper()}")
    print("#" * 80)

    total_airports = df.select("origin").distinct().count()
    total_rows = df.count()

    print("\nGLOBAL STATISTICS")
    print("-" * 40)
    print(f"Total airports: {total_airports}")
    print(f"Total rows: {total_rows}")
    print(f"Avg rows per airport: {total_rows / total_airports:.2f}")

    airports = resolve_airports(df)

    print(f"\nAirports shown: {airports}")

    for airport in airports:

        print("\n" + "=" * 60)
        print(f"AIRPORT: {airport}")
        print("=" * 60)

        df.filter(col("origin") == airport) \
          .orderBy("rank") \
          .select(
              "op_unique_carrier",
              "num_flights",
              "avg_arr_delay",
              "avg_dep_delay",
              "cancel_rate",
              "dep_delay_diff",
              "rank"
          ) \
          .show(TOP_ROWS, truncate=False)

# RUN
modes = ["spark_sql", "spark_core", "hive"] if MODES == "all" else [MODES]

dfs = {}

for m in modes:
    dfs[m] = load(m)
    inspect(m, dfs[m])

# SUMMARY
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)

for m, df in dfs.items():
    print(f"{m} -> rows: {df.count()}")

spark.stop()
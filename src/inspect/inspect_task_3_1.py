from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc

# CONFIG
MODES = "spark_core" # "spark_sql" | "spark_core" | "hive" | "all"
SIZE = "100"
HDFS_BASE = "hdfs:///big_data"

# AIRLINE CONFIG
# - "all" → usa AIRLINES_LIMIT
# - "WN" → singola airline
# - ["WN","DL"] → lista fissa (IGNORA AIRLINES_LIMIT)
TARGET_AIRLINES = "all"

AIRLINES_LIMIT = 3
TOP_ROWS = 10

PATHS = {
    "spark_sql":  f"{HDFS_BASE}/results/task_3_1/spark_sql/task_3_1_spark_sql_{SIZE}",
    "spark_core": f"{HDFS_BASE}/results/task_3_1/spark_core/task_3_1_spark_core_{SIZE}",
    "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/routes_final_{SIZE}",
}

# INIT
spark = SparkSession.builder.appName("Inspect_3_1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# NORMALIZER
def normalize(df):
    return df.select(
        col("op_unique_carrier"),
        col("route"),
        col("num_flights"),
        col("avg_arr_delay"),
        col("cancel_rate"),
        col("months")
    )

# LOAD
def load(path):
    return normalize(spark.read.parquet(path))

# AIRLINE RESOLUTION
def resolve_airlines(df):

    # caso 1: override manuale
    if TARGET_AIRLINES != "all":
        if isinstance(TARGET_AIRLINES, str):
            return [TARGET_AIRLINES]
        return TARGET_AIRLINES

    # caso 2: TOP N per presenza
    airlines = (
        df.select("op_unique_carrier")
        .distinct()
        .orderBy("op_unique_carrier")
        .rdd.map(lambda r: r[0])
        .collect()
    )

    return airlines[:AIRLINES_LIMIT]

# SHOW RESULTS
def show_results(df, mode):

    print("\n" + "=" * 60)
    print(f"RESULTS — {mode.upper()}")
    print("=" * 60)

    airlines = resolve_airlines(df)

    for airline in airlines:

        print(f"\n--- {airline} ---")

        df.filter(col("op_unique_carrier") == airline) \
          .orderBy(desc("num_flights")) \
          .select(
              "route",
              "num_flights",
              "avg_arr_delay",
              "cancel_rate",
              "months"
          ) \
          .show(TOP_ROWS, truncate=False)

        print("Total routes:",
              df.filter(col("op_unique_carrier") == airline).count())

# RUN
modes = ["spark_sql", "spark_core", "hive"] if MODES == "all" else [MODES]

dfs = {}

for m in modes:
    print(f"Loading {m}...")
    dfs[m] = load(PATHS[m])

for mode, df in dfs.items():
    show_results(df, mode)

# SUMMARY
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)

for mode, df in dfs.items():
    print(f"{mode} -> rows: {df.count()}")

spark.stop()
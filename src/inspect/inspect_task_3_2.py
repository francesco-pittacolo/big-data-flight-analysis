from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# CONFIG
MODES = "all" # "spark_sql" | "spark_core" | "hive" | "all"
SIZE = "100"

# CONFIG AEROPORTI: 
# - "all" → usa AIRLINES_LIMIT
# - "ABE" → solo quello
# - ["ABE","JFK"] → lista specifica (IGNORA AIRLINES_LIMIT)
TARGET_AIRPORTS = "all"

AIRLINES_LIMIT = 3
TOP_MONTHS = 3

HDFS_BASE = "hdfs:///big_data"

PATHS = {
    "spark_sql":  f"{HDFS_BASE}/results/task_3_2/spark_sql/task_3_2_spark_sql_{SIZE}",
    "spark_core": f"{HDFS_BASE}/results/task_3_2/spark_core/task_3_2_spark_core_{SIZE}",
    "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/final_band_stats_{SIZE}"
}

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March",
    4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September",
    10: "October", 11: "November", 12: "December"
}

spark = SparkSession.builder.appName("Inspect_3_2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# LOAD
def load(mode):
    return spark.read.parquet(PATHS[mode])

def normalize(df):
    return df.select(
        "origin",
        "month",
        "band",
        "num_flights",
        "avg_dep_delay",
        "avg_arr_delay",
        "top_causes"
    )

# AIRPORT RESOLUTION
def resolve_airports(df):

    # caso 1: singolo o lista
    if TARGET_AIRPORTS != "all":
        if isinstance(TARGET_AIRPORTS, str):
            return [TARGET_AIRPORTS]
        return TARGET_AIRPORTS

    # caso 2: "all" -> usa AIRLINES_LIMIT
    airports = (
        df.select("origin")
        .distinct()
        .orderBy("origin")
        .rdd.map(lambda r: r["origin"])
        .collect()
    )

    return airports[:AIRLINES_LIMIT]

# INSPECT
def inspect_mode(mode):

    print("\n" + "#" * 80)
    print(f"MODE: {mode.upper()}")
    print("#" * 80)

    df = normalize(load(mode))

    airports = resolve_airports(df)

    if not airports:
        print("No airports found")
        return

    for airport in airports:

        print("\n" + "=" * 70)
        print(f"AIRPORT: {airport}")
        print("=" * 70)

        df_air = df.filter(col("origin") == airport)

        months = (
            df_air.select("month")
            .distinct()
            .orderBy("month")
            .rdd.map(lambda r: r["month"])
            .collect()
        )[:TOP_MONTHS]

        for m in months:

            print(f"\nMonth: {MONTH_NAMES.get(m, m)} ({m})")

            df_air.filter(col("month") == m) \
                .withColumn(
                    "band_order",
                    when(col("band") == "low", 1)
                    .when(col("band") == "medium", 2)
                    .when(col("band") == "high", 3)
                    .otherwise(4)
                ) \
                .orderBy("band_order") \
                .drop("band_order") \
                .withColumn(
                    "top_causes",
                    when(col("top_causes").isNull(), lit([]))
                    .otherwise(col("top_causes"))
                ) \
                .select(
                    "band",
                    "num_flights",
                    "avg_dep_delay",
                    "avg_arr_delay",
                    "top_causes"
                ) \
                .show(truncate=False)

# RUN
modes = ["spark_sql", "spark_core", "hive"] if MODES == "all" else [MODES]

for m in modes:
    inspect_mode(m)

spark.stop()
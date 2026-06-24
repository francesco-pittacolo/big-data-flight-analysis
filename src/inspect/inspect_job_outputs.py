from pyspark.sql import SparkSession

# CONFIG
TASK = "3_2"   # <-- cambia qui: "3_1", "3_2", "3_3"
SIZE = "100"
HDFS_BASE = "hdfs:///big_data"

PATHS = {
    "3_1": {
        "spark_sql":  f"{HDFS_BASE}/results/task_3_1/spark_sql/task_3_1_spark_sql_{SIZE}",
        "spark_core": f"{HDFS_BASE}/results/task_3_1/spark_core/task_3_1_spark_core_{SIZE}",
        "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/routes_{SIZE}",
    },
    "3_2": {
        "spark_sql":  f"{HDFS_BASE}/results/task_3_2/spark_sql/task_3_2_spark_sql_{SIZE}",
        "spark_core": f"{HDFS_BASE}/results/task_3_2/spark_core/task_3_2_spark_core_{SIZE}",
        "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/final_band_stats_{SIZE}",
    },
    "3_3": {
        "spark_sql":  f"{HDFS_BASE}/results/task_3_3/spark_sql/task_3_3_spark_sql_{SIZE}",
        "spark_core": f"{HDFS_BASE}/results/task_3_3/spark_core/task_3_3_spark_core_{SIZE}",
        "hive":       f"hdfs:///user/hive/warehouse/flights_db.db/ranked_stats_{SIZE}",
    }
}

# INIT
spark = SparkSession.builder \
    .appName(f"Inspect_{TASK}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# LOAD & PRINT
def load(path):
    return spark.read.parquet(path)

for mode, path in PATHS[TASK].items():

    print("\n" + "=" * 80)
    print(f"{mode.upper()} — TASK {TASK}")
    print("=" * 80)

    df = load(path)

    print("\nSchema:")
    df.printSchema()

    print("\nFirst 10 rows:")
    df.show(10, truncate=80)

    print(f"\nTotal rows: {df.count()}")

spark.stop()
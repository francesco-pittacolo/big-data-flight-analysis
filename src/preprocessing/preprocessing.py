from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", dest="cluster", action="store_true", help="Run in cluster mode")
args = parser.parse_args()

spark = SparkSession.builder.appName("Clean_Flights_General").getOrCreate()

BASE_PATH = Path(__file__).resolve().parents[2]

S3_BUCKET = "big-data-2026-project"


if args.cluster:
    print("Running in cluster mode")

    INPUT_PATH = f"s3a://{S3_BUCKET}/data/raw/flight_data_2024.csv"
    BASE_OUTPUT = f"s3a://{S3_BUCKET}/data/processed/flights_cleaned"
else:
    INPUT_PATH = "file://" + str(BASE_PATH / "data" / "flight_data_2024.csv")
    BASE_OUTPUT = "hdfs:///big_data/data/processed/flights_cleaned"

# =========================================================
# SCALING FACTORS
# =========================================================
SIZES = [0.25, 0.5, 0.75, 1, 2, 4]

# =========================================================
# LOAD
# =========================================================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_PATH)
)

# =========================================================
# CLEANING
# =========================================================

df = (
    df.withColumn("op_unique_carrier", F.upper(F.trim("op_unique_carrier")))
      .withColumn("origin", F.upper(F.trim("origin")))
      .withColumn("dest", F.upper(F.trim("dest")))
)

df = (
    df.withColumn("month", F.col("month").cast("int"))
      .withColumn("cancelled", F.col("cancelled").cast("int"))
      .withColumn("diverted", F.col("diverted").cast("int"))
      .withColumn("arr_delay", F.col("arr_delay").cast("double"))
      .withColumn("dep_delay", F.col("dep_delay").cast("double"))
)


df = df.withColumn(
    "dep_delay",
    F.when(
        (F.col("cancelled") == 1),
        None
    ).otherwise(F.col("dep_delay"))
)

df = df.withColumn(
    "arr_delay",
    F.when(
        (F.col("cancelled") == 1) | (F.col("diverted") == 1),
        None
    ).otherwise(F.col("arr_delay"))
)



# =========================================================
# DATASET GENERATION
# =========================================================
for size in SIZES:

    # -----------------------------
    # SUBSAMPLING (< 1)
    # -----------------------------
    if size < 1:
        df_out = df.sample(False, size, seed=42)

    # -----------------------------
    # ORIGINAL (1x)
    # -----------------------------
    elif size == 1:
        df_out = df

    # -----------------------------
    # REPLICATION (> 1)
    # -----------------------------
    else:
        replicas = int(size)

        replicated = [
            df.withColumn("__replica_id", F.lit(i))
            for i in range(replicas)
        ]

        df_out = replicated[0]

        for extra in replicated[1:]:
            df_out = df_out.unionByName(extra)

        df_out = df_out.drop("__replica_id")

    # -----------------------------
    # OUTPUT PATH
    # -----------------------------
    output_size = int(size * 100)
    output_path = f"{BASE_OUTPUT}_{output_size}.parquet"

    print(f"Writing dataset x{size} -> {output_path}")

    (
        df_out.write
        .mode("overwrite")
        .parquet(output_path)
    )

spark.stop()
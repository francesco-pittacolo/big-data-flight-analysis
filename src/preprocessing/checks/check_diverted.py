from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

spark = SparkSession.builder.appName("check_diverted").getOrCreate()

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_PATH = BASE_DIR / "data" / "flight_data_2024.csv"


df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"file://{CSV_PATH}")
)

# 1. Conteggio totale
total = df.count()

# 2. Valori diverted (0 / 1 / null)
df.groupBy("diverted").count().show()

# 3. Conteggi separati
stats = df.select(
    F.count("*").alias("total_rows"),

    F.sum(F.when(F.col("diverted") == 1, 1).otherwise(0)).alias("diverted_1"),

    F.sum(F.when(F.col("diverted") == 0, 1).otherwise(0)).alias("diverted_0"),

    F.sum(F.when(F.col("diverted").isNull(), 1).otherwise(0)).alias("diverted_null")
)

stats.show()

# 4. check qualità su delay nei diverted
df.filter(F.col("diverted") == 1).select(
    "dep_delay",
    "arr_delay"
).describe().show()
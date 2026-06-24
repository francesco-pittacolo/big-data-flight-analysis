from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

spark = SparkSession.builder.appName("check_cancelled").getOrCreate()

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
print("Total rows:", total)

# 2. Distribuzione cancelled (0 / 1 / null)
df.groupBy("cancelled").count().show()

# 3. Conteggi separati (robusto e leggibile)
stats = df.select(
    F.count("*").alias("total_rows"),

    F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_1"),

    F.sum(F.when(F.col("cancelled") == 0, 1).otherwise(0)).alias("cancelled_0"),

    F.sum(F.when(F.col("cancelled").isNull(), 1).otherwise(0)).alias("cancelled_null")
)

stats.show()

# 4. Check qualità ritardi nei cancellati
df.filter(F.col("cancelled") == 1).select(
    "dep_delay",
    "arr_delay"
).describe().show()

# 5. Per vedere alcuni esempi
df.filter(F.col("cancelled") == 1).select(
    "dep_delay",
    "arr_delay"
).show(10, truncate=False)

df.select("arr_delay").printSchema()
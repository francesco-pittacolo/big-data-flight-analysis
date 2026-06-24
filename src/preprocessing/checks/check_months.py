from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

spark = SparkSession.builder \
    .appName("Check_Invalid_Month") \
    .getOrCreate()

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_PATH = BASE_DIR / "data" / "flight_data_2024.csv"

# Caricamento dataset
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(CSV_PATH)
)

# Conversione tipo
df = df.withColumn("month", F.col("month").cast("int"))

# Record con mese fuori range
invalid_month = df.filter(
    F.col("month").isNotNull() &
    ((F.col("month") < 1) | (F.col("month") > 12))
)

# Conteggio
print("Totale record:", df.count())

print("Record con mese fuori range:")
print(invalid_month.count())

# Distribuzione valori mese
print("Distribuzione mese:")
(
    df.groupBy("month")
    .count()
    .orderBy("month")
    .show()
)

spark.stop()
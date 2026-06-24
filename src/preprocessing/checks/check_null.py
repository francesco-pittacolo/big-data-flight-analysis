import os
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_PATH = BASE_DIR / "data" / "flight_data_2024.csv"

print("Loading CSV...")

df = pd.read_csv(
    CSV_PATH,
    low_memory=False
)

print(f"Rows loaded: {len(df):,}")
print(f"Columns: {len(df.columns)}")

# NULL COUNT
null_report = pd.DataFrame({
    "column": df.columns,
    "null_count": df.isnull().sum().values,
    "null_percentage": (
        (df.isnull().sum() / len(df) * 100).round(4).values
    )
})

null_report = null_report.sort_values(
    by="null_count",
    ascending=False
)

print("\n====================")
print("NULL REPORT")
print("====================")

print(null_report.to_string(index=False))


# salva nella stessa cartella dello script
script_dir = os.path.dirname(os.path.abspath(__file__))

output_dir = os.path.join(
    script_dir,
    "csv"
)

os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(
    output_dir,
    "null_report.csv"
)

null_report.to_csv(
    output_path,
    index=False
)

print(f"\nReport salvato in {output_path}")
import pandas as pd
import numpy as np
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[3]
CSV_PATH = BASE_DIR / "data" / "flight_data_2024.csv"


print("Loading CSV...")

df = pd.read_csv(CSV_PATH, low_memory=False)

print(f"Rows loaded: {len(df):,}")

# CSV ROW INDEX (for debugging)
df["_csv_row"] = df.index + 2

# FILTER CANCELLED / DIVERTED
removed_cd = df[(df["cancelled"] == 1) | (df["diverted"] == 1)]
df = df[(df["cancelled"] == 0) & (df["diverted"] == 0)].copy()

print(f"Removed cancelled/diverted: {len(removed_cd):,}")
print(f"Rows after filter: {len(df):,}")

# HHMM -> MINUTES
def hhmm_to_minutes(col):
    col = pd.to_numeric(col, errors="coerce")
    return (col // 100) * 60 + (col % 100)

dep = hhmm_to_minutes(df["dep_time"])
arr = hhmm_to_minutes(df["arr_time"])

crs_dep = hhmm_to_minutes(df["crs_dep_time"])
crs_arr = hhmm_to_minutes(df["crs_arr_time"])

wheels_off = hhmm_to_minutes(df["wheels_off"])
wheels_on = hhmm_to_minutes(df["wheels_on"])

#-----------
# HELPERS

def wrap_24h(x):
    return ((x + 720) % 1440) - 720


def elapsed(a, b):
    x = b - a
    return np.where(x < 0, x + 1440, x)


def timezone_ok(expected, actual):
    """
    True if difference is consistent with timezone shift (multiples of 60 min)
    """
    diff = wrap_24h(expected - actual)
    return (diff % 60) == 0
#-----------

# EXPECTED VALUES
dep_delay_calc = wrap_24h(dep - crs_dep)
arr_delay_calc = wrap_24h(arr - crs_arr)

taxi_out_calc = elapsed(dep, wheels_off)
taxi_in_calc = elapsed(wheels_on, arr)

air_time_calc = elapsed(wheels_off, wheels_on)
actual_elapsed_calc = elapsed(dep, arr)

# ERROR MASKS
dep_delay_errors = ~timezone_ok(dep_delay_calc, df["dep_delay"])
arr_delay_errors = ~timezone_ok(arr_delay_calc, df["arr_delay"])

taxi_out_errors = ~timezone_ok(taxi_out_calc, df["taxi_out"])
taxi_in_errors = ~timezone_ok(taxi_in_calc, df["taxi_in"])

air_time_errors = ~timezone_ok(air_time_calc, df["air_time"])

actual_elapsed_errors = ~timezone_ok(actual_elapsed_calc, df["actual_elapsed_time"])

component_errors = (
    df["taxi_out"] + df["air_time"] + df["taxi_in"]
) != df["actual_elapsed_time"]

# REPORT
print("\n====================")
print("VALIDATION REPORT")
print("====================")

checks = [
    ("dep_delay", dep_delay_errors),
    ("arr_delay", arr_delay_errors),
    ("taxi_out", taxi_out_errors),
    ("taxi_in", taxi_in_errors),
    ("air_time", air_time_errors),
    ("actual_elapsed_time", actual_elapsed_errors),
    ("components_sum", component_errors),
]

for name, mask in checks:
    print(f"{name:<22}: {mask.sum():,}")

# SAVE BAD ROWS
all_errors = (
    dep_delay_errors
    | arr_delay_errors
    | taxi_out_errors
    | taxi_in_errors
    | air_time_errors
    | actual_elapsed_errors
    | component_errors
)

bad_rows = df[all_errors].copy()

script_dir = os.path.dirname(os.path.abspath(__file__))

output_dir = os.path.join(
    script_dir,
    "csv"
)

os.makedirs(output_dir, exist_ok=True)

bad_rows.to_csv(os.path.join(
    output_dir,
    "bad_rows.csv"
), index=False)

print(f"\nTotal bad rows: {len(bad_rows):,}")

def show_table(mask, calc, field, n=10):

    idxs = np.where(mask)[0]
    total = len(idxs)

    if total == 0:
        return

    idxs = idxs[:n]

    if field == "taxi_out":
        cols = ["_csv_row", "origin", "dest", "dep_time", "wheels_off"]

    elif field == "taxi_in":
        cols = ["_csv_row", "origin", "dest", "wheels_on", "arr_time"]

    elif field == "air_time":
        cols = ["_csv_row", "origin", "dest", "wheels_off", "wheels_on", "air_time"]

    elif field == "actual_elapsed_time":
        cols = ["_csv_row", "origin", "dest", "dep_time", "arr_time", "actual_elapsed_time"]

    elif field in ["dep_delay", "arr_delay"]:
        cols = ["_csv_row", "origin", "dest", "crs_dep_time", "dep_time", "crs_arr_time", "arr_time"]

    else:
        cols = ["_csv_row", "origin", "dest"]

    rows = []

    for i in idxs:
        r = df.iloc[i]

        rows.append({
            **{c: r.get(c, None) for c in cols},
            "stored": r[field],
            "expected": calc[i],
            "diff": calc[i] - r[field]
        })

    out = pd.DataFrame(rows)

    print(f"\n--- {field.upper()} ERRORS ({total:,} total) ---")
    print(out.to_string(index=False))


# SHOW EXAMPLES
show_table(taxi_out_errors, taxi_out_calc, "taxi_out")
show_table(taxi_in_errors, taxi_in_calc, "taxi_in")
show_table(air_time_errors, air_time_calc, "air_time")
show_table(actual_elapsed_errors, actual_elapsed_calc, "actual_elapsed_time")
import boto3
from pathlib import Path

# CONFIG
BUCKET = "big-data-2026-project"

EXCLUDE = {
    "metastore_db",
    "derby.log",
    "__pycache__",
    "logs",
    "venv",
    ".git"
}

# ROOT PROJECT
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

UPLOAD_DIRS = [
    PROJECT_DIR / "src",
    PROJECT_DIR / "scripts"
]

s3 = boto3.client("s3")

for upload_dir in UPLOAD_DIRS:

    for local_file in upload_dir.rglob("*"):

        if local_file.is_file() and not any(
            p in EXCLUDE for p in local_file.parts
        ):

            relative = local_file.relative_to(PROJECT_DIR)

            s3_key = str(relative)

            print(
                f"Uploading {local_file} -> s3://{BUCKET}/{s3_key}"
            )

            s3.upload_file(
                str(local_file),
                BUCKET,
                s3_key
            )

print("DONE")
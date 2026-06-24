import boto3
from pathlib import Path

BUCKET = "big-data-2026-project"

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

LOCAL_FILE = PROJECT_DIR / "src" / "preprocessing" / "preprocessing.py"

S3_KEY = "src/preprocessing/preprocessing.py"

s3 = boto3.client("s3")

print(f"Uploading {LOCAL_FILE} -> s3://{BUCKET}/{S3_KEY}")

s3.upload_file(
    str(LOCAL_FILE),
    BUCKET,
    S3_KEY
)

print("DONE")
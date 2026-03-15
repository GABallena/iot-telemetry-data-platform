
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")

LOCAL_DATA_LAKE_ROOT = Path(os.getenv(
    "LOCAL_DATA_LAKE_ROOT",
    str(PROJECT_ROOT / "data_lake"),
))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "telemetry-lake")

SAMPLE_DATA_DIR = PROJECT_ROOT / "data" / "sample"

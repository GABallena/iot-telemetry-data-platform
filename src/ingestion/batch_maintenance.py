
from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("ingestion.maintenance")


WATERMARK_KEY = "metadata/watermarks/maintenance.json"


def _load_watermarks(s3: StorageClient) -> dict:
    if s3.exists(WATERMARK_KEY):
        return json.loads(s3.get_object(WATERMARK_KEY).decode())
    return {}


def _save_watermarks(s3: StorageClient, wm: dict) -> None:
    s3.put_object(WATERMARK_KEY, json.dumps(wm, indent=2))


def _csv_to_rows(path: Path) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _rows_to_parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    columns: dict[str, list] = {f.name: [] for f in schema}
    for row in rows:
        for col in columns:
            val = row.get(col)
            field = schema.field(col)
            if pa.types.is_integer(field.type) and val is not None:
                val = int(val)
            elif pa.types.is_floating(field.type) and val is not None:
                val = float(val)
            columns[col].append(val)
    table = pa.table(columns, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


MACHINES_SCHEMA = pa.schema([
    ("machine_id", pa.string()),
    ("factory_id", pa.string()),
    ("machine_type", pa.string()),
    ("install_date", pa.string()),
    ("status", pa.string()),
])

MAINTENANCE_LOGS_SCHEMA = pa.schema([
    ("maintenance_id", pa.string()),
    ("machine_id", pa.string()),
    ("timestamp", pa.string()),
    ("maintenance_type", pa.string()),
    ("part_replaced", pa.string()),
    ("downtime_minutes", pa.int64()),
])


def _ingest_machines(s3: StorageClient, source_dir: Path, batch_ts: str) -> int:
    path = source_dir / "machines.csv"
    if not path.exists():
        logger.warning("machines.csv not found, skipping")
        return 0

    rows = _csv_to_rows(path)
    key = f"raw/maintenance/machines/snapshot_{batch_ts}.parquet"
    s3.put_object(key, _rows_to_parquet_bytes(rows, MACHINES_SCHEMA))
    logger.info("machines: wrote %d rows → %s", len(rows), key)
    return len(rows)


def _ingest_maintenance_logs(
    s3: StorageClient, source_dir: Path, batch_ts: str, watermarks: dict
) -> tuple[int, str | None]:
    path = source_dir / "maintenance_logs.csv"
    if not path.exists():
        logger.warning("maintenance_logs.csv not found, skipping")
        return 0, None

    last_watermark = watermarks.get("maintenance_logs")
    rows = _csv_to_rows(path)

    if last_watermark:
        new_rows = [r for r in rows if r["timestamp"] > last_watermark]
    else:
        new_rows = rows

    if not new_rows:
        logger.info("maintenance_logs: no new rows since last watermark")
        return 0, last_watermark

    new_watermark = max(r["timestamp"] for r in new_rows)

    from collections import defaultdict
    partitions: dict[str, list[dict]] = defaultdict(list)
    for row in new_rows:
        ts = datetime.fromisoformat(row["timestamp"])
        partitions[f"year={ts.year}/month={ts.month:02d}"].append(row)

    total = 0
    for partition, part_rows in partitions.items():
        key = f"raw/maintenance/maintenance_logs/{partition}/batch_{batch_ts}.parquet"
        s3.put_object(key, _rows_to_parquet_bytes(part_rows, MAINTENANCE_LOGS_SCHEMA))
        total += len(part_rows)
        logger.info("maintenance_logs: %d rows → %s", len(part_rows), key)

    return total, new_watermark


def ingest_maintenance(source_dir: str | Path | None = None) -> dict:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from config.settings import SAMPLE_DATA_DIR

    if source_dir is None:
        source_dir = SAMPLE_DATA_DIR
    source_dir = Path(source_dir)

    s3 = get_storage_client()
    batch_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    watermarks = _load_watermarks(s3)

    logger.info("Starting batch ingestion (watermarks: %s)", watermarks)

    machines_count = _ingest_machines(s3, source_dir, batch_ts)

    logs_count, new_wm = _ingest_maintenance_logs(s3, source_dir, batch_ts, watermarks)
    if new_wm:
        watermarks["maintenance_logs"] = new_wm
        _save_watermarks(s3, watermarks)
        logger.info("Watermark updated → %s", new_wm)

    stats = {
        "machines_rows": machines_count,
        "maintenance_logs_rows": logs_count,
        "watermark": watermarks.get("maintenance_logs"),
        "batch_id": batch_ts,
    }
    logger.info("Done: %d machines, %d log entries", machines_count, logs_count)
    return stats


if __name__ == "__main__":
    ingest_maintenance()

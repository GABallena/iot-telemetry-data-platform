
from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("ingestion.production")


WATERMARK_KEY = "metadata/watermarks/production.json"


def _load_watermark(s3: StorageClient) -> str | None:
    if s3.exists(WATERMARK_KEY):
        data = json.loads(s3.get_object(WATERMARK_KEY).decode())
        return data.get("last_date")
    return None


def _save_watermark(s3: StorageClient, last_date: str) -> None:
    s3.put_object(WATERMARK_KEY, json.dumps({"last_date": last_date}, indent=2))


PRODUCTION_SCHEMA = pa.schema([
    ("date", pa.string()),
    ("machine_id", pa.string()),
    ("shift", pa.string()),
    ("units_produced", pa.int64()),
    ("scrap_units", pa.int64()),
    ("operator_id", pa.string()),
])


def _rows_to_parquet_bytes(rows: list[dict]) -> bytes:
    columns: dict[str, list] = {f.name: [] for f in PRODUCTION_SCHEMA}
    for row in rows:
        for col in columns:
            val = row.get(col)
            field = PRODUCTION_SCHEMA.field(col)
            if pa.types.is_integer(field.type) and val is not None:
                val = int(val)
            columns[col].append(val)
    table = pa.table(columns, schema=PRODUCTION_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def ingest_production(source_path: str | Path | None = None) -> dict:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from config.settings import SAMPLE_DATA_DIR

    if source_path is None:
        source_path = SAMPLE_DATA_DIR / "daily_production.csv"
    source_path = Path(source_path)

    s3 = get_storage_client()
    batch_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    last_date = _load_watermark(s3)

    logger.info("Starting ingestion (last watermark: %s)", last_date)

    with open(source_path, newline="") as f:
        rows = list(csv.DictReader(f))

    if last_date:
        new_rows = [r for r in rows if r["date"] > last_date]
    else:
        new_rows = rows

    if not new_rows:
        logger.info("No new rows since last watermark")
        return {"rows": 0, "watermark": last_date, "batch_id": batch_ts}

    partitions: dict[str, list[dict]] = defaultdict(list)
    for row in new_rows:
        dt = datetime.strptime(row["date"], "%Y-%m-%d")
        partitions[f"year={dt.year}/month={dt.month:02d}"].append(row)

    total = 0
    for partition, part_rows in partitions.items():
        key = f"raw/production/{partition}/batch_{batch_ts}.parquet"
        s3.put_object(key, _rows_to_parquet_bytes(part_rows))
        total += len(part_rows)
        logger.info("%d rows → %s", len(part_rows), key)

    new_watermark = max(r["date"] for r in new_rows)
    _save_watermark(s3, new_watermark)
    logger.info("Watermark updated → %s", new_watermark)

    stats = {
        "rows": total,
        "watermark": new_watermark,
        "batch_id": batch_ts,
    }
    logger.info("Done: %d rows ingested", total)
    return stats


if __name__ == "__main__":
    ingest_production()

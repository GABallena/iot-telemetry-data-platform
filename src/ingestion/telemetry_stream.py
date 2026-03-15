
from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import get_storage_client

logger = get_logger("ingestion.telemetry")


TELEMETRY_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("machine_id", pa.string()),
    ("factory_id", pa.string()),
    ("timestamp", pa.string()),
    ("temperature_c", pa.float64()),
    ("vibration_mm_s", pa.float64()),
    ("power_kw", pa.float64()),
    ("status", pa.string()),
])


def _events_to_parquet_bytes(events: list[dict]) -> bytes:
    columns: dict[str, list] = {f.name: [] for f in TELEMETRY_SCHEMA}
    for e in events:
        for col in columns:
            columns[col].append(e.get(col))

    table = pa.table(columns, schema=TELEMETRY_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


def _partition_key(year: int, month: int, day: int, hour: int, batch_id: str) -> str:
    return (
        f"raw/telemetry/"
        f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
        f"batch_{batch_id}.parquet"
    )


def ingest_telemetry_stream(
    source_path: str | Path | None = None,
    batch_size: int = 50,
) -> dict:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from config.settings import SAMPLE_DATA_DIR

    if source_path is None:
        source_path = SAMPLE_DATA_DIR / "telemetry_events.jsonl"
    source_path = Path(source_path)

    s3 = get_storage_client()
    batch_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")

    total_events = 0
    total_partitions = 0
    total_batches = 0
    buffer: list[dict] = []

    def flush(buf: list[dict]) -> int:
        partitions: dict[tuple, list[dict]] = defaultdict(list)
        for event in buf:
            ts = datetime.fromisoformat(event["timestamp"])
            partitions[(ts.year, ts.month, ts.day, ts.hour)].append(event)

        written = 0
        for (y, m, d, h), events in partitions.items():
            key = _partition_key(y, m, d, h, f"{batch_ts}_{written:04d}")
            parquet_data = _events_to_parquet_bytes(events)
            s3.put_object(key, parquet_data)
            written += 1
        return written

    logger.info("Starting micro-batch ingestion from %s", source_path)
    logger.info("Batch size: %d", batch_size)

    with open(source_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            buffer.append(event)
            total_events += 1

            if len(buffer) >= batch_size:
                parts = flush(buffer)
                total_partitions += parts
                total_batches += 1
                logger.info("Flushed batch %d: %d events → %d partition(s)",
                            total_batches, len(buffer), parts)
                buffer.clear()

    if buffer:
        parts = flush(buffer)
        total_partitions += parts
        total_batches += 1
        logger.info("Flushed batch %d (final): %d events → %d partition(s)",
                    total_batches, len(buffer), parts)
        buffer.clear()

    stats = {
        "source": str(source_path),
        "total_events": total_events,
        "total_batches": total_batches,
        "total_partitions_written": total_partitions,
        "batch_id": batch_ts,
    }
    logger.info("Done: %d events in %d batches", total_events, total_batches)
    return stats


if __name__ == "__main__":
    ingest_telemetry_stream()

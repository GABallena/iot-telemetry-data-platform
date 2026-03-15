from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_project_root))

from config.settings import SAMPLE_DATA_DIR
from src.storage.client import get_storage_client


def _seed_telemetry(s3) -> int:
    jsonl_path = SAMPLE_DATA_DIR / "telemetry_events.jsonl"
    if not jsonl_path.exists():
        print(f"  ⚠  {jsonl_path} not found, skipping telemetry seed.")
        return 0

    partitions: dict[tuple, list[dict]] = defaultdict(list)
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            ts = datetime.fromisoformat(event["timestamp"])
            key = (ts.year, ts.month, ts.day, ts.hour)
            partitions[key].append(event)

    count = 0
    for (year, month, day, hour), events in sorted(partitions.items()):
        partition_key = f"raw/telemetry/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/events.jsonl"
        payload = "\n".join(json.dumps(e) for e in events) + "\n"
        s3.put_object(partition_key, payload.encode())
        count += len(events)

    return count


def _seed_csv(s3, filename: str, dest_key: str) -> int:
    src_path = SAMPLE_DATA_DIR / filename
    if not src_path.exists():
        print(f"  ⚠  {src_path} not found, skipping.")
        return 0
    with open(src_path, "rb") as f:
        data = f.read()
    s3.put_object(dest_key, data)
    return data.count(b"\n") - 1


def seed_all():
    s3 = get_storage_client()
    print(f"Seeding data lake via {s3!r}\n")

    n = _seed_telemetry(s3)
    print(f"  ✓ telemetry events: {n} events across partitions")

    n = _seed_csv(s3, "machines.csv", "raw/maintenance/machines.csv")
    print(f"  ✓ machines:         {n} rows")

    n = _seed_csv(s3, "maintenance_logs.csv", "raw/maintenance/maintenance_logs.csv")
    print(f"  ✓ maintenance_logs: {n} rows")

    n = _seed_csv(s3, "daily_production.csv", "raw/production/daily_production.csv")
    print(f"  ✓ daily_production: {n} rows")

    print("\nDone. Objects in data lake:")
    for key in s3.list_objects("raw/"):
        print(f"  {key}")


if __name__ == "__main__":
    seed_all()

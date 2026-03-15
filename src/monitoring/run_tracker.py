from __future__ import annotations

import json
from datetime import UTC, datetime

from src.log import get_logger
from src.storage.client import get_storage_client

logger = get_logger("monitoring.run_tracker")

HISTORY_KEY = "metadata/runs/history.jsonl"


def record_run(
    step: str,
    *,
    status: str = "success",
    rows: int = 0,
    duration_seconds: float = 0.0,
    error: str | None = None,
    metadata: dict | None = None,
) -> dict:
    s3 = get_storage_client()

    record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "step": step,
        "status": status,
        "rows": rows,
        "duration_seconds": round(duration_seconds, 3),
        "error": error,
        "metadata": metadata or {},
    }

    existing = b""
    if s3.exists(HISTORY_KEY):
        existing = s3.get_object(HISTORY_KEY)

    line = json.dumps(record) + "\n"
    s3.put_object(HISTORY_KEY, existing + line.encode())

    logger.info("Recorded run: step=%s status=%s rows=%d", step, status, rows)
    return record


def get_run_history(step: str | None = None) -> list[dict]:
    s3 = get_storage_client()
    if not s3.exists(HISTORY_KEY):
        return []

    data = s3.get_object(HISTORY_KEY).decode()
    events = []
    for line in data.strip().split("\n"):
        if line.strip():
            record = json.loads(line)
            if step is None or record.get("step") == step:
                events.append(record)
    return events


def get_failed_runs(since_hours: int = 24) -> list[dict]:
    cutoff = datetime.now(UTC).timestamp() - (since_hours * 3600)
    failed = []
    for run in get_run_history():
        ts = datetime.fromisoformat(run["timestamp"]).timestamp()
        if ts >= cutoff and run["status"] != "success":
            failed.append(run)
    return failed

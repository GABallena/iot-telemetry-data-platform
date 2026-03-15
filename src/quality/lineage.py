from __future__ import annotations

import json
from datetime import UTC, datetime

from src.log import get_logger
from src.storage.client import get_storage_client

logger = get_logger("quality.lineage")

LINEAGE_KEY = "metadata/lineage/events.jsonl"


def record_lineage(
    step: str,
    inputs: list[str],
    outputs: list[str],
    input_rows: int = 0,
    output_rows: int = 0,
    metadata: dict | None = None,
) -> dict:
    s3 = get_storage_client()

    event = {
        "timestamp": datetime.now(UTC).isoformat(),
        "step": step,
        "inputs": inputs,
        "outputs": outputs,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "metadata": metadata or {},
    }

    existing = b""
    if s3.exists(LINEAGE_KEY):
        existing = s3.get_object(LINEAGE_KEY)

    line = json.dumps(event) + "\n"
    s3.put_object(LINEAGE_KEY, existing + line.encode())

    return event


def get_lineage() -> list[dict]:
    s3 = get_storage_client()
    if not s3.exists(LINEAGE_KEY):
        return []

    data = s3.get_object(LINEAGE_KEY).decode()
    events = []
    for line in data.strip().split("\n"):
        if line.strip():
            events.append(json.loads(line))
    return events


if __name__ == "__main__":
    events = get_lineage()
    if events:
        for e in events:
            print(json.dumps(e, indent=2))
    else:
        print("No lineage events recorded yet.")
        print("Recording a sample lineage event...")
        record_lineage(
            step="example",
            inputs=["raw/telemetry/"],
            outputs=["staging/telemetry/telemetry.parquet"],
            input_rows=500,
            output_rows=500,
        )
        print("Done. Run again to see it.")

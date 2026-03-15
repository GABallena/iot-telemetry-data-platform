from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("warehouse.queries")


def _read(s3: StorageClient, key: str) -> pa.Table | None:
    if not s3.exists(key):
        return None
    return pq.ParquetFile(pa.BufferReader(s3.get_object(key))).read()


def machines_trending_toward_failure(s3: StorageClient) -> list[dict]:
    health = _read(s3, "analytics/machine_health_features.parquet")
    if health is None:
        return []

    results = []
    for i in range(len(health)):
        avg_temp = health.column("avg_temperature_c")[i].as_py()
        avg_vib = health.column("avg_vibration_mm_s")[i].as_py()
        max_temp = health.column("max_temperature_c")[i].as_py()
        reasons = []
        if avg_temp and avg_temp > 75:
            reasons.append(f"avg_temp={avg_temp:.1f}°C")
        if avg_vib and avg_vib > 3.0:
            reasons.append(f"avg_vibration={avg_vib:.2f}mm/s")
        if max_temp and max_temp > 90:
            reasons.append(f"max_temp={max_temp:.1f}°C")
        if reasons:
            results.append(
                {
                    "machine_id": health.column("machine_id")[i].as_py(),
                    "avg_temperature_c": round(avg_temp, 1) if avg_temp else None,
                    "avg_vibration_mm_s": round(avg_vib, 2) if avg_vib else None,
                    "max_temperature_c": round(max_temp, 1) if max_temp else None,
                    "risk_factors": reasons,
                }
            )

    results.sort(key=lambda r: r.get("avg_temperature_c") or 0, reverse=True)
    return results


def overheating_before_shutdown(s3: StorageClient) -> list[dict]:
    telemetry = _read(s3, "staging/telemetry/telemetry.parquet")
    if telemetry is None:
        return []

    machine_events: dict[str, list[dict]] = {}
    for i in range(len(telemetry)):
        mid = telemetry.column("machine_id")[i].as_py()
        if mid not in machine_events:
            machine_events[mid] = []
        machine_events[mid].append(
            {
                "timestamp": telemetry.column("timestamp")[i].as_py(),
                "temperature_c": telemetry.column("temperature_c")[i].as_py(),
                "status": telemetry.column("status")[i].as_py(),
            }
        )

    results = []
    for mid, events in machine_events.items():
        events.sort(key=lambda e: e["timestamp"])
        for j in range(len(events) - 1):
            curr = events[j]
            nxt = events[j + 1]
            if curr["temperature_c"] and curr["temperature_c"] > 85 and nxt["status"] in ("idle", "off", "error"):
                results.append(
                    {
                        "machine_id": mid,
                        "hot_event_temp": round(curr["temperature_c"], 1),
                        "hot_event_time": (
                            curr["timestamp"].isoformat()
                            if hasattr(curr["timestamp"], "isoformat")
                            else str(curr["timestamp"])
                        ),
                        "next_status": nxt["status"],
                        "next_time": (
                            nxt["timestamp"].isoformat()
                            if hasattr(nxt["timestamp"], "isoformat")
                            else str(nxt["timestamp"])
                        ),
                    }
                )
                break

    return results


def frequently_failing_machines(s3: StorageClient) -> list[dict]:
    maint = _read(s3, "analytics/fact_maintenance.parquet")
    if maint is None:
        return []

    counts: dict[str, dict] = {}
    for i in range(len(maint)):
        mid = maint.column("machine_id")[i].as_py()
        if mid not in counts:
            counts[mid] = {"machine_id": mid, "maintenance_events": 0, "total_downtime_min": 0}
        counts[mid]["maintenance_events"] += 1
        dt = maint.column("downtime_minutes")[i].as_py()
        if dt:
            counts[mid]["total_downtime_min"] += dt

    results = sorted(counts.values(), key=lambda r: r["maintenance_events"], reverse=True)
    return results[:10]


def downtime_by_factory(s3: StorageClient) -> list[dict]:
    metrics = _read(s3, "analytics/factory_downtime_metrics.parquet")
    if metrics is None:
        return []

    results = []
    for i in range(len(metrics)):
        results.append(
            {
                "factory_id": metrics.column("factory_id")[i].as_py(),
                "total_downtime_minutes": metrics.column("total_downtime_minutes")[i].as_py(),
                "avg_downtime_minutes": round(metrics.column("avg_downtime_minutes")[i].as_py(), 1),
                "downtime_events": metrics.column("downtime_event_count")[i].as_py(),
            }
        )

    results.sort(key=lambda r: r["total_downtime_minutes"], reverse=True)
    return results


def maintenance_reducing_downtime(s3: StorageClient) -> list[dict]:
    metrics = _read(s3, "analytics/maintenance_effectiveness_metrics.parquet")
    if metrics is None:
        return []

    results = []
    for i in range(len(metrics)):
        results.append(
            {
                "maintenance_type": metrics.column("maintenance_type")[i].as_py(),
                "avg_downtime_minutes": round(metrics.column("avg_downtime_minutes")[i].as_py(), 1),
                "total_downtime_minutes": metrics.column("total_downtime_minutes")[i].as_py(),
                "event_count": metrics.column("event_count")[i].as_py(),
            }
        )

    results.sort(key=lambda r: r["avg_downtime_minutes"])
    return results


def machines_high_scrap(s3: StorageClient) -> list[dict]:
    scrap = _read(s3, "analytics/scrap_rate_metrics.parquet")
    if scrap is None:
        return []

    results = []
    for i in range(len(scrap)):
        results.append(
            {
                "machine_id": scrap.column("machine_id")[i].as_py(),
                "scrap_rate": round(scrap.column("scrap_rate")[i].as_py(), 4),
                "total_scrap_units": scrap.column("total_scrap_units")[i].as_py(),
                "total_units_produced": scrap.column("total_units_produced")[i].as_py(),
            }
        )

    results.sort(key=lambda r: r["scrap_rate"], reverse=True)
    return results


def lines_abnormal_variability(s3: StorageClient) -> list[dict]:
    production = _read(s3, "staging/production/production.parquet")
    if production is None:
        return []

    machine_units: dict[str, list[int]] = {}
    for i in range(len(production)):
        mid = production.column("machine_id")[i].as_py()
        units = production.column("units_produced")[i].as_py()
        if mid not in machine_units:
            machine_units[mid] = []
        machine_units[mid].append(units)

    import math

    results = []
    for mid, units in machine_units.items():
        if len(units) < 2:
            continue
        mean = sum(units) / len(units)
        if mean == 0:
            continue
        variance = sum((x - mean) ** 2 for x in units) / len(units)
        std = math.sqrt(variance)
        cv = std / mean
        results.append(
            {
                "machine_id": mid,
                "mean_units": round(mean, 1),
                "std_units": round(std, 1),
                "cv": round(cv, 4),
                "data_points": len(units),
                "abnormal": cv > 0.15,
            }
        )

    results.sort(key=lambda r: r["cv"], reverse=True)
    return results


ALL_QUERIES = {
    "machines_trending_toward_failure": machines_trending_toward_failure,
    "overheating_before_shutdown": overheating_before_shutdown,
    "frequently_failing_machines": frequently_failing_machines,
    "downtime_by_factory": downtime_by_factory,
    "maintenance_reducing_downtime": maintenance_reducing_downtime,
    "machines_high_scrap": machines_high_scrap,
    "lines_abnormal_variability": lines_abnormal_variability,
}


def run_all_queries() -> dict[str, list[dict]]:
    s3 = get_storage_client()
    logger.info("Running %d analytics queries", len(ALL_QUERIES))

    results = {}
    for name, query_fn in ALL_QUERIES.items():
        try:
            rows = query_fn(s3)
            results[name] = rows
            logger.info("  %-40s → %d row(s)", name, len(rows))
        except Exception:
            logger.exception("Query '%s' failed", name)
            results[name] = []

    return results


if __name__ == "__main__":
    import json

    results = run_all_queries()
    for name, rows in results.items():
        print(f"\n{'=' * 60}")
        print(f"  {name}: {len(rows)} result(s)")
        print(f"{'=' * 60}")
        for row in rows[:5]:
            print(f"  {json.dumps(row, default=str)}")

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("monitoring.alerts")

TEMP_THRESHOLD_C = 85.0
SCRAP_RATE_THRESHOLD = 0.05
TELEMETRY_SILENCE_HOURS = 1.0
LATE_DATA_HOURS = 2.0

ALERTS_KEY = "metadata/alerts/latest.json"


@dataclass
class Alert:
    alert_type: str
    severity: str
    message: str
    details: dict


def _read_parquet(s3: StorageClient, key: str) -> pa.Table | None:
    if not s3.exists(key):
        return None
    data = s3.get_object(key)
    return pq.ParquetFile(pa.BufferReader(data)).read()


def _check_telemetry_silence(s3: StorageClient) -> list[Alert]:
    alerts: list[Alert] = []
    telemetry = _read_parquet(s3, "staging/telemetry/telemetry.parquet")
    machines = _read_parquet(s3, "staging/machines/machines.parquet")
    if telemetry is None or machines is None:
        return alerts

    now = datetime.now(UTC)
    all_machine_ids = set(machines.column("machine_id")[i].as_py() for i in range(len(machines)))

    latest_by_machine: dict[str, datetime] = {}
    for i in range(len(telemetry)):
        mid = telemetry.column("machine_id")[i].as_py()
        ts = telemetry.column("timestamp")[i].as_py()
        if mid not in latest_by_machine or ts > latest_by_machine[mid]:
            latest_by_machine[mid] = ts

    silent_machines = []
    for mid in all_machine_ids:
        if mid not in latest_by_machine:
            silent_machines.append({"machine_id": mid, "last_event": None})
        else:
            last = latest_by_machine[mid]
            if last.tzinfo is None:
                from datetime import timezone

                last = last.replace(tzinfo=timezone.utc)
            hours_since = (now - last).total_seconds() / 3600
            if hours_since > TELEMETRY_SILENCE_HOURS:
                silent_machines.append(
                    {
                        "machine_id": mid,
                        "last_event": last.isoformat(),
                        "hours_since": round(hours_since, 1),
                    }
                )

    if silent_machines:
        alerts.append(
            Alert(
                alert_type="telemetry_silence",
                severity="warning",
                message=f"{len(silent_machines)} machine(s) have not sent telemetry in {TELEMETRY_SILENCE_HOURS}h",
                details={"machines": silent_machines},
            )
        )

    return alerts


def _check_temperature_threshold(s3: StorageClient) -> list[Alert]:
    alerts: list[Alert] = []
    health = _read_parquet(s3, "analytics/machine_health_features.parquet")
    if health is None:
        return alerts

    overheating = []
    for i in range(len(health)):
        max_temp = health.column("max_temperature_c")[i].as_py()
        if max_temp is not None and max_temp > TEMP_THRESHOLD_C:
            overheating.append(
                {
                    "machine_id": health.column("machine_id")[i].as_py(),
                    "max_temperature_c": round(max_temp, 1),
                    "avg_temperature_c": round(health.column("avg_temperature_c")[i].as_py(), 1),
                }
            )

    if overheating:
        severity = "critical" if any(m["max_temperature_c"] > 95 for m in overheating) else "warning"
        alerts.append(
            Alert(
                alert_type="temperature_exceedance",
                severity=severity,
                message=f"{len(overheating)} machine(s) exceeded {TEMP_THRESHOLD_C}°C temperature threshold",
                details={"machines": overheating},
            )
        )

    return alerts


def _check_scrap_rate_spike(s3: StorageClient) -> list[Alert]:
    alerts: list[Alert] = []
    scrap = _read_parquet(s3, "analytics/scrap_rate_metrics.parquet")
    if scrap is None:
        return alerts

    high_scrap = []
    for i in range(len(scrap)):
        rate = scrap.column("scrap_rate")[i].as_py()
        if rate is not None and rate > SCRAP_RATE_THRESHOLD:
            high_scrap.append(
                {
                    "machine_id": scrap.column("machine_id")[i].as_py(),
                    "scrap_rate": round(rate, 4),
                    "total_units": scrap.column("total_units_produced")[i].as_py(),
                    "total_scrap": scrap.column("total_scrap_units")[i].as_py(),
                }
            )

    if high_scrap:
        alerts.append(
            Alert(
                alert_type="scrap_rate_spike",
                severity="warning",
                message=f"{len(high_scrap)} machine(s) exceed {SCRAP_RATE_THRESHOLD * 100:.0f}% scrap rate threshold",
                details={"machines": high_scrap},
            )
        )

    return alerts


def _check_late_data(s3: StorageClient) -> list[Alert]:
    alerts: list[Alert] = []
    now = datetime.now(UTC)

    if s3.exists("metadata/watermarks/maintenance.json"):
        wm = json.loads(s3.get_object("metadata/watermarks/maintenance.json").decode())
        last_ts_str = wm.get("maintenance_logs", "")
        if last_ts_str:
            last_ts = datetime.fromisoformat(last_ts_str)
            if last_ts.tzinfo is None:
                from datetime import timezone

                last_ts = last_ts.replace(tzinfo=timezone.utc)
            hours_old = (now - last_ts).total_seconds() / 3600
            if hours_old > LATE_DATA_HOURS:
                alerts.append(
                    Alert(
                        alert_type="late_data",
                        severity="warning",
                        message=f"Maintenance data is {hours_old:.1f}h old (threshold: {LATE_DATA_HOURS}h)",
                        details={
                            "source": "maintenance",
                            "last_watermark": last_ts_str,
                            "hours_old": round(hours_old, 1),
                        },
                    )
                )

    if s3.exists("metadata/watermarks/production.json"):
        wm = json.loads(s3.get_object("metadata/watermarks/production.json").decode())
        last_date_str = wm.get("last_date", "")
        if last_date_str:
            from datetime import date as _date

            last_date = _date.fromisoformat(last_date_str)
            today = now.date()
            days_old = (today - last_date).days
            if days_old > 0:
                alerts.append(
                    Alert(
                        alert_type="late_data",
                        severity="info" if days_old <= 1 else "warning",
                        message=f"Production data is {days_old} day(s) behind",
                        details={"source": "production", "last_date": last_date_str, "days_old": days_old},
                    )
                )

    return alerts


def _check_pipeline_failures(s3: StorageClient) -> list[Alert]:
    from src.monitoring.run_tracker import get_failed_runs

    alerts: list[Alert] = []
    failures = get_failed_runs(since_hours=24)

    if failures:
        alerts.append(
            Alert(
                alert_type="pipeline_failure",
                severity="critical",
                message=f"{len(failures)} pipeline step(s) failed in the last 24h",
                details={"failures": failures},
            )
        )

    return alerts


def run_alerts() -> list[dict]:
    s3 = get_storage_client()
    logger.info("Running operational alert checks")

    all_alerts: list[Alert] = []
    checks = [
        ("telemetry_silence", _check_telemetry_silence),
        ("temperature_exceedance", _check_temperature_threshold),
        ("scrap_rate_spike", _check_scrap_rate_spike),
        ("late_data", _check_late_data),
        ("pipeline_failures", _check_pipeline_failures),
    ]

    for name, check_fn in checks:
        try:
            results = check_fn(s3)
            all_alerts.extend(results)
            if results:
                for a in results:
                    log_fn = logger.warning if a.severity in ("warning", "critical") else logger.info
                    log_fn("ALERT [%s] %s: %s", a.severity.upper(), a.alert_type, a.message)
            else:
                logger.info("OK    %s: no issues", name)
        except Exception:
            logger.exception("Alert check '%s' failed", name)

    report = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total_alerts": len(all_alerts),
        "critical": sum(1 for a in all_alerts if a.severity == "critical"),
        "warnings": sum(1 for a in all_alerts if a.severity == "warning"),
        "alerts": [asdict(a) for a in all_alerts],
    }
    s3.put_object(ALERTS_KEY, json.dumps(report, indent=2).encode())
    logger.info("Alert report saved → %s (%d alert(s))", ALERTS_KEY, len(all_alerts))

    return [asdict(a) for a in all_alerts]


if __name__ == "__main__":
    results = run_alerts()
    for a in results:
        print(f"  [{a['severity'].upper()}] {a['alert_type']}: {a['message']}")

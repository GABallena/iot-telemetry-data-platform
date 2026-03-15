
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("quality")


class QualityCheckError(Exception):

    def __init__(self, result: QualityResult):
        self.result = result
        super().__init__(
            f"Quality check FAILED [{result.check_name}]: {result.details} "
            f"({result.records_failed}/{result.records_checked} records)"
        )


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    severity: str
    details: str
    records_checked: int
    records_failed: int


def _read_parquet(s3: StorageClient, key: str) -> pa.Table | None:
    if not s3.exists(key):
        return None
    return pq.ParquetFile(pa.BufferReader(s3.get_object(key))).read()


def check_missing_telemetry(s3: StorageClient) -> QualityResult:
    table = _read_parquet(s3, "staging/telemetry/telemetry.parquet")
    if table is None:
        return QualityResult("missing_telemetry", False, "error",
                             "No staged telemetry data found", 0, 0)

    event_ids = [r.as_py() for r in table.column("event_id")]
    nums = sorted(int(eid.replace("evt_", "")) for eid in event_ids if eid)
    if len(nums) < 2:
        return QualityResult("missing_telemetry", True, "info",
                             "Too few events to check gaps", len(nums), 0)

    expected = set(range(nums[0], nums[-1] + 1))
    actual = set(nums)
    missing = sorted(expected - actual)

    if missing:
        sample = missing[:10]
        return QualityResult(
            "missing_telemetry", False, "warning",
            f"{len(missing)} missing event(s) in sequence. Sample: evt_{sample[0]:06d}..evt_{sample[-1]:06d}",
            len(expected), len(missing),
        )
    return QualityResult("missing_telemetry", True, "info",
                         "No gaps in telemetry event sequence",
                         len(actual), 0)


def check_negative_production(s3: StorageClient) -> QualityResult:
    table = _read_parquet(s3, "staging/production/production.parquet")
    if table is None:
        return QualityResult("negative_production", False, "error",
                             "No staged production data found", 0, 0)

    neg_units = pc.sum(pc.less(table.column("units_produced"),
                               pa.scalar(0, type=pa.int64()))).as_py()
    neg_scrap = pc.sum(pc.less(table.column("scrap_units"),
                               pa.scalar(0, type=pa.int64()))).as_py()
    total_neg = neg_units + neg_scrap

    if total_neg > 0:
        return QualityResult(
            "negative_production", False, "error",
            f"{neg_units} negative units_produced, {neg_scrap} negative scrap_units",
            len(table), total_neg,
        )
    return QualityResult("negative_production", True, "info",
                         "No negative production values",
                         len(table), 0)


def check_machine_id_mismatches(s3: StorageClient) -> QualityResult:
    machines = _read_parquet(s3, "staging/machines/machines.parquet")
    if machines is None:
        return QualityResult("machine_id_mismatch", False, "error",
                             "No staged machines data found", 0, 0)

    valid_ids = set(r.as_py() for r in machines.column("machine_id"))
    mismatches: dict[str, list[str]] = {}

    for source, key, col in [
        ("telemetry", "staging/telemetry/telemetry.parquet", "machine_id"),
        ("production", "staging/production/production.parquet", "machine_id"),
        ("maintenance_logs", "staging/maintenance_logs/maintenance_logs.parquet", "machine_id"),
    ]:
        table = _read_parquet(s3, key)
        if table is None:
            continue
        source_ids = set(r.as_py() for r in table.column(col))
        unknown = source_ids - valid_ids
        if unknown:
            mismatches[source] = sorted(unknown)

    if mismatches:
        details = "; ".join(f"{src}: {ids}" for src, ids in mismatches.items())
        total_bad = sum(len(v) for v in mismatches.values())
        return QualityResult("machine_id_mismatch", False, "warning",
                             f"Unknown machine_ids — {details}",
                             len(valid_ids), total_bad)

    return QualityResult("machine_id_mismatch", True, "info",
                         "All machine_ids match the machines dimension",
                         len(valid_ids), 0)


def check_null_critical_fields(s3: StorageClient) -> QualityResult:
    checks = [
        ("staging/telemetry/telemetry.parquet", ["event_id", "machine_id", "timestamp"]),
        ("staging/production/production.parquet", ["date", "machine_id"]),
        ("staging/maintenance_logs/maintenance_logs.parquet", ["maintenance_id", "machine_id"]),
    ]
    total_checked = 0
    total_nulls = 0
    null_details = []

    for key, cols in checks:
        table = _read_parquet(s3, key)
        if table is None:
            continue
        for col in cols:
            null_count = pc.sum(pc.is_null(table.column(col))).as_py()
            total_checked += len(table)
            if null_count > 0:
                total_nulls += null_count
                null_details.append(f"{key}:{col}={null_count}")

    if total_nulls > 0:
        return QualityResult("null_critical_fields", False, "error",
                             f"Nulls found: {', '.join(null_details)}",
                             total_checked, total_nulls)
    return QualityResult("null_critical_fields", True, "info",
                         "No nulls in critical fields",
                         total_checked, 0)


ALL_CHECKS = [
    check_missing_telemetry,
    check_negative_production,
    check_machine_id_mismatches,
    check_null_critical_fields,
]


def run_quality_checks(fail_early: bool = True) -> list[QualityResult]:
    s3 = get_storage_client()
    logger.info("Running data quality checks")

    results: list[QualityResult] = []
    for check_fn in ALL_CHECKS:
        result = check_fn(s3)

        if result.passed:
            logger.info("PASS  %s: %s", result.check_name, result.details)
        elif result.severity == "error":
            logger.error("FAIL  %s: %s  (%d/%d records)",
                         result.check_name, result.details,
                         result.records_failed, result.records_checked)
        else:
            logger.warning("WARN  %s: %s  (%d/%d records)",
                           result.check_name, result.details,
                           result.records_failed, result.records_checked)

        results.append(result)

        if fail_early and not result.passed and result.severity == "error":
            _persist_report(s3, results)
            raise QualityCheckError(result)

    _persist_report(s3, results)

    passed = sum(1 for r in results if r.passed)
    logger.info("Quality checks complete: %d/%d passed", passed, len(results))
    return results


def _persist_report(s3: StorageClient, results: list[QualityResult]) -> None:
    report = {
        "run_timestamp": datetime.now(UTC).isoformat(),
        "checks": [asdict(r) for r in results],
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
        },
    }
    s3.put_object(
        "metadata/quality/latest_report.json",
        json.dumps(report, indent=2),
    )
    logger.info("Report saved → metadata/quality/latest_report.json")
    return results


if __name__ == "__main__":
    run_quality_checks()

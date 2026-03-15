
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("transformations.staging")


def _read_raw_parquet(s3: StorageClient, prefix: str) -> pa.Table | None:
    keys = [k for k in s3.list_objects(prefix) if k.endswith(".parquet")]
    if not keys:
        return None
    tables = []
    for key in keys:
        data = s3.get_object(key)
        reader = pq.ParquetFile(pa.BufferReader(data))
        tables.append(reader.read())
    return pa.concat_tables(tables, promote_options="default")


def _write_staging(s3: StorageClient, key: str, table: pa.Table) -> None:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    s3.put_object(key, sink.getvalue().to_pybytes())


def _normalize_string_col(table: pa.Table, col: str) -> pa.Table:
    if col not in table.column_names:
        return table
    cleaned = pc.utf8_trim(pc.utf8_lower(table.column(col)), " ")
    return table.set_column(table.schema.get_field_index(col), col, cleaned)


def _drop_partition_cols(table: pa.Table) -> pa.Table:
    for col in ("year", "month", "day", "hour"):
        if col in table.column_names:
            idx = table.schema.get_field_index(col)
            table = table.remove_column(idx)
    return table


def _cast_timestamp_col(table: pa.Table, col: str) -> pa.Table:
    if col not in table.column_names:
        return table
    ts = pc.strptime(table.column(col), format="%Y-%m-%dT%H:%M:%S", unit="us")
    return table.set_column(table.schema.get_field_index(col), col, ts)


def _cast_date_col(table: pa.Table, col: str) -> pa.Table:
    if col not in table.column_names:
        return table
    dates = pc.strptime(table.column(col), format="%Y-%m-%d", unit="us")
    date_arr = pc.cast(dates, pa.date32())
    return table.set_column(table.schema.get_field_index(col), col, date_arr)


def _dedup(table: pa.Table, key_col: str) -> pa.Table:
    seen: dict[str, int] = {}
    indices = []
    for i in range(len(table)):
        val = table.column(key_col)[i].as_py()
        seen[val] = i
    indices = sorted(seen.values())
    return table.take(indices)


def _stage_telemetry(s3: StorageClient) -> int:
    table = _read_raw_parquet(s3, "raw/telemetry/")
    if table is None:
        logger.warning("No raw telemetry data found")
        return 0

    table = _drop_partition_cols(table)
    table = _normalize_string_col(table, "machine_id")
    table = _normalize_string_col(table, "factory_id")
    table = _normalize_string_col(table, "status")
    table = _cast_timestamp_col(table, "timestamp")
    table = _dedup(table, "event_id")

    _write_staging(s3, "staging/telemetry/telemetry.parquet", table)
    logger.info("telemetry: %d rows → staging/telemetry/telemetry.parquet", len(table))
    return len(table)


def _stage_machines(s3: StorageClient) -> int:
    table = _read_raw_parquet(s3, "raw/maintenance/machines/")
    if table is None:
        logger.warning("No raw machines data found")
        return 0

    table = _drop_partition_cols(table)
    table = _normalize_string_col(table, "machine_id")
    table = _normalize_string_col(table, "factory_id")
    table = _normalize_string_col(table, "machine_type")
    table = _normalize_string_col(table, "status")
    table = _cast_date_col(table, "install_date")
    table = _dedup(table, "machine_id")

    _write_staging(s3, "staging/machines/machines.parquet", table)
    logger.info("machines: %d rows → staging/machines/machines.parquet", len(table))
    return len(table)


def _stage_maintenance_logs(s3: StorageClient) -> int:
    table = _read_raw_parquet(s3, "raw/maintenance/maintenance_logs/")
    if table is None:
        logger.warning("No raw maintenance_logs data found")
        return 0

    table = _drop_partition_cols(table)
    table = _normalize_string_col(table, "machine_id")
    table = _normalize_string_col(table, "maintenance_type")
    table = _normalize_string_col(table, "part_replaced")
    table = _cast_timestamp_col(table, "timestamp")
    table = _dedup(table, "maintenance_id")

    dt = table.column("downtime_minutes")
    clamped = pc.max_element_wise(dt, pa.scalar(0, type=pa.int64()))
    table = table.set_column(
        table.schema.get_field_index("downtime_minutes"), "downtime_minutes", clamped
    )

    _write_staging(s3, "staging/maintenance_logs/maintenance_logs.parquet", table)
    logger.info("maintenance_logs: %d rows → staging/maintenance_logs/maintenance_logs.parquet", len(table))
    return len(table)


def _stage_production(s3: StorageClient) -> int:
    table = _read_raw_parquet(s3, "raw/production/")
    if table is None:
        logger.warning("No raw production data found")
        return 0

    table = _drop_partition_cols(table)
    table = _normalize_string_col(table, "machine_id")
    table = _normalize_string_col(table, "shift")
    table = _normalize_string_col(table, "operator_id")
    table = _cast_date_col(table, "date")

    for col in ("units_produced", "scrap_units"):
        arr = table.column(col)
        clamped = pc.max_element_wise(arr, pa.scalar(0, type=pa.int64()))
        table = table.set_column(
            table.schema.get_field_index(col), col, clamped
        )

    _write_staging(s3, "staging/production/production.parquet", table)
    logger.info("production: %d rows → staging/production/production.parquet", len(table))
    return len(table)


def run_staging() -> dict:
    s3 = get_storage_client()
    logger.info("Starting staging layer transformations")

    stats = {
        "telemetry": _stage_telemetry(s3),
        "machines": _stage_machines(s3),
        "maintenance_logs": _stage_maintenance_logs(s3),
        "production": _stage_production(s3),
    }

    logger.info("Complete: %s", stats)
    return stats


if __name__ == "__main__":
    run_staging()

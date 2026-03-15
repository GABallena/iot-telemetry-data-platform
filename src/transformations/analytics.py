
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.log import get_logger
from src.storage.client import StorageClient, get_storage_client

logger = get_logger("transformations.analytics")


def _read_staging(s3: StorageClient, key: str) -> pa.Table | None:
    if not s3.exists(key):
        return None
    data = s3.get_object(key)
    return pq.ParquetFile(pa.BufferReader(data)).read()


def _write_analytics(s3: StorageClient, key: str, table: pa.Table) -> None:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    s3.put_object(key, sink.getvalue().to_pybytes())
    logger.info("%d rows → %s", len(table), key)


def _build_fact_machine_events(s3: StorageClient, telemetry: pa.Table, machines: pa.Table) -> None:
    machine_map: dict[str, dict] = {}
    for i in range(len(machines)):
        mid = machines.column("machine_id")[i].as_py()
        machine_map[mid] = {
            "factory_id_dim": machines.column("factory_id")[i].as_py(),
            "machine_type": machines.column("machine_type")[i].as_py(),
        }

    machine_types = []
    for i in range(len(telemetry)):
        mid = telemetry.column("machine_id")[i].as_py()
        info = machine_map.get(mid, {})
        machine_types.append(info.get("machine_type"))

    fact = telemetry.append_column("machine_type", pa.array(machine_types, type=pa.string()))
    _write_analytics(s3, "analytics/fact_machine_events.parquet", fact)


def _build_fact_production(s3: StorageClient, production: pa.Table, machines: pa.Table) -> None:
    machine_map: dict[str, str] = {}
    for i in range(len(machines)):
        mid = machines.column("machine_id")[i].as_py()
        machine_map[mid] = machines.column("factory_id")[i].as_py()

    factory_ids = []
    for i in range(len(production)):
        mid = production.column("machine_id")[i].as_py()
        factory_ids.append(machine_map.get(mid))

    fact = production.append_column("factory_id", pa.array(factory_ids, type=pa.string()))

    units = pc.cast(fact.column("units_produced"), pa.float64())
    scrap = pc.cast(fact.column("scrap_units"), pa.float64())
    safe_units = pc.max_element_wise(units, pa.scalar(1.0))
    scrap_rate = pc.divide(scrap, safe_units)
    fact = fact.append_column("scrap_rate", scrap_rate)

    _write_analytics(s3, "analytics/fact_production.parquet", fact)


def _build_fact_maintenance(s3: StorageClient, maint: pa.Table, machines: pa.Table) -> None:
    machine_map: dict[str, dict] = {}
    for i in range(len(machines)):
        mid = machines.column("machine_id")[i].as_py()
        machine_map[mid] = {
            "factory_id": machines.column("factory_id")[i].as_py(),
            "machine_type": machines.column("machine_type")[i].as_py(),
        }

    factory_ids = []
    machine_types = []
    for i in range(len(maint)):
        mid = maint.column("machine_id")[i].as_py()
        info = machine_map.get(mid, {})
        factory_ids.append(info.get("factory_id"))
        machine_types.append(info.get("machine_type"))

    fact = maint.append_column("factory_id", pa.array(factory_ids, type=pa.string()))
    fact = fact.append_column("machine_type", pa.array(machine_types, type=pa.string()))

    _write_analytics(s3, "analytics/fact_maintenance.parquet", fact)


def _build_fact_downtime(s3: StorageClient, maint: pa.Table, machines: pa.Table) -> None:
    mask = pc.greater(maint.column("downtime_minutes"), pa.scalar(0, type=pa.int64()))
    downtime = maint.filter(mask)

    machine_map: dict[str, str] = {}
    for i in range(len(machines)):
        mid = machines.column("machine_id")[i].as_py()
        machine_map[mid] = machines.column("factory_id")[i].as_py()

    factory_ids = []
    for i in range(len(downtime)):
        mid = downtime.column("machine_id")[i].as_py()
        factory_ids.append(machine_map.get(mid))

    fact = downtime.append_column("factory_id", pa.array(factory_ids, type=pa.string()))
    _write_analytics(s3, "analytics/fact_downtime.parquet", fact)


def _group_by_agg(table: pa.Table, group_col: str, agg_specs: list[tuple[str, str, str]]) -> pa.Table:
    table = table.group_by(group_col).aggregate(
        [(src, agg) for src, agg, _ in agg_specs]
    )
    rename_map = {}
    for src, agg, out_name in agg_specs:
        auto_name = f"{src}_{agg}"
        rename_map[auto_name] = out_name
    names = [rename_map.get(n, n) for n in table.column_names]
    return table.rename_columns(names)


def _build_machine_health_features(s3: StorageClient, telemetry: pa.Table) -> None:
    features = telemetry.group_by("machine_id").aggregate([
        ("temperature_c", "mean"),
        ("temperature_c", "max"),
        ("vibration_mm_s", "mean"),
        ("vibration_mm_s", "max"),
        ("power_kw", "mean"),
        ("event_id", "count"),
    ])
    features = features.rename_columns([
        "machine_id",
        "avg_temperature_c", "max_temperature_c",
        "avg_vibration_mm_s", "max_vibration_mm_s",
        "avg_power_kw", "event_count",
    ])
    _write_analytics(s3, "analytics/machine_health_features.parquet", features)


def _build_factory_downtime_metrics(s3: StorageClient, downtime_fact: pa.Table) -> None:
    if downtime_fact is None or len(downtime_fact) == 0:
        logger.warning("No downtime data for factory metrics")
        return

    metrics = downtime_fact.group_by("factory_id").aggregate([
        ("downtime_minutes", "sum"),
        ("downtime_minutes", "mean"),
        ("downtime_minutes", "count"),
        ("downtime_minutes", "max"),
    ])
    metrics = metrics.rename_columns([
        "factory_id",
        "total_downtime_minutes", "avg_downtime_minutes",
        "downtime_event_count", "max_single_downtime_minutes",
    ])
    _write_analytics(s3, "analytics/factory_downtime_metrics.parquet", metrics)


def _build_scrap_rate_metrics(s3: StorageClient, production: pa.Table, machines: pa.Table) -> None:
    machine_map: dict[str, str] = {}
    for i in range(len(machines)):
        mid = machines.column("machine_id")[i].as_py()
        machine_map[mid] = machines.column("factory_id")[i].as_py()

    factory_ids = []
    for i in range(len(production)):
        mid = production.column("machine_id")[i].as_py()
        factory_ids.append(machine_map.get(mid))

    enriched = production.append_column("factory_id", pa.array(factory_ids, type=pa.string()))

    metrics = enriched.group_by("machine_id").aggregate([
        ("units_produced", "sum"),
        ("scrap_units", "sum"),
    ])
    metrics = metrics.rename_columns([
        "machine_id", "total_units_produced", "total_scrap_units",
    ])
    total_units = pc.cast(metrics.column("total_units_produced"), pa.float64())
    total_scrap = pc.cast(metrics.column("total_scrap_units"), pa.float64())
    safe_units = pc.max_element_wise(total_units, pa.scalar(1.0))
    scrap_rate = pc.divide(total_scrap, safe_units)
    metrics = metrics.append_column("scrap_rate", scrap_rate)

    _write_analytics(s3, "analytics/scrap_rate_metrics.parquet", metrics)


def _build_maintenance_effectiveness(s3: StorageClient, maint: pa.Table) -> None:
    metrics = maint.group_by("maintenance_type").aggregate([
        ("downtime_minutes", "mean"),
        ("downtime_minutes", "sum"),
        ("downtime_minutes", "count"),
    ])
    metrics = metrics.rename_columns([
        "maintenance_type",
        "avg_downtime_minutes", "total_downtime_minutes", "event_count",
    ])
    _write_analytics(s3, "analytics/maintenance_effectiveness_metrics.parquet", metrics)


def run_analytics() -> dict:
    s3 = get_storage_client()
    logger.info("Starting analytics layer build")

    telemetry = _read_staging(s3, "staging/telemetry/telemetry.parquet")
    machines = _read_staging(s3, "staging/machines/machines.parquet")
    maint = _read_staging(s3, "staging/maintenance_logs/maintenance_logs.parquet")
    production = _read_staging(s3, "staging/production/production.parquet")

    tables_built = []

    if telemetry is not None and machines is not None:
        _build_fact_machine_events(s3, telemetry, machines)
        tables_built.append("fact_machine_events")

    if production is not None and machines is not None:
        _build_fact_production(s3, production, machines)
        tables_built.append("fact_production")

    if maint is not None and machines is not None:
        _build_fact_maintenance(s3, maint, machines)
        tables_built.append("fact_maintenance")

        _build_fact_downtime(s3, maint, machines)
        tables_built.append("fact_downtime")

    if telemetry is not None:
        _build_machine_health_features(s3, telemetry)
        tables_built.append("machine_health_features")

    if maint is not None and machines is not None:
        downtime = _read_staging(s3, "analytics/fact_downtime.parquet")
        if downtime is None:
            downtime = _read_staging(s3, "analytics/fact_downtime.parquet")
        dt_key = "analytics/fact_downtime.parquet"
        dt_data = s3.get_object(dt_key) if s3.exists(dt_key) else None
        if dt_data:
            dt_table = pq.ParquetFile(pa.BufferReader(dt_data)).read()
            _build_factory_downtime_metrics(s3, dt_table)
            tables_built.append("factory_downtime_metrics")

    if production is not None and machines is not None:
        _build_scrap_rate_metrics(s3, production, machines)
        tables_built.append("scrap_rate_metrics")

    if maint is not None:
        _build_maintenance_effectiveness(s3, maint)
        tables_built.append("maintenance_effectiveness_metrics")

    logger.info("Complete: %d tables built", len(tables_built))
    logger.info("Tables: %s", tables_built)
    return {"tables": tables_built}


if __name__ == "__main__":
    run_analytics()

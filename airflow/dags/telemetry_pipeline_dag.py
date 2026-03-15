
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SCHEDULE = os.getenv("PIPELINE_SCHEDULE", "@hourly")


def _timed(step_name, fn, **kwargs):
    from src.monitoring.run_tracker import record_run

    t0 = time.time()
    try:
        result = fn(**kwargs)
        rows = 0
        if isinstance(result, dict):
            rows = result.get("rows", result.get("total_events", 0))
        record_run(step_name, status="success", rows=rows,
                   duration_seconds=time.time() - t0)
        return result
    except Exception as exc:
        record_run(step_name, status="failure",
                   duration_seconds=time.time() - t0, error=str(exc))
        raise


def task_ingest_telemetry(**context):
    from src.ingestion.telemetry_stream import ingest_telemetry_stream
    return _timed("ingestion.telemetry", ingest_telemetry_stream)


def task_ingest_maintenance(**context):
    from src.ingestion.batch_maintenance import ingest_maintenance
    return _timed("ingestion.maintenance", ingest_maintenance)


def task_ingest_production(**context):
    from src.ingestion.batch_production import ingest_production
    return _timed("ingestion.production", ingest_production)


def task_staging(**context):
    from src.transformations.staging import run_staging
    return _timed("transformations.staging", run_staging)


def task_analytics(**context):
    from src.transformations.analytics import run_analytics
    return _timed("transformations.analytics", run_analytics)


def task_quality(**context):
    from src.quality.checks import run_quality_checks
    return _timed("quality.checks", run_quality_checks, fail_early=False)


def task_alerts(**context):
    from src.monitoring.alerts import run_alerts
    return _timed("monitoring.alerts", run_alerts)


def task_queries(**context):
    from src.warehouse.queries import run_all_queries
    return _timed("warehouse.queries", run_all_queries)


with DAG(
    dag_id="telemetry_pipeline",
    default_args=default_args,
    description="Manufacturing IoT telemetry data pipeline",
    schedule_interval=SCHEDULE,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["telemetry", "manufacturing", "iot"],
) as dag:

    ingest_telemetry = PythonOperator(
        task_id="ingest_telemetry",
        python_callable=task_ingest_telemetry,
    )

    ingest_maintenance = PythonOperator(
        task_id="ingest_maintenance",
        python_callable=task_ingest_maintenance,
    )

    ingest_production = PythonOperator(
        task_id="ingest_production",
        python_callable=task_ingest_production,
    )

    staging = PythonOperator(
        task_id="run_staging",
        python_callable=task_staging,
    )

    analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=task_analytics,
    )

    quality = PythonOperator(
        task_id="run_quality_checks",
        python_callable=task_quality,
    )

    alerts = PythonOperator(
        task_id="run_alerts",
        python_callable=task_alerts,
    )

    queries = PythonOperator(
        task_id="run_queries",
        python_callable=task_queries,
    )

    [ingest_telemetry, ingest_maintenance, ingest_production] >> staging
    staging >> analytics >> quality >> alerts >> queries

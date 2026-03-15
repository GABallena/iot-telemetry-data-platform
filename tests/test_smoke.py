from src.storage.client import get_storage_client
from src.storage.seed import seed_all


def test_full_pipeline_smoke():
    seed_all()
    s3 = get_storage_client()
    assert len(s3.list_objects("raw/telemetry/")) > 0

    from src.ingestion.telemetry_stream import ingest_telemetry_stream

    result = ingest_telemetry_stream()
    assert result["total_events"] > 0

    from src.ingestion.batch_maintenance import ingest_maintenance

    result = ingest_maintenance()
    assert result["machines_rows"] > 0

    from src.ingestion.batch_production import ingest_production

    ingest_production()

    from src.transformations.staging import run_staging

    stats = run_staging()
    assert stats["telemetry"] > 0
    assert stats["machines"] > 0

    from src.transformations.analytics import run_analytics

    analytics = run_analytics()
    assert len(analytics["tables"]) >= 4

    from src.quality.checks import run_quality_checks

    results = run_quality_checks(fail_early=False)
    assert len(results) >= 4

    from src.monitoring.alerts import run_alerts

    run_alerts()
    assert s3.exists("metadata/alerts/latest.json")

    from src.warehouse.queries import run_all_queries

    queries = run_all_queries()
    assert len(queries) >= 7

    for key in [
        "staging/telemetry/telemetry.parquet",
        "staging/machines/machines.parquet",
        "analytics/fact_machine_events.parquet",
        "analytics/machine_health_features.parquet",
        "analytics/scrap_rate_metrics.parquet",
    ]:
        assert s3.exists(key), f"Expected output missing: {key}"

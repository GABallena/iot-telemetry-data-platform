#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
import time

from src.log import get_logger
from src.monitoring.run_tracker import record_run

logger = get_logger("orchestrator")


def _run_step(name: str, fn, **kwargs) -> dict | list | None:
    logger.info("▶ Starting step: %s", name)
    t0 = time.time()
    try:
        result = fn(**kwargs)
        elapsed = time.time() - t0
        rows = 0
        if isinstance(result, dict):
            rows = result.get("rows", result.get("total_events", 0))
        record_run(name, status="success", rows=rows, duration_seconds=elapsed)
        logger.info("✓ Completed step: %s (%.1fs)", name, elapsed)
        return result
    except Exception as exc:
        elapsed = time.time() - t0
        record_run(name, status="failure", duration_seconds=elapsed, error=str(exc))
        logger.error("✗ Step '%s' failed after %.1fs: %s", name, elapsed, exc)
        raise


STEPS = {}


def _register_steps():
    from src.ingestion.batch_maintenance import ingest_maintenance
    from src.ingestion.batch_production import ingest_production
    from src.ingestion.telemetry_stream import ingest_telemetry_stream
    from src.monitoring.alerts import run_alerts
    from src.quality.checks import run_quality_checks
    from src.transformations.analytics import run_analytics
    from src.transformations.staging import run_staging
    from src.warehouse.queries import run_all_queries

    STEPS.update({
        "ingest": [
            ("ingestion.telemetry", ingest_telemetry_stream, {}),
            ("ingestion.maintenance", ingest_maintenance, {}),
            ("ingestion.production", ingest_production, {}),
        ],
        "staging": [("transformations.staging", run_staging, {})],
        "analytics": [("transformations.analytics", run_analytics, {})],
        "quality": [("quality.checks", run_quality_checks, {"fail_early": False})],
        "alerts": [("monitoring.alerts", run_alerts, {})],
        "queries": [("warehouse.queries", run_all_queries, {})],
    })


STEP_ORDER = ["ingest", "staging", "analytics", "quality", "alerts", "queries"]


def run_pipeline(step: str | None = None) -> None:
    _register_steps()

    steps_to_run = STEP_ORDER if step is None else [step]

    logger.info("=" * 60)
    logger.info("Pipeline run: steps=%s", steps_to_run)
    logger.info("=" * 60)

    t0 = time.time()
    failed = []

    for step_name in steps_to_run:
        if step_name not in STEPS:
            logger.error("Unknown step: %s (valid: %s)", step_name, STEP_ORDER)
            sys.exit(1)

        for task_name, fn, kwargs in STEPS[step_name]:
            try:
                _run_step(task_name, fn, **kwargs)
            except Exception:
                failed.append(task_name)

    elapsed = time.time() - t0
    logger.info("=" * 60)
    if failed:
        logger.error("Pipeline finished with %d failure(s): %s  (%.1fs)", len(failed), failed, elapsed)
        sys.exit(1)
    else:
        logger.info("Pipeline finished successfully (%.1fs)", elapsed)
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Run the telemetry pipeline")
    parser.add_argument(
        "--step",
        choices=STEP_ORDER,
        default=None,
        help="Run a single step (default: run all)",
    )
    args = parser.parse_args()
    run_pipeline(step=args.step)


if __name__ == "__main__":
    main()

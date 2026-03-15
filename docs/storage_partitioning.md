# Storage Partitioning Strategy

## Overview

All data in the lake is stored as **Apache Parquet** files in an
S3-compatible object store (MinIO in production, local filesystem for
development). The lake follows a three-layer architecture with
Hive-style partitioning for time-series data.

## Layers

| Layer | Path Prefix | Purpose | Retention |
|-------|------------|---------|-----------|
| Raw | `raw/` | Immutable source data | Indefinite |
| Staging | `staging/` | Cleaned, normalized | Overwritten each run |
| Analytics | `analytics/` | Fact & feature tables | Overwritten each run |
| Metadata | `metadata/` | Watermarks, quality, lineage, runs | Append-only |

## Partitioning Schemes

### Telemetry (high-volume)

```
raw/telemetry/year=2026/month=03/day=15/hour=08/batch_001.parquet
raw/telemetry/year=2026/month=03/day=15/hour=09/batch_001.parquet
```

Partitioned by **year / month / day / hour** because:
- Telemetry arrives continuously — hourly partitions keep file sizes manageable
- Time-range queries (e.g., "last 24 hours") can prune entire partitions
- Partition granularity matches the micro-batch ingestion window

### Maintenance (medium-volume)

```
raw/maintenance/machines/snapshot_20260315_083000.parquet
raw/maintenance/maintenance_logs/year=2026/month=03/batch_20260315.parquet
```

- **Machines:** Full snapshots with timestamp suffix (slowly changing dimension)
- **Maintenance logs:** Partitioned by **year / month** — logs arrive less
  frequently than telemetry

### Production (low-volume)

```
raw/production/year=2026/month=03/batch_20260315.parquet
```

Partitioned by **year / month** — one CSV per day, monthly partitions are
sufficient.

## Staging & Analytics

Staging and analytics tables are stored as single consolidated Parquet files:

```
staging/telemetry/telemetry.parquet
staging/machines/machines.parquet
analytics/fact_machine_events.parquet
analytics/machine_health_features.parquet
```

No partitioning at these layers because:
- Data volume after aggregation fits comfortably in single files
- Simplifies query logic (no partition discovery needed)
- Tables are fully rewritten each pipeline run

## Metadata

```
metadata/watermarks/maintenance.json     # last-ingested timestamp
metadata/watermarks/production.json      # last-ingested date
metadata/quality/latest_report.json      # most recent quality check
metadata/lineage/events.jsonl            # append-only lineage log
metadata/runs/history.jsonl              # append-only run history
metadata/alerts/latest.json             # most recent alert results
```

Watermarks are small JSON files enabling incremental ingestion.
Lineage and run history use JSONL for append-friendly writes.

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Partition keys | `key=value/` (Hive-style) | `year=2026/month=03/` |
| Batch files | `batch_YYYYMMDD.parquet` | `batch_20260315.parquet` |
| Snapshots | `snapshot_YYYYMMDD_HHMMSS.parquet` | `snapshot_20260315_083000.parquet` |
| Analytics tables | `{table_name}.parquet` | `fact_production.parquet` |

## Design Decisions

1. **Parquet over CSV/JSON** — columnar format gives compression (typically
   5–10× smaller than CSV) and predicate pushdown for analytical queries.

2. **Hive-style partitioning** — compatible with Spark, Athena, DuckDB, and
   PyArrow's `ParquetDataset` reader for partition pruning.

3. **Hourly telemetry partitions** — balances file count (not too many small
   files) against query granularity (can scan a single hour).

4. **Immutable raw layer** — raw data is never modified after write. Staging
   and analytics can be regenerated from raw at any time.

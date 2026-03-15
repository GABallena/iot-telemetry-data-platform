# Data Flow

## Pipeline Stages

The pipeline executes six stages in sequence. Each stage reads from the
previous layer and writes to the next.

### 1. Seed — Bootstrap the Raw Layer

**Module:** `src/storage/seed.py`

Copies sample data from `data/sample/` into the data lake
raw layer with time-based partitioning:

```
data/sample/
  telemetry_events.jsonl  →  raw/telemetry/year=YYYY/month=MM/day=DD/hour=HH/*.parquet
  machines.csv            →  raw/maintenance/machines/snapshot_YYYYMMDD_HHMMSS.parquet
  maintenance_logs.csv    →  raw/maintenance/maintenance_logs/year=YYYY/month=MM/*.parquet
  daily_production.csv    →  raw/production/year=YYYY/month=MM/*.parquet
```

### 2. Ingestion — Incremental Data Loading

Three parallel ingestion paths, all watermark-driven to avoid re-processing:

| Ingester | Source | Strategy | Output |
|----------|--------|----------|--------|
| `telemetry_stream.py` | JSONL file | Micro-batch by hour | `raw/telemetry/year=.../hour=.../` |
| `batch_maintenance.py` | Postgres (simulated) | Timestamp watermark | `raw/maintenance/.../year=.../month=.../` |
| `batch_production.py` | CSV | Date watermark | `raw/production/year=.../month=.../` |

Watermarks are persisted as JSON in `metadata/watermarks/`.

### 3. Staging — Clean & Normalize

**Module:** `src/transformations/staging.py`

Reads raw Parquet from all partitions, applies cleaning rules, writes single
consolidated Parquet files per dataset:

- **Telemetry:** Cast types, drop nulls, normalize `machine_id` to uppercase
- **Machines:** Standardize column names, validate enums
- **Maintenance:** Parse dates, unify status labels
- **Production:** Ensure non-negative quantities, parse dates

Output: `staging/{dataset}/{dataset}.parquet`

### 4. Analytics — Fact & Feature Tables

**Module:** `src/transformations/analytics.py`

Builds eight analytics tables from staging data:

**Fact Tables:**
- `fact_machine_events` — one row per telemetry reading with health flags
- `fact_production` — daily production with scrap rate
- `fact_maintenance` — maintenance events with duration
- `fact_downtime` — machine downtime periods

**Feature / Metric Tables:**
- `machine_health_features` — temperature/vibration stats per machine
- `factory_downtime_metrics` — downtime aggregated by factory
- `scrap_rate_metrics` — scrap rates by line and product
- `maintenance_effectiveness_metrics` — maintenance impact on downtime

Output: `analytics/{table_name}.parquet`

### 5. Quality — Validation & Lineage

**Module:** `src/quality/checks.py`, `src/quality/lineage.py`

Four quality checks run against staging and analytics data:

1. **Telemetry gaps** — detect machines with missing event windows
2. **Negative production** — validate no negative quantities
3. **Machine ID mismatches** — cross-reference IDs across datasets
4. **Row count thresholds** — verify minimum expected rows

Results are written to `metadata/quality/latest_report.json`.  
Lineage events are appended to `metadata/lineage/events.jsonl`.

If `fail_early=True` (default), the pipeline raises `QualityCheckError` on
the first critical failure.

### 6. Monitoring — Alerts & Run Tracking

**Module:** `src/monitoring/alerts.py`, `src/monitoring/run_tracker.py`

Five alert checks:
1. Telemetry silence (machine stopped reporting)
2. Temperature exceedance (above threshold)
3. Scrap rate spike (above historical norm)
4. Late data arrival (past expected window)
5. Pipeline job failures (from run history)

Run metadata (start time, duration, status, error) is logged to
`metadata/runs/history.jsonl`.

## Data Flow Diagram

```
Sources ──▶ Ingestion ──▶ Raw Layer ──▶ Staging ──▶ Analytics
                                                        │
                                      Quality ◀─────────┤
                                      Monitoring ◀──────┤
                                      Dashboard ◀───────┘
```

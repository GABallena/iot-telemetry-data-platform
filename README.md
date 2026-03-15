# Manufacturing IoT Telemetry Pipeline

A data platform that ingests, transforms, validates, and serves manufacturing
IoT telemetry alongside production and maintenance data. Built with Python,
PyArrow, and MinIO.

## Quick Start

```bash
# Clone and set up
git clone <repo-url> && cd telemetry-pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# Run the full pipeline (local storage)
python run_pipeline.py

# Launch the dashboard
python -m dashboard.app
# Open http://localhost:8050
```

## Docker (Full Stack)

```bash
docker compose up --build
```

This starts four services:

| Service | Port | Purpose |
|---------|------|---------|
| MinIO | 9000 / 9001 | S3-compatible object storage |
| Postgres | 5432 | Maintenance data warehouse |
| Pipeline | — | Runs ingestion → analytics → quality → monitoring |
| Dashboard | 8050 | Browser-based KPI dashboard |

## Pipeline Stages

```
run_pipeline.py --step <stage>
```

| Stage | Description |
|-------|-------------|
| `seed` | Bootstrap raw layer from sample data |
| `ingest` | Incremental ingestion (telemetry, maintenance, production) |
| `stage` | Clean and normalize into staging layer |
| `analytics` | Build fact and feature tables |
| `quality` | Run 4 quality checks, record lineage |
| `monitor` | Execute 5 alert checks, log run metadata |

Run all stages in order: `python run_pipeline.py` (no `--step` flag).

## Project Structure

```
telemetry-pipeline/
├── config/                 # Settings (storage backend, MinIO config)
├── src/
│   ├── ingestion/          # Telemetry stream, batch maintenance/production
│   ├── transformations/    # Staging and analytics builders
│   ├── storage/            # S3 client abstraction (local + MinIO)
│   ├── quality/            # Data quality checks and lineage
│   ├── monitoring/         # Alerts and run tracking
│   └── warehouse/          # Business analytics queries
├── dashboard/              # HTTP dashboard server
├── airflow/dags/           # Airflow DAG definition
├── tests/                  # Unit and smoke tests
├── sql/                    # Postgres schema (init.sql)
├── terraform/              # Infrastructure-as-code
├── data/sample/            # Sample IoT manufacturing data
├── docs/                   # Architecture, data flow, partitioning docs
├── data_lake/              # Generated data (gitignored)
├── Dockerfile
├── docker-compose.yml
├── run_pipeline.py         # CLI orchestrator
├── requirements.txt
└── pyproject.toml          # Linting config (ruff)
```

## Data Lake Layout

Three-layer architecture on S3-compatible storage:

- **Raw** — immutable, partitioned by time (`raw/telemetry/year=.../hour=.../`)
- **Staging** — cleaned and normalized (`staging/{dataset}/`)
- **Analytics** — fact tables and feature tables (`analytics/`)

See [docs/storage_partitioning.md](docs/storage_partitioning.md) for details.

## Architecture

See [docs/architecture.md](docs/architecture.md) for a full Mermaid diagram
and component descriptions.

## Development

```bash
# Lint
ruff check .

# Test
pytest tests/ -v

# Run a single pipeline stage
python run_pipeline.py --step quality
```

## CI/CD

GitHub Actions runs on every push (`.github/workflows/ci.yml`):
1. **Lint** — `ruff check`
2. **Unit tests** — `pytest tests/ -k "not smoke"`
3. **Smoke test** — full pipeline end-to-end
4. **Deploy** — gated by `DEPLOY_ENABLED` repository variable

## Infrastructure

- **Docker Compose** — local development stack
- **Terraform** — container orchestration via Docker provider
- **MinIO** — S3-compatible object storage
- **Postgres** — relational store for maintenance data

## Configuration

Set `STORAGE_BACKEND=minio` to use MinIO instead of local filesystem.
MinIO credentials are configured via environment variables:

| Variable | Default |
|----------|---------|
| `STORAGE_BACKEND` | `local` |
| `MINIO_ENDPOINT` | `localhost:9000` |
| `MINIO_ACCESS_KEY` | `minioadmin` |
| `MINIO_SECRET_KEY` | `minioadmin` |
| `MINIO_BUCKET` | `telemetry-lake` |

## License

MIT

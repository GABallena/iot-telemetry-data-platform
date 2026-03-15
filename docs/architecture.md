# Architecture Diagram

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        JSONL["Telemetry JSONL\n(IoT sensors)"]
        CSV["Production CSV\n(daily output)"]
        PG["Postgres\n(machines, maintenance,\nparts, downtime)"]
    end

    subgraph Ingestion["Ingestion Layer"]
        TS["telemetry_stream.py\n(micro-batch)"]
        BM["batch_maintenance.py\n(watermark-based)"]
        BP["batch_production.py\n(watermark-based)"]
    end

    subgraph DataLake["Data Lake (MinIO / Local S3)"]
        RAW["Raw Layer\nPartitioned Parquet"]
        STG["Staging Layer\nCleaned Parquet"]
        ANA["Analytics Layer\nFact & Feature Tables"]
    end

    subgraph Quality["Quality & Lineage"]
        QC["Quality Checks\n(4 validations)"]
        LIN["Lineage Tracker\n(JSONL log)"]
    end

    subgraph Monitoring["Monitoring"]
        RT["Run Tracker\n(history.jsonl)"]
        AL["Alert Engine\n(5 checks)"]
    end

    subgraph Serving["Serving"]
        WH["Warehouse Queries\n(7 business questions)"]
        DASH["Dashboard\n(HTTP server, port 8050)"]
    end

    subgraph Orchestration["Orchestration"]
        AF["Airflow DAG"]
        CLI["run_pipeline.py\n(CLI orchestrator)"]
    end

    subgraph Infra["Infrastructure"]
        DC["docker-compose.yml\n(4 services)"]
        TF["Terraform\n(IaC)"]
        CI["GitHub Actions\n(CI/CD)"]
    end

    JSONL --> TS
    CSV --> BP
    PG --> BM

    TS --> RAW
    BM --> RAW
    BP --> RAW

    RAW -->|staging.py| STG
    STG -->|analytics.py| ANA

    STG --> QC
    ANA --> QC
    QC --> LIN

    ANA --> WH
    ANA --> AL
    WH --> DASH

    CLI --> TS & BM & BP
    CLI --> STG
    CLI --> ANA
    CLI --> QC
    CLI --> AL
    CLI --> RT

    AF -.->|"scheduled"| CLI

    DC --> Infra
    TF --> Infra
    CI --> Infra
```

## Component Summary

| Component | Purpose | Technology |
|-----------|---------|------------|
| Ingestion | Ingest raw data from 3 sources | Python, PyArrow |
| Data Lake | 3-layer storage (raw/staging/analytics) | MinIO (S3-compatible), Parquet |
| Transformations | Clean, normalize, build fact/feature tables | PyArrow |
| Quality | Validate data, track lineage | Custom checks, JSONL log |
| Monitoring | Track runs, fire alerts | JSONL history, threshold checks |
| Warehouse | Answer 7 business questions | PyArrow in-memory queries |
| Dashboard | Visual summary of KPIs | Python HTTP server, HTML/CSS |
| Orchestration | Schedule and run pipeline stages | Airflow DAG, CLI |
| Infrastructure | Container deployment, IaC | Docker, Terraform, GitHub Actions |

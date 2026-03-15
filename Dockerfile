FROM python:3.12-slim

LABEL maintainer="data-engineering"
LABEL description="Manufacturing IoT Telemetry Pipeline"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY src/ src/
COPY dashboard/ dashboard/
COPY airflow/ airflow/
COPY run_pipeline.py .
COPY data/sample/ data/sample/

RUN mkdir -p data_lake logs

ENV PYTHONUNBUFFERED=1
ENV STORAGE_BACKEND=minio
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV MINIO_BUCKET=telemetry-lake

CMD ["python", "run_pipeline.py"]


resource "docker_network" "pipeline_net" {
  name = "telemetry-pipeline-net"
}

resource "docker_image" "minio" {
  name         = "minio/minio:latest"
  keep_locally = true
}

resource "docker_volume" "minio_data" {
  name = "telemetry-minio-data"
}

resource "docker_container" "minio" {
  name  = "telemetry-minio"
  image = docker_image.minio.image_id

  command = ["server", "/data", "--console-address", ":9001"]

  env = [
    "MINIO_ROOT_USER=${var.minio_root_user}",
    "MINIO_ROOT_PASSWORD=${var.minio_root_password}",
  ]

  ports {
    internal = 9000
    external = 9000
  }
  ports {
    internal = 9001
    external = 9001
  }

  volumes {
    volume_name    = docker_volume.minio_data.name
    container_path = "/data"
  }

  networks_advanced {
    name = docker_network.pipeline_net.id
  }

  healthcheck {
    test         = ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
    interval     = "10s"
    timeout      = "5s"
    retries      = 5
    start_period = "5s"
  }
}

resource "docker_image" "postgres" {
  name         = "postgres:16-alpine"
  keep_locally = true
}

resource "docker_volume" "pg_data" {
  name = "telemetry-pg-data"
}

resource "docker_container" "postgres" {
  name  = "telemetry-postgres"
  image = docker_image.postgres.image_id

  env = [
    "POSTGRES_DB=${var.postgres_db}",
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
  ]

  ports {
    internal = 5432
    external = 5432
  }

  volumes {
    volume_name    = docker_volume.pg_data.name
    container_path = "/var/lib/postgresql/data"
  }

  networks_advanced {
    name = docker_network.pipeline_net.id
  }

  healthcheck {
    test         = ["CMD-SHELL", "pg_isready -U ${var.postgres_user} -d ${var.postgres_db}"]
    interval     = "10s"
    timeout      = "5s"
    retries      = 5
    start_period = "10s"
  }
}

resource "docker_image" "pipeline" {
  name = "telemetry-pipeline:latest"
  build {
    context    = "${path.module}/.."
    dockerfile = "Dockerfile"
  }
}

resource "docker_volume" "pipeline_logs" {
  name = "telemetry-pipeline-logs"
}

resource "docker_container" "pipeline" {
  name  = "telemetry-pipeline"
  image = docker_image.pipeline.image_id

  depends_on = [
    docker_container.minio,
    docker_container.postgres,
  ]

  env = [
    "STORAGE_BACKEND=minio",
    "MINIO_ENDPOINT=telemetry-minio:9000",
    "MINIO_ACCESS_KEY=${var.minio_root_user}",
    "MINIO_SECRET_KEY=${var.minio_root_password}",
    "MINIO_BUCKET=${var.minio_bucket}",
    "LOG_LEVEL=INFO",
  ]

  volumes {
    volume_name    = docker_volume.pipeline_logs.name
    container_path = "/app/logs"
  }

  networks_advanced {
    name = docker_network.pipeline_net.id
  }
}

resource "docker_container" "dashboard" {
  name  = "telemetry-dashboard"
  image = docker_image.pipeline.image_id

  command = ["python", "-m", "dashboard.app"]

  depends_on = [
    docker_container.pipeline,
  ]

  env = [
    "STORAGE_BACKEND=minio",
    "MINIO_ENDPOINT=telemetry-minio:9000",
    "MINIO_ACCESS_KEY=${var.minio_root_user}",
    "MINIO_SECRET_KEY=${var.minio_root_password}",
    "MINIO_BUCKET=${var.minio_bucket}",
    "DASHBOARD_PORT=8050",
  ]

  ports {
    internal = 8050
    external = var.dashboard_port
  }

  networks_advanced {
    name = docker_network.pipeline_net.id
  }
}

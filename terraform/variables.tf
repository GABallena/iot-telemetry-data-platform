variable "docker_host" {
  description = "Docker daemon socket"
  type        = string
  default     = "unix:///var/run/docker.sock"
}

variable "minio_root_user" {
  description = "MinIO root username"
  type        = string
  default     = "minioadmin"
}

variable "minio_root_password" {
  description = "MinIO root password"
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "postgres_user" {
  description = "PostgreSQL username"
  type        = string
  default     = "pipeline"
}

variable "postgres_password" {
  description = "PostgreSQL password"
  type        = string
  default     = "pipeline"
  sensitive   = true
}

variable "postgres_db" {
  description = "PostgreSQL database name"
  type        = string
  default     = "telemetry"
}

variable "minio_bucket" {
  description = "S3 bucket name for the data lake"
  type        = string
  default     = "telemetry-lake"
}

variable "dashboard_port" {
  description = "Host port for the dashboard"
  type        = number
  default     = 8050
}

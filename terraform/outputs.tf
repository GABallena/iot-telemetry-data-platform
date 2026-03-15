output "minio_api_url" {
  description = "MinIO S3 API endpoint"
  value       = "http://localhost:9000"
}

output "minio_console_url" {
  description = "MinIO web console"
  value       = "http://localhost:9001"
}

output "postgres_url" {
  description = "PostgreSQL connection string"
  value       = "postgresql://${var.postgres_user}:${var.postgres_password}@localhost:5432/${var.postgres_db}"
  sensitive   = true
}

output "dashboard_url" {
  description = "Dashboard URL"
  value       = "http://localhost:${var.dashboard_port}"
}

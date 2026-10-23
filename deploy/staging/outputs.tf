output "cluster" {
  value = google_container_cluster.staging.name
}

output "spanner_instance" {
  value = "projects/${var.project}/instances/${google_spanner_instance.staging.name}"
}

output "objects_bucket" {
  value = google_storage_bucket.objects.name
}

output "logs_bucket" {
  value = google_storage_bucket.logs.name
}

output "service_account" {
  value = google_service_account.ok.email
}

output "registry" {
  value = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.ok.repository_id}"
}

output "secrets_service_account" {
  value = local.secrets == 1 ? google_service_account.secrets[0].email : ""
}

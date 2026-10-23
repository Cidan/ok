# ok's staging cell (docs/hosted.md, "Proof at scale, as built"): a regional GKE cluster across a
# region's three zones, a Spanner instance, a bucket for the SCM's objects and one for automation's
# logs, and workload identity for ok's pods. NATS and ok run in the cluster (README.md). It costs
# money while it stands: size it, price it, apply it, run, and destroy it.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
  backend "gcs" {} # -backend-config="bucket=…" -backend-config="prefix=ok-staging"
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_container_cluster" "staging" {
  name     = var.name
  location = var.region # regional: a control plane and nodes in each zone

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false # staging is destroyed after its run

  release_channel { channel = "REGULAR" }
  workload_identity_config { workload_pool = "${var.project}.svc.id.goog" }
}

resource "google_container_node_pool" "ok" {
  name     = "ok"
  cluster  = google_container_cluster.staging.id
  location = var.region

  node_count = var.nodes_per_zone
  node_config {
    machine_type = var.machine_type
    disk_size_gb = 100
    disk_type    = "pd-ssd"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config { mode = "GKE_METADATA" }
    labels = { "ok.staging/pool" = "ok" }
  }
}

resource "google_spanner_instance" "staging" {
  name             = var.name
  config           = "regional-${var.region}"
  display_name     = "ok staging"
  processing_units = var.spanner_processing_units
  # Enterprise: ok identity's directory search uses Spanner's full-text search, which Standard
  # lacks (docs/install.md). A hosted service's cells, which keep no directory, need only Standard.
  edition       = "ENTERPRISE"
  force_destroy = true
}

resource "google_storage_bucket" "objects" {
  name                        = "${var.project}-${var.name}-objects"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "logs" {
  name                        = "${var.project}-${var.name}-logs"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}

# ok's pods — the server, the deployer, and the load generator as a worker — reach Spanner and the
# buckets as one Google service account, through workload identity.
resource "google_service_account" "ok" {
  account_id   = "${var.name}-ok"
  display_name = "ok staging's roles"
}

resource "google_spanner_instance_iam_member" "ok" {
  instance = google_spanner_instance.staging.name
  role     = "roles/spanner.databaseAdmin" # each role makes its database when it first starts
  member   = "serviceAccount:${google_service_account.ok.email}"
}

resource "google_storage_bucket_iam_member" "objects" {
  bucket = google_storage_bucket.objects.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ok.email}"
}

resource "google_storage_bucket_iam_member" "logs" {
  bucket = google_storage_bucket.logs.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ok.email}"
}

resource "google_service_account_iam_member" "workload" {
  for_each           = toset(["ok-server", "ok-load", "ok-deploy"])
  service_account_id = google_service_account.ok.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[${var.namespace}/${each.key}]"
  depends_on         = [google_container_cluster.staging]
}

# ok's images, which the nodes pull: they run as the project's default compute account, which may
# hold no role that reads a registry.
resource "google_artifact_registry_repository" "ok" {
  repository_id = "ok"
  location      = var.region
  format        = "DOCKER"
  description   = "ok staging images"
}

data "google_compute_default_service_account" "nodes" {}

resource "google_artifact_registry_repository_iam_member" "nodes" {
  repository = google_artifact_registry_repository.ok.name
  location   = var.region
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_compute_default_service_account.nodes.email}"
}

# The Spanner client's built-in metrics, which it writes to Cloud Monitoring.
resource "google_project_iam_member" "ok_metrics" {
  project = var.project
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.ok.email}"
}

# ok secrets (docs/secrets.md), apart from ok's other roles: the one principal that reaches the root
# key and Secret Manager, where a repository's settings may keep its values (secrets.stores), and
# its own database in the instance.
locals {
  secrets = var.secrets_root == "" ? 0 : 1
}

resource "google_service_account" "secrets" {
  count        = local.secrets
  account_id   = "${var.name}-secrets"
  display_name = "ok staging's secrets"
}

resource "google_kms_crypto_key_iam_member" "secrets" {
  count         = local.secrets
  crypto_key_id = var.secrets_root
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${google_service_account.secrets[0].email}"
}

resource "google_project_iam_member" "secrets_store" {
  count   = local.secrets
  project = var.project
  role    = "roles/secretmanager.admin" # it makes, reads, and deletes its secrets
  member  = "serviceAccount:${google_service_account.secrets[0].email}"
}

resource "google_spanner_instance_iam_member" "secrets" {
  count    = local.secrets
  instance = google_spanner_instance.staging.name
  role     = "roles/spanner.databaseAdmin"
  member   = "serviceAccount:${google_service_account.secrets[0].email}"
}

resource "google_service_account_iam_member" "secrets_workload" {
  count              = local.secrets
  service_account_id = google_service_account.secrets[0].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[${var.namespace}/ok-secrets]"
  depends_on         = [google_container_cluster.staging]
}

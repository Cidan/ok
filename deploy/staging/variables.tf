variable "project" {
  type        = string
  description = "The Google Cloud project staging stands in."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "name" {
  type    = string
  default = "ok-staging"
}

variable "namespace" {
  type        = string
  default     = "ok"
  description = "The Kubernetes namespace ok's chart is installed in."
}

variable "machine_type" {
  type    = string
  default = "n2-standard-8"
}

variable "nodes_per_zone" {
  type    = number
  default = 1
}

variable "spanner_processing_units" {
  type        = number
  default     = 1000
  description = "A node is 1000; a cell's full share measured seven (docs/hosted.md, \"Proof at scale, as built\")."
}

variable "secrets_root" {
  type        = string
  default     = ""
  description = "A Cloud KMS key's resource name, which ok secrets wraps repositories' keys under (docs/secrets.md, \"Keys\"); empty runs no ok secrets. A key ring outlives everything, so the key is made apart (README.md) and named here."
}

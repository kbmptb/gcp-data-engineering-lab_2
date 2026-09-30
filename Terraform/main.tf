
# Bucket pour l'environnement de développement et production
resource "google_storage_bucket" "bucket" {
  name                        = var.bucket_name
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}



# Dataset BigQuery pour l'environnement de développement et production
resource "google_bigquery_dataset" "dataset" {
dataset_id = var.dataset_name
friendly_name = "Dataset ${var.environment}"
description = "Dataset Terraform ${var.environment}"
location = "EU"
}



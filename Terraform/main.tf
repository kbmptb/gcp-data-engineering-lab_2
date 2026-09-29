
# Bucket pour l'environnement de développement et production
resource "google_storage_bucket" "bucket-demo-dev" {
  name                        = var.bucket_name_dev
  location                    = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}


resource "google_storage_bucket" "bucket-demo-prod" {

  name     = var.bucket_name_prod
  location = "EU"
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Dataset BigQuery pour l'environnement de développement et production
resource "google_bigquery_dataset" "dataflow_demo_dev" {
  dataset_id    = var.dataset_id_dev
  friendly_name = "Mon Dataset Terraform"
  description   = "Dataset créé via Terraform"
  location      = "EU"
}


resource "google_bigquery_dataset" "dataflow_demo_prod" {
  dataset_id    = var.dataset_id_prod
  friendly_name = "Mon Dataset Terraform"
  description   = "Dataset créé via Terraform"
  location      = "EU"
}
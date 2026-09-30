variable "project_id" {
  type        = string
  description = "L'ID de votre projet GCP"
}

variable "region" {
  type        = string
  description = "La région GCP par défaut"
  default     = "europe-west1"
}

# variable nom des buckets GCS pour les environnements de développement et production
variable "bucket_name" {
  type        = string
  description = "Nom unique du bucket GCS"
}


# variable des datasets BigQuery pour les environnements de développement et production

variable "dataset_name" {
  type        = string
  description = "ID du dataset BigQuery"
}

 
# variable service account ID
variable "account_id" {
  type        = string
  description = "ID du compte de service"
}

# variable d 'environnement pour les environnements de développement et production
variable "environment" {
  type        = string
  description = "L'environnement (dev, prod)"
}
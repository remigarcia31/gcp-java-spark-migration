variable "project_id" {
  description = "L'ID unique du projet GCP."
  type        = string
}

variable "region" {
  description = "La région GCP principale pour les ressources."
  type        = string
}

variable "gcs_bucket_name_suffix" {
  description = "Suffixe unique à ajouter au nom du bucket GCS"
  type        = string
}

variable "bq_dataset_name" {
  description = "Nom du dataset BigQuery pour les résultats."
  type        = string
}
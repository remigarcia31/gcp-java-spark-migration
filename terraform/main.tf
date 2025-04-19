# Crée un bucket Google Cloud Storage
resource "google_storage_bucket" "data_bucket" {
  name          = "${var.project_id}-${var.gcs_bucket_name_suffix}"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true # pour la gestion des permissions

  versioning {
    enabled = true # Bonne pratique pour récupérer des fichiers supprimés
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # supprime les objets après 30 jours (pour le dev/test)
    }
  }

  labels = {
    environment = "development"
    project     = "aero-data-analysis"
  }
}

# Crée un dataset BigQuery
resource "google_bigquery_dataset" "results_dataset" {
  dataset_id                  = var.bq_dataset_name
  project                     = var.project_id
  location                    = var.region
  delete_contents_on_destroy  = false # Sécurité: ne pas supprimer les tables si on détruit le dataset via TF

  labels = {
    environment = "development"
    project     = "aero-data-analysis"
  }
}
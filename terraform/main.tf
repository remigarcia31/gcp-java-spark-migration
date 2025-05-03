# Récupère les informations du projet GCP actuel
data "google_project" "project" {}

# --- Permissions Préalables Nécessaires ---

# 1. Donner les permissions nécessaires à l'agent de service Eventarc au niveau du projet
resource "google_project_iam_member" "eventarc_sa_agent_permissions" {
  project = data.google_project.project.project_id
  role    = "roles/eventarc.serviceAgent"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
}

# 2. Donner au Compte de Service GCS le droit de publier sur Pub/Sub
# (Requis pour que GCS envoie des notifications à Eventarc via un topic intermédiaire)
resource "google_project_iam_member" "gcs_sa_pubsub_publisher" {
  project = data.google_project.project.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

# --- Ressources Principales ---

# Crée un bucket Google Cloud Storage
resource "google_storage_bucket" "data_bucket" {
  project       = var.project_id
  name          = "${var.project_id}-${var.gcs_bucket_name_suffix}"
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  versioning { enabled = true }
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 30 }
  }
  labels = {
    environment = "development"
    project     = "aero-data-analysis"
  }
  # Dépend de la permission donnée au SA GCS (bonne pratique)
  depends_on = [google_project_iam_member.gcs_sa_pubsub_publisher]
}

# Crée un dataset BigQuery
resource "google_bigquery_dataset" "results_dataset" {
  project                     = var.project_id
  dataset_id                  = var.bq_dataset_name
  location                    = var.region
  delete_contents_on_destroy  = false
  labels = {
    environment = "development"
    project     = "aero-data-analysis"
  }
}

# Table BigQuery pour l'état mis à jour des avions
resource "google_bigquery_table" "aircraft_status_table" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id   = "aircraft_status_updated"
  schema = jsonencode([
    { "name" : "aircraft_id", "type" : "STRING", "mode" : "REQUIRED"},
    { "name" : "model", "type" : "STRING", "mode" : "NULLABLE"},
    { "name" : "updated_total_flight_hours", "type" : "FLOAT", "mode" : "NULLABLE"}
  ])
  deletion_protection = false
}

# --- Ressources pour Cloud Function GCS -> Pub/Sub ---

# Topic Pub/Sub pour les notifications finales (celui utilisé par la fonction)
resource "google_pubsub_topic" "notification_topic" {
  project = var.project_id
  name    = var.pubsub_topic_id # Défini dans terraform.tfvars
}

# Compte de service pour la Cloud Function
resource "google_service_account" "function_sa" {
  project      = var.project_id
  account_id   = "aero-pubsub-notifier-sa"
  display_name = "Service Account for Aero GCS->PubSub Function"
}

# Autorisation Pub/Sub Publisher pour le compte de service de la fonction
resource "google_project_iam_member" "function_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

# Autorisation Eventarc Receiver pour le compte de service de la fonction
resource "google_project_iam_member" "function_eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.function_sa.email}"
}

# Zipper le code source de la fonction
data "archive_file" "function_source_zip" {
  type        = "zip"
  source_dir  = "../cloud_function_src/"
  output_path = "/tmp/aero_function_source_${timestamp()}.zip"
}

# Uploader le code zippé sur GCS
resource "google_storage_bucket_object" "function_source_archive" {
  name   = "function_source/source-${data.archive_file.function_source_zip.output_md5}.zip"
  bucket = google_storage_bucket.data_bucket.name
  source = data.archive_file.function_source_zip.output_path
}

# Créer la Cloud Function (Gen 2)
resource "google_cloudfunctions2_function" "gcs_trigger_function" {
  project  = var.project_id
  name     = "aero-gcs-notify-function"
  location = var.region

  build_config {
    runtime     = "python311"
    entry_point = "gcs_event_handler"
    source {
      storage_source {
        bucket = google_storage_bucket.data_bucket.name
        object = google_storage_bucket_object.function_source_archive.name
      }
    }
  }

  service_config {
    max_instance_count    = 3
    min_instance_count    = 0
    available_memory      = "256Mi"
    timeout_seconds       = 60
    environment_variables = {
      PROJECT_ID = var.project_id
      TOPIC_ID   = var.pubsub_topic_id
    }
    service_account_email = google_service_account.function_sa.email
    all_traffic_on_latest_revision = true
    ingress_settings               = "ALLOW_ALL"
  }

  depends_on = [
    google_storage_bucket_object.function_source_archive,
    google_project_iam_member.function_pubsub_publisher,
    google_project_iam_member.function_eventarc_receiver
  ]
}

# Créer le Trigger Eventarc pour lier GCS à la Cloud Function
resource "google_eventarc_trigger" "gcs_success_trigger" {
  project  = var.project_id
  name     = "trigger-aero-gcs-success-file"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.data_bucket.name
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.gcs_trigger_function.service_config[0].service
      region  = var.region
    }
  }

  # Utilise le SA de la fonction comme identité pour le trigger
  service_account = google_service_account.function_sa.email

  transport {
    pubsub {}
  }

  # Dépend de la fonction, des permissions de l'agent Eventarc, et de la permission GCS->PubSub
  depends_on = [
    google_cloudfunctions2_function.gcs_trigger_function,
    google_project_iam_member.eventarc_sa_agent_permissions,
    google_project_iam_member.function_eventarc_receiver,
    google_project_iam_member.gcs_sa_pubsub_publisher # Ajout de cette dépendance
  ]
}

# Donner au Service Agent d'Eventarc le droit d'invoquer la Cloud Function (via son service Cloud Run)
resource "google_cloud_run_v2_service_iam_member" "eventarc_trigger_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.gcs_trigger_function.service_config[0].service
  role     = "roles/run.invoker"
  //member   = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-eventarc.iam.gserviceaccount.com"
  member   = "allUsers"
}
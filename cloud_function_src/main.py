import functions_framework
import cloudevents.http
from google.cloud import pubsub_v1
import os
import logging
import re # Pour vérifier le nom du fichier

# Configuration du Logging
logging.basicConfig(level=logging.INFO)

# Récupération de la config depuis les variables d'environnement
# Ces variables seront définies via Terraform lors du déploiement de la fonction
PROJECT_ID = os.environ.get("PROJECT_ID", "default-project-id") # Fournir une valeur par défaut évite les erreurs au démarrage
TOPIC_ID = os.environ.get("TOPIC_ID", "default-topic-id")

publisher = None
topic_path = None
# Initialisation du client Pub/Sub (une seule fois si possible)
if PROJECT_ID != "default-project-id" and TOPIC_ID != "default-topic-id":
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
else:
    logging.warning("PROJECT_ID or TOPIC_ID environment variable not set.")

# Pattern pour détecter le fichier _SUCCESS dans le bon dossier
# Ex: processed/aircraft_status_updated/_SUCCESS
SUCCESS_FILE_PATTERN = r"^processed/([^/]+)/_SUCCESS$"

@functions_framework.cloud_event
def gcs_event_handler(cloud_event: cloudevents.http.CloudEvent):
    """
    Fonction déclenchée par un événement GCS (création/finalisation d'objet).
    Vérifie si l'objet est un fichier _SUCCESS au bon endroit et publie
    une notification Pub/Sub si c'est le cas.
    """
    global publisher, topic_path # Utilise les variables globales initialisées

    if not publisher or not topic_path:
        logging.error("Publisher client not initialized properly due to missing environment variables.")
        return # Ne peut rien faire sans la config

    try:
        event_data = cloud_event.data
        bucket = event_data.get("bucket")
        name = event_data.get("name")

        if not name:
            logging.warning("Aucun nom d'objet trouvé dans l'événement GCS.")
            return

        logging.info(f"Événement reçu pour l'objet : gs://{bucket}/{name}")

        # Vérifier si le nom correspond au pattern _SUCCESS
        match = re.match(SUCCESS_FILE_PATTERN, name)
        if match:
            table_name = match.group(1) # Extrait le nom de la table du chemin
            logging.info(f"Fichier _SUCCESS détecté pour la table : {table_name}. Publication de la notification...")

            # Construire le message
            message_str = f"Pipeline Spark terminé avec succès pour la table BigQuery '{table_name}'."
            message_bytes = message_str.encode("utf-8")

            # Publier le message (gestion d'erreur basique)
            try:
                publish_future = publisher.publish(topic_path, data=message_bytes)
                message_id = publish_future.result(timeout=60) # Attente confirmation (timeout 60s)
                logging.info(f"Message {message_id} publié sur {topic_path}.")
            except Exception as pub_e:
                logging.error(f"Échec de la publication sur Pub/Sub pour {topic_path}: {pub_e}", exc_info=True)

        else:
            logging.info(f"L'objet gs://{bucket}/{name} n'est pas le fichier _SUCCESS attendu. Ignoré.")

    except Exception as e:
        logging.error(f"Erreur lors du traitement de l'événement GCS : {e}", exc_info=True)
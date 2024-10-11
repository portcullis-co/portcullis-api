import os

def generate_pipeline_name(organization, source, source_id):
    return f"{organization}_{source}_pipeline_{source_id}"

def generate_destination_name(organization, destination):
    return f"{organization}_{destination}"

TEMPORAL_NAMESPACE = "portcullis"
TEMPORAL_SERVER_URL = os.getenv("TEMPORAL_SERVER_URL", "localhost:7233")
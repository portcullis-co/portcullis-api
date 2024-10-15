import os

def generate_transfer_name(organization, source):
    return f"{organization}_{source}_transfer_{source}"

def generate_destination_name(organization, destination):
    return f"{organization}_{destination}"

TEMPORAL_NAMESPACE = "default"
TEMPORAL_SERVER_URL = os.getenv("TEMPORAL_SERVER_URL", "localhost:7233")
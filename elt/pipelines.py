from typing import Dict, Any
from temporalio import activity
import json
from .transfer import transfer_data

@activity.defn
async def run_pipeline(organization: str, source: str, import_id: str, export_id: str, type: str, dataset_name: str, link_credentials: str, source_credentials: str, source_warehouse: str, import_warehouse: str) -> str:
    # Parse credentials
    link_creds = json.loads(link_credentials)
    source_creds = json.loads(source_credentials)
    
    try:
        result = transfer_data(
            source_type=source_warehouse,
            source_credentials=source_creds,
            destination_type=import_warehouse,
            destination_credentials=link_creds
        )
        return f"Pipeline completed: {result}"
    except Exception as e:
        raise RuntimeError(f"Unexpected error in run_pipeline: {str(e)}")
from typing import Dict, Any
from temporalio import activity
import json
from .transfer import transfer_data
import traceback

@activity.defn
async def run_pipeline(organization: str, source: str, import_id: str, export_id: str, type: str, dataset_name: str, link_credentials: dict, source_credentials: dict, source_warehouse: str, import_warehouse: str) -> str:
    try:
        result = transfer_data(
            source_type=source_warehouse,
            source_credentials=source_credentials,
            destination_type=import_warehouse,
            destination_credentials=link_credentials
        )
        return f"Pipeline completed: {result}"
    except Exception as e:
        error_traceback = traceback.format_exc()
        raise RuntimeError(f"Unexpected error in run_pipeline: {str(e)}\n\nTraceback:\n{error_traceback}")
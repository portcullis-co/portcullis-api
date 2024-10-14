from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from temporalio.client import Client
from temporal.workflows import PortcullisTransferWorkflow, GetSourceTablesWorkflow, TransferDataWorkflow
from config import TEMPORAL_SERVER_URL, TEMPORAL_NAMESPACE
import os
import sys
from supabase import create_client, Client as SupabaseClient
from dotenv import load_dotenv
from typing import Dict, Any
import json

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

router = APIRouter()

class SourceRequest(BaseModel):
    organization: str
    type: str
    source_credentials: Dict[str, Any]

class TransferRequest(BaseModel):
    organization: str
    source: str
    import_id: str
    export_id: str
    link_credentials: Dict[str, Any]
    source_credentials: Dict[str, Any]
    type: str
    source_warehouse: str
    import_warehouse: str
    dataset_name: str

def get_supabase_client() -> SupabaseClient:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase configuration is missing")
    return create_client(url, key)

@router.post("/sources")
async def create_source(request: SourceRequest):
    try:
        # Validate the input
        if not request.type or not request.source_credentials:
            raise HTTPException(status_code=400, detail="Type and credentials are required")

        # Insert the source into the database
        insert_result = await insert_source(
            request.organization,
            request.type,
            request.source_credentials
        )

        if "error" in insert_result:
            raise HTTPException(status_code=500, detail=insert_result["error"])

        return {"message": "Source created successfully", "source_id": insert_result["id"]}

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)  # Log the error
        raise HTTPException(status_code=500, detail=error_message)

async def insert_source(organization: str, type: str, credentials: dict):
    data = {
        "organization": organization,
        "type": type,
        "credentials": credentials,
    }
    
    try:
        supabase = get_supabase_client()
        result = supabase.table("sources").insert(data).execute()
        if result.data:
            return {"message": "Successfully inserted source data", "id": result.data[0]['id']}
        else:
            return {"error": "Failed to insert source data"}
    except Exception as e:
        return {"error": f"Error inserting source data: {str(e)}"}

@router.options("/transfer")
async def options_transfer():
    return JSONResponse(content={}, status_code=200)

@router.post("/transfer")
async def run_transfer(background_tasks: BackgroundTasks, request: TransferRequest):
    try:
        # First, insert the transfer data into the database
        insert_result = await insert_transfer(
            request.organization,
            request.source,
            request.import_id,
            request.export_id,
            request.type,
            request.dataset_name,
            request.source_warehouse,
            request.import_warehouse,
            request.link_credentials,
            request.source_credentials,
        )

        if "error" in insert_result:
            raise HTTPException(status_code=500, detail=insert_result["error"])

        async def execute_workflow():
            try:
                client = await Client.connect(TEMPORAL_SERVER_URL, namespace=TEMPORAL_NAMESPACE)
                
                # Use the appropriate connection params based on the warehouse type
                await client.execute_workflow(
                    PortcullisTransferWorkflow.run,
                    args=[
                        request.organization,
                        request.source,
                        request.import_id,
                        request.export_id,
                        request.type,
                        request.dataset_name,
                        request.source_warehouse,
                        request.import_warehouse,
                        request.link_credentials,
                        request.source_credentials,
                    ],
                    id=f"{request.organization}_{request.source}_workflow",
                    task_queue="portcullis-task-queue"
                )
            except Exception as e:
                print(f"Workflow execution failed: {str(e)}")
                # You might want to update the database or notify the user about the failure

        background_tasks.add_task(execute_workflow)
        return {"message": "Transfer execution started", "transfer_id": insert_result["id"]}

    except Exception as e:
        # Handle the exception
        print(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

async def insert_transfer(organization: str, source: str, import_id: str, export_id: str, type: str, dataset_name: str, import_warehouse: str, source_warehouse: str, link_credentials: dict, source_credentials: dict):
    table_name = "imports" if type == "Import" else "exports"
    id_to_use = import_id if type == "Import" else export_id
    
    data = {
        "id": id_to_use,
        "organization": organization,
        "source": source,
        "import_warehouse": import_warehouse,
        "source_warehouse": source_warehouse,
        "type": type,
        "dataset_name": dataset_name,
        "source_credentials": source_credentials,
        "link_credentials": link_credentials,
    }
    
    try:
        supabase = get_supabase_client()
        result = supabase.table(table_name).insert(data).execute()
        if result.data:
            return {"message": f"Successfully inserted {type} transfer data", "id": id_to_use}
        else:
            return {"error": f"Failed to insert {type} transfer data"}
    except Exception as e:
        return {"error": f"Error inserting {type} transfer data: {str(e)}"}

@router.get("/status/{workflow_id}")
async def get_transfer_status(workflow_id: str):
    client = await Client.connect(TEMPORAL_SERVER_URL)
    workflow = await client.get_workflow_handle(workflow_id)
    status = await workflow.query("status")
    return {"status": status}

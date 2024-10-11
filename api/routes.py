from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from temporalio.client import Client
from temporal.workflows import PortcullisPipelineWorkflow
from config import TEMPORAL_SERVER_URL

router = APIRouter()

class PipelineRequest(BaseModel):
    organization: str
    source: str
    import_id: str
    export_id: str
    link_credentials: dict
    source_credentials: dict
    type: str
    dataset_name: str

@router.post("/pipeline")
async def run_pipeline(background_tasks: BackgroundTasks, request: PipelineRequest):
    async def execute_workflow():
        client = await Client.connect(TEMPORAL_SERVER_URL)
        await client.execute_workflow(
            PortcullisPipelineWorkflow.run,
            args=[request.organization, request.source, request.import_id, request.export_id, request.type, request.dataset_name, request.link_credentials, request.source_credentials],
            id=f"{request.organization}_{request.source}_workflow",
            task_queue="portcullis-task-queue",
        )
    background_tasks.add_task(execute_workflow)
    return {"message": "Pipeline execution started"}

@router.get("/status/{workflow_id}")
async def get_pipeline_status(workflow_id: str):
    client = await Client.connect(TEMPORAL_SERVER_URL)
    workflow = await client.get_workflow_handle(workflow_id)
    status = await workflow.query("status")
    return {"status": status}
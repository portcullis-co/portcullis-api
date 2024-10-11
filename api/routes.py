from fastapi import APIRouter, BackgroundTasks
from temporalio.client import Client
from temporal.workflows import PortcullisPipelineWorkflow
from config import TEMPORAL_NAMESPACE, TEMPORAL_SERVER_URL

router = APIRouter()

@router.post("/pipeline")
async def run_pipeline(background_tasks: BackgroundTasks, organization: str, source: str, source_id: str, destination: str, dataset_name: str):
    async def execute_workflow():
        client = await Client.connect(TEMPORAL_SERVER_URL)
        await client.execute_workflow(
            PortcullisPipelineWorkflow.run,
            args=[organization, source, source_id, destination, dataset_name],
            id=f"{organization}_{source}_workflow_{source_id}",
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
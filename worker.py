import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporal.workflows import PortcullisPipelineWorkflow
from elt.activities import parse_json_credentials, warehouse_query, get_tables, transfer_table
from config import TEMPORAL_SERVER_URL, TEMPORAL_NAMESPACE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_worker():
    try:
        client = await Client.connect(TEMPORAL_SERVER_URL, namespace=TEMPORAL_NAMESPACE)
        
        # Create custom sandbox restrictions
        custom_restrictions = SandboxRestrictions.default.with_passthrough_modules("snowflake.connector")
        
        worker = Worker(
            client,
            task_queue="portcullis-task-queue",
            workflows=[PortcullisPipelineWorkflow],
            activities=[parse_json_credentials, warehouse_query, get_tables, transfer_table],
            workflow_runner=SandboxedWorkflowRunner(restrictions=custom_restrictions)
        )
        
        logger.info("Worker started, listening on task queue 'portcullis-task-queue'")
        await worker.run()
    except Exception as e:
        logger.error(f"Error in run_worker: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(run_worker())
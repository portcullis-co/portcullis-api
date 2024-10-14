import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporal.workflows import GetSourceTablesWorkflow, TransferDataWorkflow, PortcullisTransferWorkflow
from elt.activities import parse_json_credentials, get_tables, transfer_table, get_credentials
from config import TEMPORAL_SERVER_URL, TEMPORAL_NAMESPACE
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_worker():
    try:
        # Check for required environment variables
        required_env_vars = ["SUPABASE_URL", "SUPABASE_KEY", "TEMPORAL_SERVER_URL"]
        for var in required_env_vars:
            if not os.getenv(var):
                raise ValueError(f"Missing required environment variable: {var}")

        client = await Client.connect(TEMPORAL_SERVER_URL, namespace=TEMPORAL_NAMESPACE)
        
        # Create custom sandbox restrictions
        custom_restrictions = SandboxRestrictions.default.with_passthrough_modules(
            "snowflake.connector",
            "requests",
            "json",
            "os",
            "dotenv",
            "typing",
            "datetime",
            "logging",
            "traceback",
            "sniffio",
            "urllib",
            "httpx",
            "supabase",
            "datetime",
            "clickhouse_connect"
        )
        
        worker = Worker(
            client,
            task_queue="portcullis-task-queue",
            workflows=[GetSourceTablesWorkflow, TransferDataWorkflow, PortcullisTransferWorkflow],
            activities=[parse_json_credentials, get_tables, transfer_table, get_credentials],
            workflow_runner=SandboxedWorkflowRunner(restrictions=custom_restrictions)
        )
        
        logger.info("Worker started, listening on task queue 'portcullis-task-queue'")
        await worker.run()
    except Exception as e:
        logger.error(f"Error in run_worker: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(run_worker())
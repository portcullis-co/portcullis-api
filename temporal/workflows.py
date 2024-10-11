from temporalio import workflow
from elt.pipelines import run_pipeline
from typing import Dict, Any

@workflow.defn
class PortcullisPipelineWorkflow:
    @workflow.run
    async def run(self, organization: str, source: str, destination: str, dataset_name: str, credentials: Dict[str, Any]) -> str:
        run_pipeline(organization, source, destination, dataset_name, credentials)
        return "Pipeline completed successfully"
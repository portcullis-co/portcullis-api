from temporalio import workflow
from elt.activities import parse_json_credentials, run_pipeline, snowflake_query
from typing import Dict, Any
from datetime import timedelta

@workflow.defn
class PortcullisPipelineWorkflow:
    @workflow.run
    async def run(
        self,
        organization: str,
        source: str,
        import_id: str,
        export_id: str,
        type: str,
        dataset_name: str,
        connection_params: dict,
        query: str,
        link_credentials: str,
        source_credentials: str,
        source_warehouse: str,
        import_warehouse: str,
    ) -> str:
        # Parse JSON strings to dictionaries in an activity
        link_creds, source_creds = await workflow.execute_activity(
            parse_json_credentials,
            args=[link_credentials, source_credentials],
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        # Execute Snowflake query in a separate activity
        snowflake_result = await workflow.execute_activity(
            snowflake_query,
            args=[query, connection_params],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        # Run the pipeline with the Snowflake query results
        return await workflow.execute_activity(
            run_pipeline,
            args=[organization, source, import_id, export_id, type, dataset_name, 
                  link_creds, source_creds, source_warehouse, import_warehouse, snowflake_result],
            start_to_close_timeout=timedelta(minutes=30)
        )
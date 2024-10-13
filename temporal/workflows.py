from temporalio import workflow
from elt.activities import parse_json_credentials, warehouse_query, get_tables, transfer_table
from typing import Dict, Any, List
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
        
        # Get all tables from the source warehouse
        tables = await workflow.execute_activity(
            get_tables,
            args=[source_warehouse, connection_params],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        # Transfer each table
        for table in tables:
            await workflow.execute_activity(
                transfer_table,
                args=[source_warehouse, import_warehouse, table, connection_params, link_creds],
                start_to_close_timeout=timedelta(minutes=30)
            )
        
        return f"Pipeline completed: Transferred {len(tables)} tables"
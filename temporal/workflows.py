from temporalio import workflow
from elt.activities import transfer_warehouse
from typing import List
from datetime import timedelta

@workflow.defn
class PortcullisTransferWorkflow:
    @workflow.run
    async def run(
        self,
        organization: str,
        source: str,
        import_id: str,
        export_id: str,
        type: str,
        dataset_name: str,
        source_warehouse: str,
        import_warehouse: str,
        source_credentials: dict,
        link_credentials: dict
    ) -> str:
        result = await workflow.execute_activity(
            transfer_warehouse,
            args=[organization, import_id, source_warehouse, import_warehouse, source_credentials, link_credentials],
            start_to_close_timeout=timedelta(minutes=30)
        )
        
        return f"Transfer completed: {result}"

@workflow.defn
class TransferDataWorkflow:
    @workflow.run
    async def run(
        self,
        organization: str,
        source: str,
        destination: str,
        import_id: str,
        export_id: str,
        type: str,
        dataset_name: str,
        tables: List[str],
        source_warehouse: str,
        import_warehouse: str,
    ) -> str:
        source_creds, import_creds = await workflow.execute_activity(
            args=[organization, import_id],
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        for table in tables:
            await workflow.execute_activity(
                transfer_warehouse,
                args=[source_warehouse, import_warehouse, table, source_creds, import_creds],
                start_to_close_timeout=timedelta(minutes=30)
            )
        
        return f"Transfer completed: Transferred {len(tables)} tables"
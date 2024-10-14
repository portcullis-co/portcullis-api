from temporalio import workflow
from elt.activities import parse_json_credentials, get_tables, transfer_table, get_credentials
from typing import List
from datetime import timedelta
import json

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
        link_credentials: dict,
    ) -> str:
        tables = await workflow.execute_activity(
            get_tables,
            args=[source_warehouse, source_credentials],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        for table in tables:
            await workflow.execute_activity(
                transfer_table,
                args=[source_warehouse, import_warehouse, table, source_credentials, link_credentials],
                start_to_close_timeout=timedelta(minutes=30)
            )
        
        return f"Transfer completed: Transferred {len(tables)} tables"

@workflow.defn
class GetSourceTablesWorkflow:
    @workflow.run
    async def run(
        self,
        organization: str,
        source: str,
        import_id: str,
        source_warehouse: str,
    ) -> List[str]:
        source_creds, _ = await workflow.execute_activity(
            get_credentials,
            args=[organization, import_id],
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        tables = await workflow.execute_activity(
            get_tables,
            args=[source_warehouse, source_creds],
            start_to_close_timeout=timedelta(minutes=5)
        )
        
        return tables

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
            get_credentials,
            args=[organization, import_id],
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        for table in tables:
            await workflow.execute_activity(
                transfer_table,
                args=[source_warehouse, import_warehouse, table, source_creds, import_creds],
                start_to_close_timeout=timedelta(minutes=30)
            )
        
        return f"Transfer completed: Transferred {len(tables)} tables"
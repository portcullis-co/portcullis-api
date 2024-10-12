from temporalio import activity
import json
from .pipelines import run_pipeline as pipeline_run
import snowflake.connector

@activity.defn
async def parse_json_credentials(link_credentials: str, source_credentials: str) -> tuple:
    link_creds = json.loads(link_credentials)
    source_creds = json.loads(source_credentials)
    return link_creds, source_creds

@activity.defn
async def run_pipeline(organization: str, source: str, import_id: str, export_id: str, type: str, dataset_name: str, link_credentials: dict, source_credentials: dict, source_warehouse: str, import_warehouse: str, snowflake_result: list) -> str:
    return await pipeline_run(organization, source, import_id, export_id, type, dataset_name, link_credentials, source_credentials, source_warehouse, import_warehouse, snowflake_result)

@activity.defn
async def snowflake_query(query: str, connection_params: dict):
    conn = snowflake.connector.connect(**connection_params)
    try:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()
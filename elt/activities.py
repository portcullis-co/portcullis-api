from typing import List
from temporalio import activity
from supabase import create_client, Client
import os
import json
import logging
from dotenv import load_dotenv
from .transfer import get_connector

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

LINK_CREDENTIAL_WAREHOUSES = ['clickhouse', 'postgres']  # Add other warehouse types that should use link_credentials

if not supabase_url or not supabase_key:
    logging.error("Supabase URL or key is missing. Please check your environment variables.")
    supabase = None
else:
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {str(e)}")
        supabase = None

@activity.defn
async def get_credentials(organization: str, import_id: str) -> tuple:
    try:
        # Query the 'imports' table in Supabase
        response = supabase.table("imports").select("source_credentials, link_credentials").eq("organization", organization).eq("id", import_id).execute()
        
        if len(response.data) == 0:
            raise ValueError(f"No import found for organization {organization} and import_id {import_id}")
        
        # The credentials are already dictionaries, so we don't need to use json.loads()
        source_credentials = response.data[0]['source_credentials']
        import_credentials = response.data[0]['link_credentials']
        return source_credentials, import_credentials
    except Exception as e:
        activity.logger.error(f"Error retrieving credentials: {str(e)}")
        raise

@activity.defn
async def parse_json_credentials(credentials: str) -> dict:
    return json.loads(credentials)

@activity.defn
async def get_tables(warehouse_type: str, credentials: dict) -> List[str]:
    logging.info(f"Getting tables for {warehouse_type}")
    
    connector = get_connector(warehouse_type, credentials)
    
    try:
        connector.connect()
        tables = connector.get_tables()
        logging.info(f"Retrieved {len(tables)} tables from {warehouse_type}")
        return tables
    except Exception as e:
        logging.error(f"Error getting tables from {warehouse_type}: {str(e)}", exc_info=True)
        raise
    finally:
        connector.close()

@activity.defn
async def transfer_table(source_warehouse: str, import_warehouse: str, table: str, source_creds: dict, link_creds: dict) -> str:
    logging.info(f"Transferring table {table} from {source_warehouse} to {import_warehouse}")

    source = get_connector(source_warehouse, source_creds)
    destination = get_connector(import_warehouse, link_creds)

    try:
        source.connect()
        destination.connect()

        logging.info(f"Fetching data from {source_warehouse} table: {table}")
        data = source.get_table_data(table)

        if data:
            logging.info(f"Creating table in {import_warehouse}: {table}")
            if isinstance(data[0], tuple):
                # Handle tuple case
                schema = ', '.join([f"COLUMN_{i} VARCHAR(16777216)" for i in range(len(data[0]))])
            elif isinstance(data[0], dict):
                # Handle dictionary case
                schema = ', '.join([f"{k} VARCHAR(16777216)" for k in data[0].keys()])
            else:
                raise ValueError(f"Unexpected data type: {type(data[0])}")

            destination.create_table(table, schema)

            logging.info(f"Inserting data into {import_warehouse} table: {table}")
            destination.insert_data(table, data)

        return f"Transferred table {table} from {source_warehouse} to {import_warehouse} with {len(data)} rows"

    except Exception as e:
        logging.error(f"Error transferring table {table}: {str(e)}", exc_info=True)
        raise
    finally:
        source.close()
        destination.close()

@activity.defn
async def warehouse_query(warehouse_type: str, query: str, connection_params: dict):
    if not query:
        logging.warning(f"Empty query provided for {warehouse_type}")
        return []
    
    logging.info(f"Executing query on {warehouse_type}: {query}")
    connector = get_connector(warehouse_type, connection_params)
    connector.connect()
    try:
        result = connector.execute_query(query)
        logging.info(f"Query result: {result}")
        return result if result is not None else []
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        raise
    finally:
        connector.close()
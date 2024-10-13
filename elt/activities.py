from typing import List
from temporalio import activity
import json
from .transfer import get_connector, transfer_data
import logging

@activity.defn
async def parse_json_credentials(link_credentials: str, source_credentials: str) -> tuple:
    link_creds = json.loads(link_credentials)
    source_creds = json.loads(source_credentials)
    return link_creds, source_creds

@activity.defn
async def get_tables(warehouse_type: str, connection_params: dict) -> List[str]:
    logging.info(f"Getting tables for {warehouse_type}")
    connector = get_connector(warehouse_type, connection_params)
    try:
        connector.connect()
        tables = connector.get_tables()
        logging.info(f"Retrieved {len(tables)} tables from {warehouse_type}")
        return tables
    except Exception as e:
        logging.error(f"Error getting tables from {warehouse_type}: {str(e)}")
        raise
    finally:
        connector.close()

@activity.defn
async def transfer_table(source_type: str, destination_type: str, table_name: str, source_params: dict, destination_params: dict) -> str:
    source = get_connector(source_type, source_params)
    destination = get_connector(destination_type, destination_params)
    
    source.connect()
    destination.connect()
    
    try:
        # Get table data from source
        data = source.get_table_data(table_name)
        
        if data:
            # Create table in destination
            schema = ', '.join([f"{k} TEXT" for k in data[0].keys()])  # Simplified schema
            destination.create_table(table_name, schema)
            
            # Insert data into destination
            destination.insert_data(table_name, data)
            
        return f"Transferred table: {table_name}"
    except Exception as e:
        logging.error(f"Error transferring table {table_name}: {str(e)}")
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
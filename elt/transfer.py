from .connectors.postgres import PostgresConnector
from .connectors.clickhouse import ClickhouseConnector
from .connectors.snowflake import SnowflakeConnector
from .connectors.base import WarehouseConnector
import logging

def get_connector(warehouse_type: str, credentials: dict) -> WarehouseConnector:
    warehouse_type = warehouse_type.lower()
    logging.info(f"Getting connector for {warehouse_type}")
    logging.info(f"Credentials type: {type(credentials)}")
    logging.info(f"Credentials keys: {credentials.keys()}")

    if warehouse_type == 'postgres':
        return PostgresConnector(credentials)
    elif warehouse_type == 'clickhouse':
        return ClickhouseConnector(credentials)
    elif warehouse_type == 'snowflake':
        return SnowflakeConnector(credentials)
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse_type}")

def transfer_data(source_type, source_credentials, destination_type, destination_credentials):
    source = get_connector(source_type, source_credentials)
    destination = get_connector(destination_type, destination_credentials)

    source.connect()
    destination.connect()

    try:
        tables = source.get_tables()
        for table in tables:
            data = source.get_table_data(table)
            if data:
                schema = ', '.join([f"{k} TEXT" for k in data[0].keys()])  # Simplified schema
                destination.create_table(table, schema)
                destination.insert_data(table, data)

        return f"Transferred data for {len(tables)} tables"
    finally:
        source.close()
        destination.close()
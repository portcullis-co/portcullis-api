from clickhouse_driver import Client
from .base import WarehouseConnector
import logging

class ClickhouseConnector(WarehouseConnector):
    def __init__(self, credentials):
        self.credentials = credentials
        self.client = None

    def connect(self):
        if 'host' not in self.credentials:
            raise ValueError("'host' is required in credentials for Clickhouse connection")
        
        valid_args = ['host', 'port', 'database', 'user', 'password', 'secure']
        filtered_credentials = {k: v for k, v in self.credentials.items() if k in valid_args}
        
        logging.info(f"Attempting to connect to ClickHouse at {filtered_credentials['host']}:{filtered_credentials.get('port', 9000)}")
        
        try:
            self.client = Client(**filtered_credentials)
            self.client.execute("SELECT 1")  # Test the connection
            logging.info("Successfully connected to ClickHouse")
        except Exception as e:
            logging.error(f"Failed to connect to ClickHouse: {str(e)}")
            raise

    def execute_query(self, query, params=None):
        return self.client.execute(query, params)

    def get_tables(self):
        query = "SHOW TABLES"
        logging.info(f"Executing query: {query}")
        try:
            result = self.execute_query(query)
            tables = [row[0] for row in result]
            logging.info(f"Retrieved {len(tables)} tables")
            return tables
        except Exception as e:
            logging.error(f"Error retrieving tables: {str(e)}")
            raise

    def get_table_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def create_table(self, table_name, schema):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema}) ENGINE = MergeTree() ORDER BY tuple()"
        self.execute_query(query)

    def insert_data(self, table_name, data):
        if not data:
            return
        columns = list(data[0].keys())
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
        values = [tuple(row.values()) for row in data]
        self.client.execute(query, values)

    def close(self):
        if self.client:
            self.client.disconnect()
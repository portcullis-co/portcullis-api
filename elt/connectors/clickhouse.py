import clickhouse_connect
import logging
from .base import WarehouseConnector
import requests

class ClickhouseConnector(WarehouseConnector):
    def __init__(self, credentials, is_source=True):
        self.credentials = credentials
        self.is_source = is_source
        self.client = None
        self.session = requests.Session()

    def connect(self):
        try:
            logging.info(f"Attempting to connect to ClickHouse as {'source' if self.is_source else 'destination'} with credentials: {self.credentials}")
            host = self.credentials.get('host') or self.credentials.get('account', '').split('.')[0]
            port = self.credentials.get('port', 8443)
            username = self.credentials.get('user') or self.credentials.get('username')
            password = self.credentials['password']
            database = self.credentials.get('database', 'default')
            self.client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                secure=True
            )
            logging.info(f"Successfully connected to ClickHouse at {host}:{port}")
        except Exception as e:
            logging.error(f"Failed to connect to ClickHouse: {str(e)}")
            raise

    def execute_query(self, query, params=None):
        try:
            result = self.client.query(query, parameters=params)
            return result.result_rows
        except Exception as e:
            logging.error(f"Error executing query: {str(e)}")
            raise

    def get_tables(self):
        return [row[0] for row in self.execute_query("SHOW TABLES")]

    def get_table_data(self, table_name):
        return self.execute_query(f"SELECT * FROM {table_name}")

    def create_table(self, table_name, schema):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema}) ENGINE = MergeTree() ORDER BY tuple()"
        self.execute_query(query)

    def close(self):
        if self.client:
            self.client.close()
        logging.info("Closed ClickHouse connection")

    def insert_data(self, table_name, data):
        if not data:
            return

        if isinstance(data[0], dict):
            columns = list(data[0].keys())
            self.client.insert(table_name, data, column_names=columns)
        elif isinstance(data[0], tuple):
            self.client.insert(table_name, data)
        else:
            raise ValueError(f"Unexpected data type: {type(data[0])}")
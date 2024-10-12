from clickhouse_driver import Client
from .base import WarehouseConnector

class ClickhouseConnector(WarehouseConnector):
    def __init__(self, credentials):
        self.credentials = credentials
        self.client = None

    def connect(self):
        self.client = Client(**self.credentials)

    def execute_query(self, query, params=None):
        return self.client.execute(query, params)

    def get_tables(self):
        query = "SHOW TABLES"
        return [row[0] for row in self.execute_query(query)]

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
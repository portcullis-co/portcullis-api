import snowflake.connector
from .base import WarehouseConnector
import logging

class SnowflakeConnector(WarehouseConnector):
    def __init__(self, credentials, is_source=True):
        self.credentials = credentials
        self.is_source = is_source
        self.connection = None
        self.cursor = None
        
    def connect(self):
        logging.info(f"Attempting to connect to Snowflake as {'source' if self.is_source else 'destination'} with credentials: {self.credentials}")
        if 'account' not in self.credentials:
            raise ValueError("Snowflake account must be specified in the credentials")
        self.connection = snowflake.connector.connect(**self.credentials)
        self.cursor = self.connection.cursor(snowflake.connector.DictCursor)
        logging.info("Successfully connected to Snowflake")

    # ... (rest of the methods remain the same)

    def execute_query(self, query, params=None):
        if not query:
            return None
        self.cursor.execute(query, params)
        if self.cursor.description:
            return self.cursor.fetchall()
        return None

    def get_tables(self):
        query = "SHOW TABLES"
        return [row['name'] for row in self.execute_query(query)]

    def get_table_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def create_table(self, table_name, schema):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        self.execute_query(query)

    def insert_data(self, table_name, data):
        if not data:
            return
        if isinstance(data[0], dict):
            columns = list(data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            values = [tuple(row.values()) for row in data]
        elif isinstance(data[0], tuple):
            columns = [f"COLUMN_{i}" for i in range(len(data[0]))]
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            values = data
        else:
            raise ValueError(f"Unexpected data type: {type(data[0])}")
        
        self.cursor.executemany(query, values)
        self.connection.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
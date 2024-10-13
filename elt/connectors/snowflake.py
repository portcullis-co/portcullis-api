import snowflake.connector
from .base import WarehouseConnector

class SnowflakeConnector(WarehouseConnector):
    def __init__(self, credentials):
        self.credentials = credentials
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = snowflake.connector.connect(**self.credentials)
        self.cursor = self.connection.cursor(snowflake.connector.DictCursor)

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
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [tuple(row.values()) for row in data]
        self.cursor.executemany(query, values)
        self.connection.commit()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
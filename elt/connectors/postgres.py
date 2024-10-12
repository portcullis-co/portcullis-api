import psycopg2
from psycopg2.extras import RealDictCursor
from .base import WarehouseConnector

class PostgresConnector(WarehouseConnector):
    def __init__(self, credentials):
        self.credentials = credentials
        self.connection = None

    def connect(self):
        self.connection = psycopg2.connect(**self.credentials)

    def execute_query(self, query, params=None):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_tables(self):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        return [row['table_name'] for row in self.execute_query(query)]

    def get_table_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def create_table(self, table_name, schema):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        self.connection.commit()

    def insert_data(self, table_name, data):
        columns = data[0].keys()
        values = [tuple(row.values()) for row in data]
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        with self.connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, query, values)
        self.connection.commit()

    def close(self):
        if self.connection:
            self.connection.close()
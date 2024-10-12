from abc import ABC, abstractmethod

class WarehouseConnector(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_table_data(self, table_name):
        pass

    @abstractmethod
    def create_table(self, table_name, schema):
        pass

    @abstractmethod
    def insert_data(self, table_name, data):
        pass

    @abstractmethod
    def close(self):
        pass
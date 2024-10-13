from abc import ABC, abstractmethod

class WarehouseConnector(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute_query(self, query, params=None):
        pass

    def get_tables(self):
        raise NotImplementedError("This method is not supported for this warehouse type")

    def get_table_data(self, table_name):
        raise NotImplementedError("This method is not supported for this warehouse type")

    def create_table(self, table_name, schema):
        raise NotImplementedError("This method is not supported for this warehouse type")

    def insert_data(self, table_name, data):
        raise NotImplementedError("This method is not supported for this warehouse type")
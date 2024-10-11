import dlt
from typing import Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import generate_pipeline_name, generate_destination_name

def create_sql_pipeline(organization: str, source: str, source_id: str, destination: str, dataset_name: str, credentials: Dict[str, Any]):
    pipeline_name = generate_pipeline_name(organization, source, source_id)
    destination_name = generate_destination_name(organization, destination)
    
    @dlt.source
    def sql_source():
        try:
            # Extract database connection details from credentials
            db_type = credentials.get("db_type", "postgresql")
            host = credentials.get("host")
            port = credentials.get("port")
            database = credentials.get("database")
            username = credentials.get("username")
            password = credentials.get("password")
            query = credentials.get("query")

            # Construct the database URL
            db_url = f"{db_type}://{username}:{password}@{host}:{port}/{database}"

            # Create a database engine
            engine = create_engine(db_url)

            # Execute the query and yield results
            with engine.connect() as connection:
                result = connection.execute(text(query))
                for row in result:
                    yield dict(row)

        except SQLAlchemyError as e:
            yield {"error": f"Database error: {str(e)}"}

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_name,
        dataset_name=dataset_name
    )

    load_info = pipeline.run(sql_source())
    return load_info

def run_pipeline(organization: str, source: str, source_id: str, destination: str, dataset_name: str, credentials: Dict[str, Any]):
    load_info = create_sql_pipeline(organization, source, source_id, destination, dataset_name, credentials)
    print(f"Pipeline load info: {load_info}")
    return load_info
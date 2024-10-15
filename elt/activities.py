from temporalio import activity
import logging
import dlt
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from dlt.sources.sql_database    import sql_database
from sqlalchemy import create_engine
import logging
from dlt.sources.credentials import ConnectionStringCredentials
import re
from sqlalchemy.engine.url import make_url

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")

if not supabase_url or not supabase_key:
    logging.error("Supabase URL or key is missing. Please check your environment variables.")
    supabase = None
else:
    try:
        supabase: Client = create_client(supabase_url, supabase_key)
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {str(e)}")
        supabase = None

def get_connection_string(warehouse_type: str, credentials: dict) -> str:
    warehouse_type = warehouse_type.lower()
    
    if warehouse_type == "snowflake":
        required_keys = ['user', 'password', 'account', 'database', 'schema', 'warehouse']
        if not all(key in credentials for key in required_keys):
            missing_keys = [key for key in required_keys if key not in credentials]
            raise ValueError(f"Missing required Snowflake credentials: {', '.join(missing_keys)}")
        return f"snowflake://{credentials['user']}:{credentials['password']}@{credentials['account']}/{credentials['database']}/{credentials['schema']}?warehouse={credentials['warehouse']}"
    elif warehouse_type == "clickhouse":
        required_keys = ['host', 'database', 'user', 'password']
        if not all(key in credentials for key in required_keys):
            missing_keys = [key for key in required_keys if key not in credentials]
            raise ValueError(f"Missing required ClickHouse credentials: {', '.join(missing_keys)}")
        return f"clickhouse+native://{credentials['user']}:{credentials['password']}@{credentials['host']}:9440/{credentials['database']}?secure=true&verify=false"
    else:
        raise ValueError(f"Unsupported warehouse type: {warehouse_type}")
    
def get_source_credentials(warehouse_type: str, credentials: dict) -> dict:
    warehouse_type = warehouse_type.lower()
    if warehouse_type == "snowflake":
        return {
            "drivername": "snowflake",
            "User": credentials['user'],
            "Password": credentials['password'],
            "Account": credentials['account'],
            "Database": credentials['database'],
            "Schema": credentials['schema'],
            "Warehouse": credentials['warehouse']
        }
    
    elif warehouse_type == "bigquery":
        return {
            "drivername": "bigquery",
            "Project": credentials['project'],
            "Dataset": credentials['dataset'],
            "Keyfile": credentials['keyfile']
        }
    
    elif warehouse_type == "databricks":
        return {
            "drivername": "databricks",
            "Host": credentials['host'],
            "HttpPath": credentials['http_path'],
            "Token": credentials['token']
        }
    
    else:
        raise ValueError(f"Unsupported source type: {warehouse_type}")

@activity.defn
async def transfer_warehouse(organization: str, import_id: str, source_warehouse: str, import_warehouse: str, source_credentials: dict, link_credentials: dict) -> str:
    if not supabase:
        raise Exception("Supabase client is not initialized")

    try:
        # Log the warehouse types and credentials (masked) for debugging
        logging.info(f"Import warehouse type: {import_warehouse}")
        logging.info(f"Source warehouse type: {source_warehouse}")
        masked_link_credentials = {k: v if k not in ['password', 'key', 'token'] else '****' for k, v in link_credentials.items()}
        masked_source_credentials = {k: v if k not in ['password', 'key', 'token'] else '****' for k, v in source_credentials.items()}
        logging.info(f"Link credentials: {masked_link_credentials}")
        logging.info(f"Source credentials: {masked_source_credentials}")

        # Configure source (import_warehouse) - ClickHouse
        import_connection_string = get_connection_string(import_warehouse, link_credentials)
        logging.info(f"Import connection string: {mask_connection_string(import_connection_string)}")
        source_config = sql_database(ConnectionStringCredentials(import_connection_string))  # Use sql_database correctly

        # Configure destination (source_warehouse) - Snowflake
        source_connection_string = get_connection_string(source_warehouse, source_credentials)
        logging.info(f"Source connection string: {mask_connection_string(source_connection_string)}")

        url = make_url(source_connection_string)
        drivername = url.drivername

        destination_credentials = get_source_credentials(source_warehouse, source_credentials)

        destination_engine = create_engine(source_connection_string)
        # Create the destination config
        destination_config = dlt.destinations.sqlalchemy(
            engine=destination_engine,
            credentials=destination_credentials
        )

        # Create pipeline
        pipeline = dlt.pipeline(
            pipeline_name=f"{organization}_{import_id}_transfer",
            destination=destination_config,
            dataset_name=f"{organization}_{import_id}_dataset"
        )

        # Run the pipeline
        load_info = pipeline.run(source_config)

        return f"Transfer completed successfully. Loaded {load_info.load_packages[0].count} rows in total."

    except Exception as e:
        logging.error(f"Error in transfer_warehouse: {str(e)}")
        raise

def mask_connection_string(connection_string: str) -> str:
    # Simple masking function, you might want to improve this for production use
    return re.sub(r':(.+?)@', ':****@', connection_string)
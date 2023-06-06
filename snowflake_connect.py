from dotenv import load_dotenv
import snowflake.connector
import os

# Load variables from .env
load_dotenv()

# Get sensitive data from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
query_tag = os.getenv('SNOWFLAKE_QUERY_TAG')

# Connect to Snowflake
connection = snowflake.connector.connect(
    user = user,
    password = password,
    account = account,
    warehouse = warehouse,
    database = database,
    schema = schema,
    session_parameters = {
        'QUERY_TAG': query_tag
    }
)
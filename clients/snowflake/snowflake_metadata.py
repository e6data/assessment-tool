import snowflake.connector
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


def run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir):
    try:
        logger.info(f"Executing query for {csv_filename} metadata")
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(result, columns=columns)

        output_path = os.path.join(csv_output_dir, f'{csv_filename}.parquet')
        df.to_parquet(output_path, index=False)
        logger.info(f"Data written to {csv_filename}")
    except Exception as e:
        logger.error(f"Failed to execute query for {csv_filename}: {e}")


def extract_metadata(directory):
    host = os.environ.get('SNOWFLAKE_HOST')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    role = os.environ.get('SNOWFLAKE_ROLE')
    database = 'SNOWFLAKE'
    schema = 'ACCOUNT_USAGE'
    csv_output_dir = directory
    os.makedirs(csv_output_dir, exist_ok=True)
    try:
        logger.info("Creating connection with Snowflake")
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=host,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        queries = {
            'tables': """SELECT a.table_catalog, a.table_schema, a.table_name, a.table_type, a.row_count, a.bytes, 
                    a.clustering_key,b.view_definition FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES a left join 
                    SNOWFLAKE.ACCOUNT_USAGE.VIEWS b on a.table_catalog=b.table_catalog and a.table_schema=b.table_schema
                     and a.table_name=b.table_name WHERE a.DELETED IS NULL and a.table_catalog not in ('SNOWFLAKE')""",
            'columns': """SELECT table_catalog, table_schema, table_name, ordinal_position, column_name, data_type 
                        FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS WHERE DELETED IS NULL""",
            'functions': """SELECT function_schema, function_name, data_type, FUNCTION_LANGUAGE, FUNCTION_DEFINITION, 
                        argument_signature FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS WHERE DELETED IS NULL""",
            'warehouse': """SHOW WAREHOUSES""",
            'warehouse_usage': """SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                                    WHERE DATE(start_time) <= CURRENT_DATE() 
                                    AND DATE(start_time) >= CURRENT_DATE() - INTERVAL '30 DAY'"""
        }

        cursor = conn.cursor()
        logger.info("Connected to Snowflake.")

        use_admin_role = f"USE ROLE {role};"
        cursor.execute(use_admin_role)
        logger.info(f"Using {role} for extracting metadata")

        for csv_filename, query in queries.items():
            run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir)
        cursor.close()
        conn.close()
        logger.info("Connection Closed.")
    except Exception as e:
        logger.error(f"Error extracting Snowflake metadata: {str(e)}")

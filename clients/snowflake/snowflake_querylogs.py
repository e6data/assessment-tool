import snowflake.connector
from datetime import datetime
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)


def extract_query_logs(directory):
    host = os.environ.get('SNOWFLAKE_HOST')
    user = os.environ.get('SNOWFLAKE_USER')
    role = os.environ.get('SNOWFLAKE_ROLE')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    database = 'SNOWFLAKE'
    schema = 'ACCOUNT_USAGE'
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')
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
        cursor = conn.cursor()
        logger.info("Connected to snowflake")
        cursor.execute(f"USE ROLE {role};")
        logger.info(f"Using {role} for extracting query logs")

        start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
        end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

        start_timestamp = start_date.strftime('%Y-%m-%dT00:00:00Z')
        end_timestamp = end_date.strftime('%Y-%m-%dT23:59:59Z')

        history_query = f"""
            SELECT query_id, query_text, database_name, schema_name, query_type, 
                       user_name, warehouse_size, execution_status, 
                       error_message, start_time, end_time, bytes_scanned, 
                       percentage_scanned_from_cache, bytes_written, rows_produced,
                       partitions_scanned, partitions_total, bytes_spilled_to_local_storage, 
                       bytes_spilled_to_remote_storage, bytes_sent_over_the_network, 
                       total_elapsed_time, compilation_time, execution_time, queued_overload_time,
                       credits_used_cloud_services 
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE end_time >= to_timestamp_ltz('{start_timestamp}') 
            AND end_time <= to_timestamp_ltz('{end_timestamp}') 
            AND is_client_generated_statement = FALSE;
        """
        logger.info(f"Fetching query history it may take few minutes...")
        cursor.execute(history_query)
        result = cursor.fetchall()
        if result:
            columns = [col[0] for col in cursor.description]
            df = pd.DataFrame(result, columns=columns)
            df["START_TIME"] = df["START_TIME"].astype(str)
            df["END_TIME"] = df["END_TIME"].astype(str)
            logger.info(f"Writing query history into parquet...")
            parquet_filename = f"{csv_output_dir}/query_history_snowflake_1.parquet"
            df.to_parquet(parquet_filename, index=False)
            logger.info(f"Data has been exported to {os.path.basename(parquet_filename)}")
            logger.info(f"Query Log Successfully Exported to {csv_output_dir}")
        else:
            logger.info("No queries found for the specified date range.")
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Failed to connect with Snowflake: {str(e)}")

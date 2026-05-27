import os
import logging

import pandas as pd

from snowflake.snowflake_common import (
    connect_with_retry,
    use_role,
    close_quietly,
    write_parquet,
)

logger = logging.getLogger(__name__)


def run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir):
    try:
        logger.info(f"Executing query for {csv_filename} metadata")
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        output_path = os.path.join(csv_output_dir, f'{csv_filename}.parquet')
        write_parquet(df, output_path)
        logger.info(f"Data written to {csv_filename}")
    except Exception as e:
        logger.error(f"Failed to execute query for {csv_filename}: {e}")


def extract_metadata(directory):
    role = os.environ.get('SNOWFLAKE_ROLE')
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')
    os.makedirs(directory, exist_ok=True)

    queries = {
        'tables': """SELECT a.*, b.view_definition
                FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES a
                LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.VIEWS b
                    ON a.table_catalog = b.table_catalog
                    AND a.table_schema = b.table_schema
                    AND a.table_name = b.table_name
                WHERE a.DELETED IS NULL AND a.table_catalog NOT IN ('SNOWFLAKE')""",
        'views': """SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS
                    WHERE DELETED IS NULL AND table_catalog NOT IN ('SNOWFLAKE')""",
        'columns': """SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
                    WHERE DELETED IS NULL AND table_catalog NOT IN ('SNOWFLAKE')""",
        'functions': """SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS
                    WHERE DELETED IS NULL AND function_catalog NOT IN ('SNOWFLAKE')""",
        'warehouse': """SHOW WAREHOUSES""",
        'warehouse_usage': f"""SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                                WHERE DATE(start_time) between date('{query_log_start}')
                                AND date('{query_log_end}') """
    }

    conn = None
    cursor = None
    try:
        conn = connect_with_retry()
        cursor = conn.cursor()
        use_role(cursor, role)
        logger.info("Using role for extracting metadata")

        for csv_filename, query in queries.items():
            run_query_and_save_to_csv(cursor, query, csv_filename, directory)
        logger.info("Metadata extraction completed.")
    except Exception as e:
        logger.error(f"Error extracting Snowflake metadata: {e}")
    finally:
        close_quietly(cursor, conn)
        logger.info("Connection Closed.")

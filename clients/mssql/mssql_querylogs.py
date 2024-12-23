import pandas as pd
import pymssql
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)


def extract_query_logs():
    server = os.environ.get('MSSQL_SERVER')
    port = os.environ.get('MSSQL_PORT')
    user = os.environ.get('MSSQL_USER')
    password = os.environ.get('MSSQL_PASSWORD')
    database = os.environ.get('MSSQL_DATABASE')
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')
    csv_output_dir = 'mssql-query-logs'
    os.makedirs(csv_output_dir, exist_ok=True)
    logger.info("Connecting to MSSQL Server...")
    try:
        conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            port=port,
            as_dict=True
        )
        cursor = conn.cursor()
        logger.info("Connected to MSSQL Server")
        start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
        end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

        history_query = f"""
                        SELECT * FROM sys.dm_exec_query_stats AS qs
                        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
                        WHERE qs.creation_time >= '{start_date}'
                        AND qs.creation_time <= '{end_date}';
                        """
        logger.info("Extracting Query logs...")
        cursor.execute(history_query)
        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]

        df = pd.DataFrame.from_records(rows, columns=columns)
        if df.empty:
            logger.info(f"No queries were found between {query_log_start} and {query_log_end}.")
        else:
            logger.info(f"Writing query history into csv...")
            parquet_filename = f"{csv_output_dir}/query_history_mssql.parquet"
            df.to_parquet(parquet_filename, index=False)
            logger.info(f"Data has been exported to {os.path.basename(parquet_filename)}")

            logger.info(f"Query Log Successfully Exported to {csv_output_dir}")
        cursor.close()
        conn.close()
        logger.info("Connection Closed")
    except Exception as e:
        logger.error(f"Error extracting MSSQL metadata: {str(e)}")



import os
import logging
from datetime import datetime

import pandas as pd

from snowflake.snowflake_common import (
    connect_with_retry,
    use_role,
    close_quietly,
    write_parquet,
)

logger = logging.getLogger(__name__)


def extract_query_logs(directory):
    role = os.environ.get('SNOWFLAKE_ROLE')
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')
    os.makedirs(directory, exist_ok=True)

    start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
    end_date = datetime.strptime(query_log_end, '%Y-%m-%d')
    start_timestamp = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_timestamp = end_date.strftime('%Y-%m-%dT23:59:59Z')

    conn = None
    cursor = None
    try:
        conn = connect_with_retry()
        cursor = conn.cursor()
        use_role(cursor, role)
        logger.info("Using role for extracting query logs")

        history_query = f"""
            SELECT *
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE end_time >= to_timestamp_ltz('{start_timestamp}')
            AND end_time <= to_timestamp_ltz('{end_timestamp}');
        """
        logger.info("Fetching query history; this may take a few minutes...")
        cursor.execute(history_query)
        result = cursor.fetchall()

        if not result:
            logger.info("No queries found for the specified date range.")
            return

        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        parquet_path = os.path.join(directory, "query_history_snowflake.parquet")
        logger.info("Writing query history into parquet...")
        write_parquet(df, parquet_path)
        logger.info(f"Data has been exported to {os.path.basename(parquet_path)}")
        logger.info(f"Query Log Successfully Exported to {directory}")

    except Exception as e:
        logger.error(f"Failed during Snowflake query log extraction: {e}")
    finally:
        close_quietly(cursor, conn)

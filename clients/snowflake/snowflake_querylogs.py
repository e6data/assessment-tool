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
            SELECT reader_account_name, query_id, query_text,
                       database_id, database_name, schema_id, schema_name, query_type,
                       session_id, authn_event_id,
                       user_name, role_name,
                       warehouse_id, warehouse_name, warehouse_size, warehouse_type,
                       cluster_number, query_tag, execution_status,
                       error_code, error_message, start_time, end_time,
                       total_elapsed_time,
                       bytes_scanned, percentage_scanned_from_cache,
                       bytes_written, bytes_written_to_result, bytes_read_from_result,
                       rows_produced, rows_inserted, rows_updated, rows_deleted,
                       rows_unloaded, bytes_deleted,
                       partitions_scanned, partitions_total,
                       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage,
                       bytes_sent_over_the_network,
                       compilation_time, execution_time,
                       queued_provisioning_time, queued_repair_time, queued_overload_time,
                       transaction_blocked_time,
                       outbound_data_transfer_cloud, outbound_data_transfer_region,
                       outbound_data_transfer_bytes,
                       inbound_data_transfer_cloud, inbound_data_transfer_region,
                       inbound_data_transfer_bytes,
                       list_external_files_time,
                       credits_used_cloud_services,
                       reader_account_deleted_on,
                       release_version,
                       external_function_total_invocations,
                       external_function_total_sent_rows, external_function_total_received_rows,
                       external_function_total_sent_bytes, external_function_total_received_bytes,
                       query_load_percent, is_client_generated_statement,
                       query_acceleration_bytes_scanned, query_acceleration_partitions_scanned,
                       query_acceleration_upper_limit_scale_factor,
                       transaction_id, child_queries_wait_time, role_type,
                       query_hash, query_hash_version,
                       query_parameterized_hash, query_parameterized_hash_version,
                       secondary_role_stats,
                       rows_written_to_result,
                       query_retry_time, query_retry_cause, fault_handling_time,
                       user_type,
                       user_database_name, user_database_id,
                       user_schema_name, user_schema_id,
                       bind_values
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE end_time >= to_timestamp_ltz('{start_timestamp}')
            AND end_time <= to_timestamp_ltz('{end_timestamp}')
            AND is_client_generated_statement = FALSE;
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

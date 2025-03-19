import pandas as pd
import redshift_connector
import os
import logging

logger = logging.getLogger(__name__)


def run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir):
    try:
        logger.info(f"Executing query for {csv_filename}")
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(result, columns=columns)

        output_path = os.path.join(csv_output_dir, f'{csv_filename}.parquet')
        df.to_parquet(output_path, index=False)
        logger.info(f"Data written to {csv_filename}")
    except Exception as e:
        logger.error(f"Failed to execute query for {csv_filename}: {e}")


def extract_query_logs(directory):
    host = os.environ.get('REDSHIFT_HOST')
    user = os.environ.get('REDSHIFT_USER')
    password = os.environ.get('REDSHIFT_PASSWORD')
    port = os.environ.get('REDSHIFT_PORT')
    database = os.environ.get('REDSHIFT_DATABASE')
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')
    csv_output_dir = directory
    os.makedirs(csv_output_dir, exist_ok=True)
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        tcp_keepalive=True,
        password=password,
        sslmode='require'
    )

    logger.info("Connecting to Redshift...")
    try:
        cursor = conn.cursor()
        logger.info("Connected to Redshift")
        queries = {
                    'query_text': f"""SELECT STL_QUERY."userid",STL_QUERY."xid",STL_QUERY."pid",STL_QUERY."query",STL_QUERY."aborted",STL_QUERY."concurrency_scaling_status",
                                    STL_QUERY."starttime",STL_QUERY."endtime",STL_QUERYTEXT."sequence",STL_QUERYTEXT."text" 
                                    FROM STL_QUERY join STL_QUERYTEXT using (userid, xid, pid, query)
                                    WHERE STL_QUERY.starttime >= cast('{query_log_start}' as timestamp) 
                                    AND STL_QUERY.starttime <= cast('{query_log_end}' as timestamp);""",
                    'query_ddl': f"""SELECT
                                        any_value(L.userid) AS UserId, any_value(L.starttime) AS StartTime,
                                        any_value(L.endtime) AS EndTime, any_value(trim(label)) AS Label,
                                        L.xid AS xid, L.pid AS pid,
                                        LISTAGG(CASE WHEN LEN(RTRIM(L.text)) = 0 THEN text ELSE RTRIM(L.text) END)
                                          WITHIN GROUP (ORDER BY L.sequence) AS SqlText
                                        FROM STL_DDLTEXT L
                                        WHERE starttime >= cast('{query_log_start}' as timestamp) 
                                        AND starttime <= cast('{query_log_end}' as timestamp)
                                        GROUP BY L.xid, L.pid ;""",
                    'query_metrics': f"""SELECT userid, service_class, query, segment, step_type, starttime, slices, 
                                       max_rows, rows, max_cpu_time, cpu_time, max_blocks_read, blocks_read, 
                                       max_run_time, run_time, max_blocks_to_disk, blocks_to_disk, step, 
                                       max_query_scan_size, query_scan_size, query_priority, query_queue_time, 
                                       service_class_name 
                                       FROM STL_QUERY_METRICS WHERE starttime >= cast('{query_log_start}' as timestamp) 
                                       AND starttime <= cast('{query_log_end}' as timestamp);""",
                    'query_queue_info': f"""SELECT database, query, xid, userid, queue_start_time, 
                                          exec_start_time, service_class, slots, queue_elapsed, exec_elapsed, 
                                          wlm_total_elapsed, commit_queue_elapsed, commit_exec_time 
                                          FROM SVL_QUERY_QUEUE_INFO WHERE queue_start_time >= cast('{query_log_start}' as timestamp) 
                                          AND queue_start_time <= cast('{query_log_end}' as timestamp);""",
                    'query_wlm': f"""SELECT userid, xid, task, query, service_class, slot_count, service_class_start_time,
                                  queue_start_time, queue_end_time, total_queue_time, exec_start_time,
                                  exec_end_time, total_exec_time, service_class_end_time, final_state,
                                  query_priority FROM STL_WLM_QUERY WHERE queue_start_time >= cast('{query_log_start}' as timestamp) 
                                  AND queue_start_time <= cast('{query_log_end}' as timestamp);"""
                   }

        logger.info("Extracting Query logs...")

        for csv_filename, query in queries.items():
            run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir)
        cursor.close()
        conn.close()
        logger.info("Connection Closed.")

        logger.info(f"Query Log Successfully Exported to {os.path.abspath(csv_output_dir)}")

    except Exception as e:
        logger.error(f"Failed to connect Redshift: {e}")

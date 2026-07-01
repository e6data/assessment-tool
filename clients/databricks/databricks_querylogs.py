import os
import time
import json
import logging
from datetime import datetime, timedelta

import numpy as np
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks import sql

logger = logging.getLogger(__name__)


def _write_parquet(df, path):
    # Convert dict/list/ndarray columns to JSON strings (e.g., query_parameters)
    def serialize_value(x):
        if isinstance(x, np.ndarray):
            return json.dumps(x.tolist())
        elif isinstance(x, (dict, list)):
            return json.dumps(x)
        return x

    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if any value in the column is a dict, list, or ndarray
            sample = df[col].dropna().head(1)
            if len(sample) > 0 and isinstance(sample.iloc[0], (dict, list, np.ndarray)):
                df[col] = df[col].apply(serialize_value)

    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.round("ms")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, coerce_timestamps='ms')


def extract_query_logs(directory):
    catalog = 'system'
    database = 'information_schema'
    access_token = os.environ.get('DBR_ACCESS_TOKEN')
    http_path = os.environ.get('DBR_WAREHOUSE_HTTP')
    dbr_server_hostname = os.environ.get('DBR_HOST')
    start_date_str = os.environ.get('QUERY_LOG_START')
    end_date_str = os.environ.get('QUERY_LOG_END')
    parquet_output_dir = directory
    os.makedirs(parquet_output_dir, exist_ok=True)

    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(days=1)
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        raise

    def create_dbr_connection():
        return sql.connect(
            server_hostname=dbr_server_hostname,
            http_path=http_path,
            access_token=access_token,
            schema=database,
            catalog=catalog,
        )

    def create_dbr_con_with_retry(retry_count=0):
        max_retries = 3
        try:
            logger.info(f"[{retry_count + 1}/{max_retries}] Connecting to Databricks SQL warehouse...")
            conn = create_dbr_connection()
            logger.info("Connected to Databricks SQL endpoint.")
            return conn
        except Exception as err:
            logger.error(f"Connection failed: {err}")
            if retry_count >= max_retries - 1:
                raise
            time.sleep(10)
            return create_dbr_con_with_retry(retry_count + 1)

    def system_query_history_accessible(conn):
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM system.query.history LIMIT 1")
                cur.fetchall()
            return True
        except Exception as e:
            logger.warning(f"system.query.history is not accessible: {e}")
            return False

    def extract_via_sql_hourly(conn):
        current = start_date
        while current < end_date:
            next_hour = current + timedelta(hours=1)
            st = current.strftime('%Y-%m-%d %H:%M:%S')
            et = next_hour.strftime('%Y-%m-%d %H:%M:%S')
            query = (
                f"SELECT * FROM system.query.history"
                f" WHERE start_time >= TIMESTAMP '{st}'"
                f"   AND start_time <  TIMESTAMP '{et}'"
            )
            logger.info(f"Querying history from {st} to {et}")
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    data = cur.fetchall()
                    cols = [d[0] for d in cur.description]
            except Exception as e:
                logger.error(f"Hourly chunk {st} failed: {e}")
                current = next_hour
                continue

            if data:
                df = pd.DataFrame(data, columns=cols)
                filename = f"query_history_{current.strftime('%Y%m%d_%H')}.parquet"
                _write_parquet(df, os.path.join(parquet_output_dir, filename))
            current = next_hour

    def fetch_query_history_via_rest(start_time_ms, end_time_ms):
        api_url = f"https://{dbr_server_hostname}/api/2.0/sql/history/queries"
        headers = {"Authorization": f"Bearer {access_token}"}
        query_history = []
        next_page_token = None
        max_pages = 10000

        logger.info("Starting to fetch query history via REST API...")
        for page_number in range(max_pages):
            payload = {
                "filter_by": {
                    "statuses": ["FINISHED"],
                    "start_time_ms": start_time_ms,
                    "end_time_ms": end_time_ms,
                },
                "include_metrics": True,
                "max_results": 100,
            }
            if next_page_token:
                payload["page_token"] = next_page_token

            try:
                response = requests.get(api_url, json=payload, headers=headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error during API call on page {page_number + 1}: {e}")
                break

            response_data = response.json()
            queries = response_data.get('res', [])
            query_history.extend(queries)
            logger.info(f"Page {page_number + 1}: fetched {len(queries)}. Total: {len(query_history)}")

            next_page_token = response_data.get("next_page_token", None)
            if not response_data.get("has_more", True) or not next_page_token:
                break
            time.sleep(0.5)

        query_history = [
            q for q in query_history
            if "This is a system generated query from sql editor" not in q.get("query_text", "")
        ]
        return query_history

    def save_rest_history_to_parquet(query_history, output_parquet):
        if not query_history:
            logger.info(f"No data to write in {output_parquet}")
            return

        data = []
        for query in query_history:
            query_text = query.get("query_text", "")
            if "SELECT * FROM system.information_schema" in query_text:
                continue
            if "SET use_cached_result = false " in query_text:
                continue
            metrics = query.get("metrics", {})
            data.append({
                "query_id": query.get("query_id"),
                "query_text": query_text,
                "user_id": query.get("user_id"),
                "user": query.get("user_name"),
                "start_time": query.get("query_start_time_ms"),
                "end_time": query.get("execution_end_time_ms"),
                "status": query.get("status"),
                "total_time_ms": metrics.get("total_time_ms"),
                "read_bytes": metrics.get("read_bytes"),
                "rows_produced_count": metrics.get("rows_produced_count"),
                "compilation_time_ms": metrics.get("compilation_time_ms"),
                "execution_time_ms": metrics.get("execution_time_ms"),
                "read_remote_bytes": metrics.get("read_remote_bytes"),
                "write_remote_bytes": metrics.get("write_remote_bytes"),
                "read_cache_bytes": metrics.get("read_cache_bytes"),
                "spill_to_disk_bytes": metrics.get("spill_to_disk_bytes"),
                "task_total_time_ms": metrics.get("task_total_time_ms"),
                "read_files_count": metrics.get("read_files_count"),
                "read_partitions_count": metrics.get("read_partitions_count"),
                "photon_total_time_ms": metrics.get("photon_total_time_ms"),
                "rows_read_count": metrics.get("rows_read_count"),
                "result_fetch_time_ms": metrics.get("result_fetch_time_ms"),
                "network_sent_bytes": metrics.get("network_sent_bytes"),
                "result_from_cache": metrics.get("result_from_cache"),
                "pruned_bytes": metrics.get("pruned_bytes"),
                "pruned_files_count": metrics.get("pruned_files_count"),
                "provisioning_queue_start_timestamp": metrics.get("provisioning_queue_start_timestamp"),
                "overloading_queue_start_timestamp": metrics.get("overloading_queue_start_timestamp"),
                "query_compilation_start_timestamp": metrics.get("query_compilation_start_timestamp"),
            })

        df = pd.DataFrame(data)
        _write_parquet(df, output_parquet)
        logger.info(f"Query history exported to {output_parquet}")

    def extract_via_rest():
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int((end_date - timedelta(milliseconds=1)).timestamp() * 1000)
        history = fetch_query_history_via_rest(start_ms, end_ms)
        output_parquet = os.path.join(parquet_output_dir, "query_history_output.parquet")
        save_rest_history_to_parquet(history, output_parquet)

    conn = None
    try:
        try:
            conn = create_dbr_con_with_retry()
        except Exception as e:
            logger.warning(f"Could not connect to SQL warehouse ({e})")

        use_sql_path = conn is not None and system_query_history_accessible(conn)

        if use_sql_path:
            logger.info("Extracting query history via system.query.history (hourly chunks)...")
            extract_via_sql_hourly(conn)
        else:
            logger.info("Falling back to Query History REST API...")
            extract_via_rest()
    except Exception as e:
        logger.error(f"Query log extraction failed: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")

from databricks import sql
from datetime import datetime, timedelta
import os
import requests
import time
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

parquet_output_dir = 'databricks-query-logs'
os.makedirs(parquet_output_dir, exist_ok=True)


def extract_query_logs():
    catalog = 'system'
    database = 'information_schema'
    access_token = os.environ.get('DBR_ACCESS_TOKEN')
    warehouse_id = os.environ.get('DBR_WAREHOUSE_ID')

    DBR_HOSTNAME = os.environ.get('DBR_HOST')
    API_URL = f"https://{DBR_HOSTNAME}/api/2.0/sql/history/queries"

    def create_DBR_connection():
        return sql.connect(server_hostname=DBR_HOSTNAME,
                           http_path=f'/sql/1.0/warehouses/{warehouse_id}',
                           access_token=access_token,
                           schema=database,
                           catalog=catalog
                           )

    def create_DBR_con(retry_count=0):
        max_retry_count = 3
        logger.info(f'TIMESTAMP : {datetime.now()} Connecting to DBR database ...')
        now = time.time()
        try:
            dbr_connection = create_DBR_connection()
            logger.info(
                'TIMESTAMP : {} connected with database {} and catalog {} in {} seconds'.format(datetime.now(),
                                                                                                database, catalog,
                                                                                                time.time() - now))
            return dbr_connection
        except Exception as e:
            logger.error(e)
            logger.error(
                'TIMESTAMP : {} Failed to connect to the DBR database with {}'.format(datetime.now(),
                                                                                      database))
            if retry_count > max_retry_count:
                raise e
            logger.error('Retry to connect in {} seconds...'.format(10))
            retry_count += 1
            return create_DBR_con(retry_count=retry_count)

    def fetch_query_history(start_time, end_time):
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        query_history = []
        next_page_token = None
        max_pages = 100

        logger.info("Starting to fetch query history...")

        for page_number in range(max_pages):
            payload = {
                "filter_by": {
                    "statuses": ["FINISHED"],
                    "start_time_ms": start_time,
                    "end_time_ms": end_time
                },
                "include_metrics": True,
                "max_results": 100
            }

            if next_page_token:
                payload["page_token"] = next_page_token

            try:
                response = requests.get(API_URL, json=payload, headers=headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Error during API call on page {page_number + 1}: {e}")
                break

            response_data = response.json()


            queries = response_data.get('res', [])
            query_history.extend(queries)
            logger.info(f"Page {page_number + 1}: Fetched {len(queries)} queries. Total so far: {len(query_history)}")

            next_page_token = response_data.get("next_page_token", None)

            if not response_data.get("has_more", True) or not next_page_token:
                logger.info("No more pages to fetch.")
                break

            time.sleep(0.5)

        logger.info(f"Filtering out system-generated queries. Initial count: {len(query_history)}")
        query_history = [
            query for query in query_history
            if "This is a system generated query from sql editor" not in query.get("query_text", "")
        ]
        logger.info(f"Filtered query count: {len(query_history)}")

        output_parquet = f"{parquet_output_dir}/query_history_output.parquet"
        save_query_history_to_parquet(query_history, output_parquet)

    def save_query_history_to_parquet(query_history, output_parquet):
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
                "user": query.get("user"),
                "start_time": query.get("start_time"),
                "end_time": query.get("end_time"),
                "state": query.get("state"),
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
                "query_compilation_start_timestamp": metrics.get("query_compilation_start_timestamp")
            })

        df = pd.DataFrame(data)

        df.to_parquet(output_parquet, index=False)
        logger.info(f"Query history exported to {output_parquet}")

    def fetch_query_history_by_date(start_date_str, end_date_str):
        start_time = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_time = datetime.strptime(end_date_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(milliseconds=1)
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        fetch_query_history(start_time_ms, end_time_ms)

    start_date = os.environ.get('QUERY_LOG_START')
    end_date = os.environ.get('QUERY_LOG_END')

    fetch_query_history_by_date(start_date, end_date)

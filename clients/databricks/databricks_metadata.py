import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib3.util import Retry

if not hasattr(Retry, 'backoff_jitter'):
    Retry.backoff_jitter = 0.0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

access_token = os.environ.get('DBR_ACCESS_TOKEN')
dbr_server_hostname = os.environ.get('DBR_HOST')
dbr_warehouse_id = os.environ.get('DBR_WAREHOUSE_ID')

start_date = os.environ.get('QUERY_LOG_START')
end_date = os.environ.get('QUERY_LOG_END')

try:
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date   = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
except ValueError as ve:
    logger.error(f"Invalid date format for start_date_str/end_date_str. Use YYYY-MM-DD. Error: {ve}")
    raise

queries = {
    'tables': "SELECT * FROM system.information_schema.tables;",
    'columns': "SELECT * FROM system.information_schema.columns;",
    'views': "SELECT * FROM system.information_schema.views;",
    'billing': f"""
        WITH cte AS (
            SELECT 
                u.usage_date, 
                u.account_id, 
                u.workspace_id, 
                u.sku_name, 
                u.cloud, 
                u.usage_start_time, 
                u.usage_end_time, 
                u.usage_date, 
                ROUND(u.usage_quantity, 2) AS uq, 
                u.usage_metadata, 
                u.product_features, 
                w.warehouse_name, 
                u.usage_unit, 
                u.usage_quantity
            FROM system.billing.usage u 
            INNER JOIN (
                SELECT DISTINCT account_id, workspace_id, warehouse_id, warehouse_name 
                FROM system.compute.warehouses
            ) w 
            ON u.account_id = w.account_id 
               AND u.workspace_id = w.workspace_id 
               AND u.usage_metadata.warehouse_id = w.warehouse_id
            WHERE billing_origin_product = 'SQL' 
              AND usage_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
            ORDER BY u.usage_start_time
        ),
        cte1 AS (
            SELECT 
                COALESCE(p.price_end_time, DATE_ADD(CURRENT_DATE(), 1)) AS coalesced_price_end_time, 
                cte.*, 
                COALESCE(cte.usage_quantity * p.pricing.effective_list.default, 0) AS usage_usd
            FROM system.billing.list_prices p
            LEFT JOIN cte 
              ON cte.sku_name = p.sku_name
             AND cte.usage_unit = p.usage_unit
            WHERE currency_code = 'USD'
        )
        SELECT *
        FROM cte1;
    """,
    'event': f"""
        WITH cte AS (
            SELECT 
                *,
                COALESCE(
                    LEAD(warehouses.change_time) OVER (
                        PARTITION BY warehouses.warehouse_id
                        ORDER BY warehouses.change_time ASC
                    ), CURRENT_TIMESTAMP()
                ) AS next_change_time 
            FROM system.compute.warehouses
        )
        SELECT
            warehouse_name, cluster_count, event_type, min_clusters, max_clusters, change_time, event_time, next_change_time
        FROM system.compute.warehouse_events
        INNER JOIN cte
          ON warehouse_events.warehouse_id = cte.warehouse_id 
         AND event_time BETWEEN change_time AND next_change_time
        WHERE warehouse_name = 'Customer-demo'
          AND DATE(event_time) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY event_time DESC;
    """,
    'warehouse_info': f"""
        SELECT * 
        FROM system.compute.warehouses
        WHERE DATE(change_time) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """
}


def run_query_rest(query: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    submit_url = f"https://{dbr_server_hostname}/api/2.0/sql/statements"
    payload = {
        "statement": query,
        "warehouse_id": dbr_warehouse_id
    }
    r = requests.post(submit_url, headers=headers, json=payload)
    r.raise_for_status()
    statement_id = r.json()["statement_id"]
    logger.info(f"Submitted query, statement id: {statement_id}")

    status_url = f"https://{dbr_server_hostname}/api/2.0/sql/statements/{statement_id}"
    while True:
        r = requests.get(status_url, headers=headers)
        r.raise_for_status()
        res = r.json()
        state = res["status"]["state"]
        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise Exception(f"Query {statement_id} failed with state: {state}")

    columns = []
    if "manifest" in res and "schema" in res["manifest"]:
        columns = [col["name"] for col in res["manifest"]["schema"]["columns"]]
    data_array = []
    if "result" in res and "data_array" in res["result"]:
        data_array = res["result"]["data_array"]

    return columns, data_array


def run_query_and_save_to_parquet(query: str, filename: str, output_dir: str):
    try:
        logger.info(f"Executing query for {filename} metadata")
        columns, data_array = run_query_rest(query)
        logger.info(f"Query returned {len(data_array)} row(s) with columns: {columns}")

        df = pd.DataFrame(data_array, columns=columns) if data_array else pd.DataFrame(columns=columns)

        if filename == 'tables' and 'created' in df.columns and 'last_altered' in df.columns:
            df['created'] = df['created'].astype(str)
            df['last_altered'] = df['last_altered'].astype(str)

        output_path = os.path.join(output_dir, f'{filename}.parquet')
        df.to_parquet(output_path, index=False)
        logger.info(f"Data written to {output_path}")
    except Exception as e:
        logger.error(f"Failed to execute query for {filename}: {e}")


def run_query_rest(query: str):
    headers = {
        'Authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    url = f"https://{dbr_server_hostname}/api/2.0/sql/statements"
    payload = {'statement': query, 'warehouse_id': dbr_warehouse_id}
    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    sid = r.json()['statement_id']
    logger.info(f"Submitted {sid}")

    status_url = f"{url}/{sid}"
    while True:
        r = requests.get(status_url, headers=headers)
        r.raise_for_status()
        j = r.json()
        state = j['status']['state']
        if state in ('SUCCEEDED','FAILED','CANCELED'):
            break
        time.sleep(1)
    if state != 'SUCCEEDED':
        raise RuntimeError(f"Query {sid} failed: {state}")

    cols = [c['name'] for c in j.get('manifest',{}).get('schema',{}).get('columns',[])]
    data = j.get('result',{}).get('data_array', [])
    return cols, data



def run_query_and_save(query: str, filename: str, outdir: str):
    cols, data = run_query_rest(query)
    df = pd.DataFrame(data, columns=cols) if data else pd.DataFrame(columns=cols)
    # convert timestamp columns for tables
    if filename=='tables' and 'created' in df.columns:
        df['created'] = df['created'].astype(str)
        df['last_altered'] = df['last_altered'].astype(str)
    path = os.path.join(outdir, f"{filename}.parquet")
    df.to_parquet(path, index=False)
    logger.info(f"Wrote {len(df)} rows to {path}")


def run_hourly_query_history(outdir: str):
    parquet_path = os.path.join(outdir, 'query_history.parquet')
    chunks = []
    current = start_date
    while current < end_date:
        next_hour = current + timedelta(hours=1)
        start_ts = current.strftime('%Y-%m-%d %H:%M:%S')
        end_ts = next_hour.strftime('%Y-%m-%d %H:%M:%S')
        q = (
            f"""SELECT * FROM system.query.history 
            WHERE start_time >= TIMESTAMP '{start_ts}' 
            AND start_time < TIMESTAMP '{end_ts}'"""
        )
        logger.info(f"Querying history from {start_ts} to {end_ts}")
        cols, data = run_query_rest(q)
        if data:
            df = pd.DataFrame(data, columns=cols)
            chunks.append(df)
        current = next_hour

    if chunks:
        result_df = pd.concat(chunks, ignore_index=True)
    else:
        result_df = pd.DataFrame(columns=cols if 'cols' in locals() else [])

    if os.path.exists(parquet_path):
        existing = pd.read_parquet(parquet_path)
        result_df = pd.concat([existing, result_df], ignore_index=True).drop_duplicates()
    result_df.to_parquet(parquet_path, index=False)
    logger.info(f"Appended history: total {len(result_df)} rows in {parquet_path}")


def extract_metadata(directory):
    output_dir = os.path.abspath(directory)
    os.makedirs(output_dir, exist_ok=True)
    for filename, query in queries.items():
        run_query_and_save_to_parquet(query, filename, output_dir)
    run_hourly_query_history(output_dir)
    logger.info("Databricks metadata extraction completed.")


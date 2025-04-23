from databricks import sql
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_metadata(directory):
    export_metadata = os.environ.get('EXPORT_METADATA', False)
    catalog = 'system'
    database = 'information_schema'
    access_token = os.environ.get('DBR_ACCESS_TOKEN')
    http_path = os.environ.get('DBR_WAREHOUSE_HTTP')
    dbr_server_hostname = os.environ.get('DBR_HOST')
    start_date_str = os.environ.get('QUERY_LOG_START')
    end_date_str = os.environ.get('QUERY_LOG_END')

    # parse date bounds
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date   = datetime.strptime(end_date_str,   '%Y-%m-%d') + timedelta(days=1)
    except Exception as e:
        logger.error(f"Invalid date format: {e}")
        raise

    output_dir = directory
    os.makedirs(output_dir, exist_ok=True)

    def create_DBR_connection():
        return sql.connect(
            server_hostname=dbr_server_hostname,
            http_path=http_path,
            access_token=access_token,
            schema=database,
            catalog=catalog
        )

    def create_DBR_con(retry_count=0):
        max_retries = 3
        logger.info(f"[{retry_count+1}/{max_retries}] Connecting to Databricks...")
        try:
            conn = create_DBR_connection()
            logger.info("Connected to Databricks SQL endpoint.")
            return conn
        except Exception as err:
            logger.error(f"Connection failed: {err}")
            if retry_count >= max_retries - 1:
                raise
            time.sleep(10)
            return create_DBR_con(retry_count + 1)

    conn = create_DBR_con()

    queries_metadata = {
        'tables': "SELECT * FROM system.information_schema.tables;",
        'columns': "SELECT * FROM system.information_schema.columns;",
        'views':   "SELECT * FROM system.information_schema.views;"
    }

    queries = {
        'usage': f"""
            WITH cte AS (
              SELECT u.*
              FROM system.billing.usage u
              JOIN (
                SELECT DISTINCT account_id,workspace_id,warehouse_id,warehouse_name
                FROM system.compute.warehouses
              ) w
                ON u.account_id = w.account_id
               AND u.workspace_id = w.workspace_id
               AND u.usage_metadata.warehouse_id = w.warehouse_id
             WHERE billing_origin_product = 'SQL'
               AND u.usage_date BETWEEN DATE('{start_date_str}') AND DATE('{end_date_str}')
            ),
            cte1 AS (
              SELECT
                COALESCE(p.price_end_time, DATE_ADD(CURRENT_DATE(),1)) AS coalesced_price_end_time,
                p.price_start_time,
                p.currency_code,
                p.pricing.effective_list.default AS price_per_unit,
                cte.*, 
                COALESCE(cte.usage_quantity * p.pricing.effective_list.default,0) AS usage_usd
              FROM system.billing.list_prices p
              LEFT JOIN cte
                ON cte.sku_name = p.sku_name
               AND cte.usage_unit = p.usage_unit
             WHERE p.currency_code = 'USD'
            )
            SELECT * FROM cte1
            ORDER BY usage_start_time;
        """,
        'event': f"""
            WITH cte AS (
              SELECT *,
                     COALESCE(
                       LEAD(change_time) OVER (PARTITION BY warehouse_id ORDER BY change_time),
                       CURRENT_TIMESTAMP()
                     ) AS next_change_time
              FROM system.compute.warehouses
            )
            SELECT ev.*
              FROM system.compute.warehouse_events ev
              JOIN cte ON ev.warehouse_id = cte.warehouse_id
                    AND ev.event_time BETWEEN cte.change_time AND cte.next_change_time
             WHERE DATE(ev.event_time) BETWEEN '{start_date_str}' AND '{end_date_str}'
             ORDER BY ev.event_time DESC;
        """,
        'warehouse_info': f"""
            SELECT *
              FROM system.compute.warehouses
             WHERE DATE(change_time) BETWEEN '{start_date_str}' AND '{end_date_str}';
        """,

    }

    def run_query_and_save(query, name):
        try:
            logger.info(f"Executing query for {name} metadata...")
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

            df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
            if name == 'tables' and 'created' in df.columns:
                df['created'] = df['created'].astype(str)
                df['last_altered'] = df['last_altered'].astype(str)

            path = os.path.join(output_dir, f"{name}.parquet")
            df.to_parquet(path, index=False)
            logger.info(f"Wrote {len(df)} rows to {path}")
        except Exception as e:
            logger.error(f"Error in {name}: {e}")

    if export_metadata:
        for name, q in queries_metadata.items():
            run_query_and_save(q, name)

    for name, q in queries.items():
        run_query_and_save(q, name)

    # hourly query history
    def run_hourly_query_history(outdir):
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
            with conn.cursor() as cur:
                cur.execute(query)
                data = cur.fetchall()
                cols = [d[0] for d in cur.description]

            if data:
                df = pd.DataFrame(data, columns=cols)

                filename = f"query_history_{current.strftime('%Y%m%d_%H')}.parquet"
                df.to_parquet(
                    os.path.join(outdir, filename),
                    index=False
                )
            current = next_hour

    run_hourly_query_history(output_dir)
    conn.close()
    logger.info("Databricks metadata extraction completed.")
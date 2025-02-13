import pandas as pd
from datetime import datetime
import os
import logging
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError
import trino

logger = logging.getLogger(__name__)

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger.setLevel(logging.INFO)


def extract_query_logs():
    auth_type = os.environ.get('TRINO_AUTH_TYPE')
    host = os.environ.get('TRINO_HOST')
    port = os.environ.get('TRINO_PORT')
    user = os.environ.get('TRINO_USER')
    password = os.environ.get('TRINO_PASSWORD')
    use_https = os.environ.get('TRINO_USE_HTTPS', 'false').lower() in ('true', '1', 'yes')
    ca_cert_path = os.environ.get('TRINO_CERT_PATH')
    key_path = os.environ.get('TRINO_KEY_PATH')
    parquet_output_dir = 'trino-query-logs'
    os.makedirs(parquet_output_dir, exist_ok=True)
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')

    logger.info("Connecting to Trino...")

    try:
        if auth_type == 'none':
            connection = connect(
                host=host,
                port=port,
                user=user,
                http_scheme='https' if use_https else 'http'
            )
        elif auth_type == 'BasicAuthentication':
            connection = connect(
                host=host,
                port=port,
                user=user,
                http_scheme='https' if use_https else 'http',
                auth=trino.auth.BasicAuthentication(user, password)
            )
        elif auth_type == 'OAuth2Authentication':
            connection = connect(
                host=host,
                port=port,
                user=user,
                http_scheme='https' if use_https else 'http',
                auth=trino.auth.OAuth2Authentication()
            )
        elif auth_type == 'CertificateAuthentication':
            connection = connect(
                host=host,
                port=port,
                user=user,
                http_scheme='https' if use_https else 'http',
                auth=trino.auth.CertificateAuthentication(ca_cert_path, key_path),
            )
        else:
            logger.error(f"Unknown TRINO_AUTH_TYPE: {auth_type}")
            return
        cursor = connection.cursor()
        logger.info("Connected to Trino")
        start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
        end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

        history_query = f"""
            select * from system.runtime.queries where date(created)>=date('{start_date.strftime('%Y-%m-%d')}') 
            and date(created)<=date('{end_date.strftime('%Y-%m-%d')}')
        """
        logger.info("Extracting Query logs...")
        cursor.execute(history_query)
        result = cursor.fetchall()
        df = pd.DataFrame(result)

        if df.empty:
            logger.info(f"No queries were found between {query_log_start} and {query_log_end}.")
        else:
            parquet_filename = f"{parquet_output_dir}/query_history_trino.parquet"
            df.to_parquet(parquet_filename, index=False)
            logger.info(f"Data has been exported to {os.path.basename(parquet_filename)}")
        logger.info(f"Query Log Successfully Exported to {parquet_output_dir}")
        connection.close()
    except TrinoQueryError as e:
        logger.error(f"Failed to extract query logs or encountered an error: {str(e)}")

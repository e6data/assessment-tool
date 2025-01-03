import pandas as pd
from datetime import datetime
import os
import trino
from pystarburst import Session
import logging

logger = logging.getLogger(__name__)

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger.setLevel(logging.INFO)


def extract_query_logs():
    host = os.environ.get('STARBURST_HOST')
    port = os.environ.get('STARBURST_PORT')
    user = os.environ.get('STARBURST_USER')
    password = os.environ.get('STARBURST_PASSWORD')
    catalog = os.environ.get('STARBURST_CATALOG')
    schema = os.environ.get('STARBURST_SCHEMA')
    query_log_start = os.environ.get('QUERY_LOG_START')
    query_log_end = os.environ.get('QUERY_LOG_END')

    parquet_output_dir = 'starburst-query-logs'
    os.makedirs(parquet_output_dir, exist_ok=True)

    db_parameters = {
        "host": host,
        "port": int(port),
        "http_scheme": "https",
        "auth": trino.auth.BasicAuthentication(user, password)
    }
    session_properties = {
        'catalog': catalog,
        'schema': schema
    }
    logger.info("Connecting to Starburst...")
    try:
        session = Session.builder.configs(db_parameters).configs(session_properties).create()
        logger.info("Connected to Starburst")
        start_date = datetime.strptime(query_log_start, '%Y-%m-%d')
        end_date = datetime.strptime(query_log_end, '%Y-%m-%d')

        history_query = f"""
                        SELECT * 
                        FROM galaxy_telemetry.public.query_history
                        WHERE "date" BETWEEN '{start_date.strftime('%Y-%m-%d')}' 
                                           AND '{end_date.strftime('%Y-%m-%d')}'
                        """
        logger.info("Extracting Query logs...")
        result = session.sql(history_query).collect()
        df = pd.DataFrame(result)

        if df.empty:
            logger.info(f"No queries were found between {query_log_start} and {query_log_end}.")
        else:
            df['session_properties'] = df['session_properties'].astype(str)
            df['create_time'] = df['create_time'].astype(str)
            df['execution_start_time'] = df['execution_start_time'].astype(str)
            df['end_time'] = df['end_time'].astype(str)

            parquet_filename = f"{parquet_output_dir}/query_history_starburst.parquet"
            df.to_parquet(parquet_filename, index=False)
            logger.info(f"Data has been exported to {os.path.basename(parquet_filename)}")
        logger.info(f"Query Log Successfully Exported to {parquet_output_dir}")
        session.close()

    except Exception as e:
        logger.error(f"Failed to extract query logs or encountered an error: {str(e)}")

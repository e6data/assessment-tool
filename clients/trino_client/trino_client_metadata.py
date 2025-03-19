import pandas as pd
import os
import logging
import trino
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError


MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_metadata(directory):
    auth_type = os.environ.get('TRINO_AUTH_TYPE')
    host = os.environ.get('TRINO_HOST')
    port = os.environ.get('TRINO_PORT')
    user = os.environ.get('TRINO_USER')
    password = os.environ.get('TRINO_PASSWORD')
    use_https = os.environ.get('TRINO_USE_HTTPS', 'false').lower() in ('true', '1', 'yes')
    ca_cert_path = os.environ.get('TRINO_CERT_PATH')
    key_path = os.environ.get('TRINO_KEY_PATH')
    catalog = os.environ.get('TRINO_CATALOG')
    schema = os.environ.get('TRINO_SCHEMA')
    export_stats = os.environ.get('COLUMN_STATS', 'false').lower() == 'true'
    parquet_output_dir = directory
    os.makedirs(parquet_output_dir, exist_ok=True)
    stats_output_path = os.path.join(parquet_output_dir, f'stats_{schema}.parquet')

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

        table_info = f"""SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type 
                         FROM {catalog}.information_schema.columns WHERE table_catalog='{catalog}'"""
        logger.info("Extracting Catalog info, it may take a few minutes...")
        cursor.execute(table_info)
        schema_info = cursor.fetchall()
        # schema_info = session.sql(table_info).collect()
        df_columns = pd.DataFrame(schema_info)
        schema_output_dir = os.path.join(parquet_output_dir, f'schema_info_{catalog}.parquet')
        df_columns.to_parquet(schema_output_dir, index=False)
        logger.info(f"Catalog information saved to {schema_output_dir}")

        if export_stats:
            logger.info(f"COLUMN_STATS is set to True. Exporting table and column stats for schema '{schema}'")
            metadata_tables_query = f"""SHOW TABLES FROM "{catalog}"."{schema}" """
            logger.info(f"Fetching tables from {schema}")
            cursor.execute(metadata_tables_query)
            tables_result = cursor.fetchall()
            df_tables = pd.DataFrame(tables_result)

            combined_stats = pd.DataFrame()

            if df_tables.empty:
                logger.info(f"No tables found in schema {schema}")
            else:
                for table_row in df_tables.itertuples():
                    table_name = table_row[1]
                    try:
                        analyze_stats = f"ANALYZE \"{catalog}\".\"{schema}\".\"{table_name}\""
                        cursor.execute(analyze_stats)
                        logger.info(f"Analysing stats for {table_name}")

                    except TrinoQueryError as e:
                        logger.error(f"Failed to fetch analyse {str(e)}")
                    metadata_stats_query = f"SHOW STATS FOR \"{catalog}\".\"{schema}\".\"{table_name}\""

                    logger.info(f"Fetching stats for {table_name}")
                    cursor.execute(metadata_stats_query)
                    stats_result = cursor.fetchall()
                    # stats_result = session.sql(metadata_stats_query).collect()
                    df_stats = pd.DataFrame(stats_result)

                    if not df_stats.empty:
                        df_stats['schema_name'] = schema
                        df_stats['table_name'] = table_name
                        combined_stats = pd.concat([combined_stats, df_stats], ignore_index=True)

                if not combined_stats.empty:
                    combined_stats.to_parquet(stats_output_path, index=False)
                    logger.info(f"Stats for schema '{schema}' saved to {stats_output_path}")
                else:
                    logger.info(f"No stats available for schema '{schema}'")

        connection.close()
    except TrinoQueryError as e:
        logger.error(f"Failed to extract query logs or encountered an error: {str(e)}")

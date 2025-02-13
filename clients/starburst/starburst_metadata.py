import pandas as pd
import os
import trino
from pystarburst import Session
import logging

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_metadata():
    host = os.environ.get('STARBURST_HOST')
    port = os.environ.get('STARBURST_PORT')
    user = os.environ.get('STARBURST_USER')
    password = os.environ.get('STARBURST_PASSWORD')
    catalog = os.environ.get('STARBURST_CATALOG')
    schema = os.environ.get('STARBURST_SCHEMA')
    export_stats = os.environ.get('COLUMN_STATS', 'false').lower() == 'true'
    parquet_output_dir = 'starburst-metadata'
    os.makedirs(parquet_output_dir, exist_ok=True)
    stats_output_path = os.path.join(parquet_output_dir, f'stats_{schema}.parquet')

    logger.info("Connecting to Starburst...")
    try:
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
        session = Session.builder.configs(db_parameters).configs(session_properties).create()
        logger.info("Connected to Starburst")

        table_info = f"""SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type 
                         FROM {catalog}.information_schema.columns WHERE table_catalog='{catalog}'"""
        logger.info("Extracting Catalog info, it may take a few minutes...")
        schema_info = session.sql(table_info).collect()
        df_columns = pd.DataFrame(schema_info)
        schema_output_dir = os.path.join(parquet_output_dir, f'schema_info_{catalog}.parquet')
        df_columns.to_parquet(schema_output_dir, index=False)
        logger.info(f"Catalog information saved to {schema_output_dir}")

        if export_stats:
            logger.info(f"COLUMN_STATS is set to True. Exporting table and column stats for schema '{schema}'")
            metadata_tables_query = f"""SHOW TABLES FROM "{schema}" """
            logger.info(f"Fetching tables from {schema}")
            tables_result = session.sql(metadata_tables_query).collect()
            df_tables = pd.DataFrame(tables_result)

            combined_stats = pd.DataFrame()

            if df_tables.empty:
                logger.info(f"No tables found in schema {schema}")
            else:
                for table_row in df_tables.itertuples():
                    table_name = table_row[1]
                    analyze_stats = f"ANALYZE \"{catalog}\".\"{schema}\".\"{table_name}\""
                    session.sql(analyze_stats)
                    logger.info(f"Running Analyse on {table_name}")
                    metadata_stats_query = f"SHOW STATS FOR \"{schema}\".\"{table_name}\""

                    logger.info(f"Fetching stats for {table_name}")
                    stats_result = session.sql(metadata_stats_query).collect()
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

        session.close()
    except Exception as e:
        logger.error(f"Error extracting Starburst metadata: {str(e)}")

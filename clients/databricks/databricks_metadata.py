from databricks import sql
import pandas as pd
from datetime import datetime
import os
import time
import logging


def extract_metadata(directory):
    catalog = 'system'
    database = 'information_schema'
    access_token = os.environ.get('DBR_ACCESS_TOKEN')
    http_path = os.environ.get('DBR_WAREHOUSE_ID')
    dbr_server_hostname = os.environ.get('DBR_HOST')
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    csv_output_dir = directory
    os.makedirs(csv_output_dir, exist_ok=True)
    def create_DBR_connection():
        return sql.connect(
            server_hostname=dbr_server_hostname,
            http_path=http_path,
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
                'TIMESTAMP : {} Connected with database {} and catalog {} in {} seconds'.format(datetime.now(),
                                                                                                 database, catalog,
                                                                                                 time.time() - now))
            return dbr_connection
        except Exception as e:
            logger.error(e)
            logger.error(
                'TIMESTAMP : {} Failed to connect to the DBR database with {}'.format(datetime.now(),
                                                                                      database))
            if retry_count >= max_retry_count:
                raise e
            logger.error('Retrying connection in 10 seconds...')
            time.sleep(10)
            retry_count += 1
            return create_DBR_con(retry_count=retry_count)

    try:
        logger.info("Creating connection with Databricks")
        conn = create_DBR_con()

        queries = {
            'tables': "SELECT * FROM system.information_schema.tables;",
            'columns': "SELECT * FROM system.information_schema.columns;",
            'views': "SELECT * FROM system.information_schema.views;"
        }

        logger.info("Connected to Databricks.")

        def run_query_and_save_to_csv(query, parquet_filename):
            try:
                logger.info(f"Executing query for {parquet_filename} metadata")
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]

                result_df = pd.DataFrame(result, columns=columns)
                if query == 'tables':
                    result_df['created']=result_df['created'].astype(str)
                    result_df['last_altered'] = result_df['last_altered'].astype(str)
                output_path = os.path.join(csv_output_dir, f'{parquet_filename}.parquet')
                result_df.to_parquet(output_path, index=False)
                logger.info(f"Data written to {parquet_filename}")
            except Exception as e:
                logger.error(f"Failed to execute query for {parquet_filename}: {e}")

        for parquet_filename, query in queries.items():
            run_query_and_save_to_csv(query, parquet_filename)

        conn.close()
        logger.info("Databricks metadata extraction completed.")

    except Exception as e:
        logger.error(f"Error extracting Databricks metadata: {str(e)}")

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


def extract_metadata(directory):
    host = os.environ.get('REDSHIFT_HOST')
    user = os.environ.get('REDSHIFT_USER')
    password = os.environ.get('REDSHIFT_PASSWORD')
    port = os.environ.get('REDSHIFT_PORT')
    database = os.environ.get('REDSHIFT_DATABASE')
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
                    'types': f"""SELECT n.nspname as "Schema", pg_catalog.format_type(t.oid, NULL) AS "Name", pg_catalog.obj_description(t.oid, 'pg_type') as "Description" 
                                    FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace 
                                    WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) 
                                    AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem ) AND pg_catalog.pg_type_is_visible(t.oid) 
                                    ORDER BY 1, 2""",
                    'aggregates': f"""SELECT n.nspname as "Schema", p.proname AS "Name", pg_catalog.format_type(p.prorettype, NULL) AS "Result_data_type", 
                                        CASE WHEN p.pronargs = 0 THEN CAST('*' AS pg_catalog.text) ELSE oidvectortypes(p.proargtypes) END AS "Argument_data_types", pg_catalog.obj_description(p.oid, 'pg_proc') as "Description" 
                                        FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace 
                                        WHERE p.proisagg AND pg_catalog.pg_function_is_visible(p.oid) ORDER BY 1, 2, 4""",
                    'casts': f"""SELECT pg_catalog.format_type(castsource, NULL) AS "Source_type", pg_catalog.format_type(casttarget, NULL) AS "Target_type", 
                                    CASE WHEN castfunc = 0 THEN '(binary coercible)' ELSE p.proname END as "Function", CASE WHEN c.castcontext = 'e' THEN 'no' WHEN c.castcontext = 'a' THEN 'in assignment' ELSE 'yes' END as "Implicit?"
                                    FROM pg_catalog.pg_cast c LEFT JOIN pg_catalog.pg_proc p ON c.castfunc = p.oid LEFT JOIN pg_catalog.pg_type ts ON c.castsource = ts.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = ts.typnamespace 
                                    LEFT JOIN pg_catalog.pg_type tt ON c.casttarget = tt.oid LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid = tt.typnamespace 
                                    WHERE ( (true  AND pg_catalog.pg_type_is_visible(ts.oid)) OR (true  AND pg_catalog.pg_type_is_visible(tt.oid)) ) ORDER BY 1, 2""",
                    'columns': f"""select * from information_schema.columns""",
                    'table_def': f"""select * from pg_table_def""",
                    'views': f"""select * from pg_views""",
                    'user': f"""select * from pg_user"""

        }

        logger.info("Extracting Metadata...")

        for csv_filename, query in queries.items():
            run_query_and_save_to_csv(cursor, query, csv_filename, csv_output_dir)
        cursor.close()
        conn.close()
        logger.info("Connection Closed.")

        logger.info(f"Metadata Successfully Exported to {os.path.abspath(csv_output_dir)}")

    except Exception as e:
        logger.error(f"Failed to connect Redshift: {e}")

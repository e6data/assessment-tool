import pandas as pd
import pymssql
import os
import logging

logger = logging.getLogger(__name__)


def run_query_and_save_to_parquet(cursor, query, parquet_filename, parquet_output_dir):
    try:
        logger.info(f"Executing query for {parquet_filename} metadata")
        cursor.execute(query)
        result = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(result, columns=columns)

        output_path = os.path.join(parquet_output_dir, f'{parquet_filename}.parquet')
        df.to_parquet(output_path, index=False)
        logger.info(f"Data written to {parquet_filename}")
    except Exception as e:
        logger.error(f"Failed to execute query for {parquet_filename}: {e}")


def extract_metadata(directory):
    server = os.environ.get('MSSQL_SERVER')
    port = os.environ.get('MSSQL_PORT')
    user = os.environ.get('MSSQL_USER')
    password = os.environ.get('MSSQL_PASSWORD')
    database = os.environ.get('MSSQL_DATABASE')
    parquet_output_dir = directory
    os.makedirs(parquet_output_dir, exist_ok=True)
    logger.info("Connecting to MSSQL Server...")
    try:
        conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            port=port,
            as_dict=True
        )
        cursor = conn.cursor()
        logger.info("Connected to MSSQL Server")
        queries = {
            'schema': """select tbl.table_schema , tbl.table_name,col.column_name, col.data_type, a.rows
                        FROM information_schema.tables tbl
                        INNER JOIN information_schema.columns col 
                            ON col.table_name = tbl.table_name
                            AND col.table_schema = tbl.table_schema
                        join 
                        (SELECT 
                                t.name AS TableName,
                                p.rows
                            FROM sys.tables t
                            INNER JOIN sys.partitions p ON t.object_id = p.object_id
                            WHERE p.index_id IN (0, 1)) a
                            on tbl.table_name=a.TableName
                            ORDER BY TableName;""",
            'column_min_max': """DECLARE @sql NVARCHAR(MAX);
                            SET @sql = N'';
                            
                            SELECT @sql += 
                                'SELECT ''' + t.name + ''' AS TableName, ''' + c.name + ''' AS ColumnName, 
                                        MIN(' + c.name + ') AS MinValue, 
                                        MAX(' + c.name + ') AS MaxValue 
                                 FROM ' + s.name + '.' + t.name + ' UNION ALL '
                            FROM sys.tables t
                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                            JOIN sys.columns c ON t.object_id = c.object_id
                            JOIN sys.types tp ON c.user_type_id = tp.user_type_id
                            WHERE tp.name IN ('int', 'bigint', 'smallint', 'decimal', 'numeric', 'float', 'real');
                            
                            SET @sql = LEFT(@sql, LEN(@sql) - 10);
                            
                            EXEC sp_executesql @sql;""",
        }
        for parquet_filename, query in queries.items():

            run_query_and_save_to_parquet(cursor, query, parquet_filename, parquet_output_dir)
        cursor.close()
        conn.close()
        logger.info("Connection Closed")
    except Exception as e:
        logger.error(f"Error extracting Snowflake metadata: {str(e)}")

# e6data Pilot Assessment Tool
The Pilot Assessment tool enables the extraction of metadata and query history (including query metrics) from a existing data
warehouse for further analysis.
```
Metadata: database name, database size, table name, table size, row count, partition info, external databases/tables info
Query Logs: query text, query plan, query metrics ( execution time, planning time, byte scanned, etc.)
```
### Steps to Run the Script:

#### Databricks

Pre Assessment Requirements:

- Databricks Permission to view system.information_schema
- Python 3.9 or above
- Pandas, Databricks SQL Python connector


> pip install pandas \
> pip install databricks-sql-connector

Set environment variables pertaining to your databricks configurations (host, access_token, warehouse_id, etc.)


> export DBR_HOST=<databricks_host> \
> export DBR_WAREHOUSE_ID=<warehouse_id> \
> export DBR_ACCESS_TOKEN=<databrciks_token> \
> export query_log_start='YYYY-MM-DD' (Example 2024-10-11) \
> export query_log_end='YYYY-MM-DD' (Example 2024-10-15)

To run the assessment script :
> python3 clients/main.py databricks


#### Snowflake
Pre Assessment Requirements:

- Snowflake role to view query history and information schema (ACCOUNT_ADMIN preferred)
- Python 3.9 or above
- Pandas, Snowflake SQL Python connector

> pip install pandas \
> pip install snowflake-connector-python


Set environment variables pertaining to your snowflake configurations (host, warehouse, role, user, password, etc.)
> export SNOWFLAKE_HOST=<snowflake_host> \
> export SNOWFLAKE_WAREHOUSE=<warehouse_name> \
> export SNOWFLAKE_ROLE='ACCOUNTADMIN' \
> export SNOWFLAKE_USER=<snowflake_username> \
> export SNOWFLAKE_PASSWORD=<snowflake_password> \
> export QUERY_LOG_START='YYYY-MM-DD' \
> export QUERY_LOG_END='YYYY-MM-DD'

To run the assessment script :

> python3 clients/main.py snowflake

#### Starburst 
Pre Assessment Requirements:

- Starburst permissions to view query history
- Python 3.9 or above
- Pandas, Starburst SQL Python connector


> pip install pandas \
> pip install pystarburst


Set environment variables pertaining to your starburst configurations (host, user, password, catalog, etc.) 

> export STARBURST_HOST=<starburst_host> \
> export STARBURST_PORT=<starburst_port> \
> export STARBURST_USER=<starburst_username> \
> export STARBURST_PASSWORD=<starburst_password> \
> export STARBURST_CATALOG=<catalog_name> \
> export STARBURST_SCHEMA=<schema_name> \
> export QUERY_LOG_START='YYYY-MM-DD' \
> export QUERY_LOG_END='YYYY-MM-DD' \
> export COLUMN_STATS=true
```
Set `COLUMN_STATS=true` to extract both schema and column statistics, or `COLUMN_STATS=false` to extract only schema information.
```
To run the assessment script :

> python3 clients/main.py starburst

#### MSSQL Server
Pre Assessment Requirements:

- MSSQL server permission to view System tables and Information Schema
- Python 3.9 or above
- Pandas, MSSQL Python connector


> pip install pandas \
> pip install pymssql


Set environment variables pertaining to your MSSQL server configurations (server, user, password, catalog, etc.) 

> export MSSQL_SERVER=<mssql_server> \
> export MSSQL_PORT=<mssql_port> \
> export MSSQL_USER=<mssql_username> \
> export MSSQL_PASSWORD=<mssql_password> \
> export MSSQL_DATABASE=<mssql_database> \
> export QUERY_LOG_START='YYYY-MM-DD' \
> export QUERY_LOG_END='YYYY-MM-DD'

To run the assessment script :

> python3 clients/main.py mssql
>
```
Two directories named <client>-metadata for Metadata and <client>-query-logs for Query Logs will be generated with the help of above script.
```
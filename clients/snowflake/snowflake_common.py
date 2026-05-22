import os
import time
import logging
import snowflake.connector
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DEFAULT_DATABASE = 'SNOWFLAKE'
DEFAULT_SCHEMA = 'ACCOUNT_USAGE'
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 10


def _resolve_passphrase():
    passphrase = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') or None
    fmt = os.environ.get('SNOWFLAKE_PASSPHRASE_INPUT_FORMAT') or 'STRING'
    if passphrase and fmt == 'TEXT_FILE':
        with open(passphrase) as f:
            passphrase = f.read().strip()
    return passphrase


def _connect_once():
    host = os.environ.get('SNOWFLAKE_ACCOUNT_IDENTIFIER')
    user = os.environ.get('SNOWFLAKE_USER')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    auth_type = os.environ.get('SNOWFLAKE_AUTH_TYPE')

    common_kwargs = dict(
        user=user,
        account=host,
        warehouse=warehouse,
        database=DEFAULT_DATABASE,
        schema=DEFAULT_SCHEMA,
    )

    if auth_type == 'USERNAME_PASSWORD':
        password = os.environ.get('SNOWFLAKE_PASSWORD')
        return snowflake.connector.connect(password=password, **common_kwargs)
    if auth_type == 'KEY_PAIR':
        private_key = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH')
        passphrase = _resolve_passphrase()
        return snowflake.connector.connect(
            private_key_file=private_key,
            private_key_file_pwd=passphrase,
            **common_kwargs,
        )
    raise ValueError(
        f"Invalid SNOWFLAKE_AUTH_TYPE: {auth_type!r}. "
        "Must be 'USERNAME_PASSWORD' or 'KEY_PAIR'."
    )


def connect_with_retry(max_retries=DEFAULT_MAX_RETRIES,
                       retry_delay_seconds=DEFAULT_RETRY_DELAY_SECONDS):
    attempt = 0
    while True:
        attempt += 1
        try:
            logger.info(f"[{attempt}/{max_retries}] Creating connection with Snowflake")
            conn = _connect_once()
            logger.info("Connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Connection attempt {attempt} failed: {e}")
            if attempt >= max_retries:
                raise
            time.sleep(retry_delay_seconds)


def use_role(cursor, role):
    cursor.execute(f"USE ROLE {role};")
    logger.info(f"Using {role}")


def close_quietly(cursor, conn):
    if cursor is not None:
        try:
            cursor.close()
        except Exception as e:
            logger.warning(f"Error closing cursor: {e}")
    if conn is not None:
        try:
            conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")


def write_parquet(df, path):
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.round("ms")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, coerce_timestamps='ms')

import boto3
import os
from datetime import datetime, timezone
import pandas as pd
from pandas import json_normalize

MAX_ATHENA_BATCH_SIZE = 50
OUTPUT_FILE = 'athena_query_logs.parquet'
athena_client = boto3.client(
    'athena',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=os.environ.get('AWS_SESSION_TOKEN')
)


def get_execution_ids():
    query_params = {}
    while True:
        response = athena_client.list_query_executions(**query_params)
        yield from response['QueryExecutionIds']
        if 'NextToken' not in response:
            break
        query_params['NextToken'] = response['NextToken']


def get_query_executions(ids):
    return athena_client.batch_get_query_execution(
        QueryExecutionIds=ids
    )['QueryExecutions']


def fetch_and_flatten(start_date, end_date, output_dir):
    execution_ids = []
    records = []
    stop_fetching = False
    batch_counter = 1

    for qid in get_execution_ids():
        if stop_fetching:
            break

        execution_ids.append(qid)

        if len(execution_ids) == MAX_ATHENA_BATCH_SIZE:
            print(f"Processing batch {batch_counter}")
            should_stop = process_batch(execution_ids, records, start_date, end_date, batch_counter)
            if should_stop:
                stop_fetching = True
            execution_ids = []
            batch_counter += 1

    if execution_ids and not stop_fetching:
        print(f"Processing final batch {batch_counter}")
        process_batch(execution_ids, records, start_date, end_date, batch_counter)

    save_results(records, output_dir)


def process_batch(execution_ids, records, start_date, end_date, batch_counter):
    should_stop = False
    try:
        stats = get_query_executions(execution_ids)
        for record in stats:
            try:
                submission_dt = record['Status']['SubmissionDateTime']
                if submission_dt.tzinfo is not None:
                    submission_dt = submission_dt.astimezone(timezone.utc).replace(tzinfo=None)

                if submission_dt > end_date:
                    continue

                if submission_dt < start_date:
                    should_stop = True
                    break

                flat = json_normalize(record, sep='.')
                records.append(flat)

            except KeyError as ke:
                print(f"Skipping record with missing fields: {ke}")
            except Exception as e:
                print(f"Error processing record: {e}")

        return should_stop

    except Exception as batch_error:
        print(f"Batch {batch_counter} failed: {batch_error}")
        return False


def save_results(records, output_dir):
    if records:
        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        df = pd.concat(records, ignore_index=True)
        datetime_cols = df.select_dtypes(include=['datetime']).columns
        for col in datetime_cols:
            df[col] = df[col].apply(lambda x: x.isoformat() if not pd.isna(x) else '')

        # Build full output path
        output_path = os.path.join(output_dir, OUTPUT_FILE)
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} records to {output_path}")
    else:
        print("No records to save")


# def extract_query_logs(directory):
#     start_date_query = datetime(2025, 5, 1, tzinfo=timezone.utc).replace(tzinfo=None)
#     end_date_query = datetime(2025, 5, 20, tzinfo=timezone.utc).replace(tzinfo=None)
#
#     fetch_and_flatten(start_date_query, end_date_query, directory)


def extract_query_logs(directory):
    # Get dates from environment variables
    start_date_str = os.environ.get('QUERY_LOG_START')
    end_date_str = os.environ.get('QUERY_LOG_END')

    if not start_date_str or not end_date_str:
        raise ValueError("Both QUERY_LOG_START and QUERY_LOG_END must be set in environment variables")

    try:
        start_date = datetime.fromisoformat(start_date_str)
        end_date = datetime.fromisoformat(end_date_str)

        if start_date.tzinfo:
            start_date = start_date.astimezone(timezone.utc).replace(tzinfo=None)
        if end_date.tzinfo:
            end_date = end_date.astimezone(timezone.utc).replace(tzinfo=None)

    except ValueError as e:
        raise ValueError(
            f"Invalid date format: {e}. Use ISO format (e.g., '2025-05-20')")

    fetch_and_flatten(start_date, end_date, directory)



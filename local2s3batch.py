from datetime import datetime
import pandas as pd
from io import StringIO
from airflow import DAG
import boto3
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Constants
FILE_PATH = '/home/growlt246/airflow/dags/adult.data'  # Update with the correct file path
BATCH_SIZE = 1000
S3_BUCKET = 'nikunj-input'
LAST_PROCESSED_LINE_KEY = 'last_processed_line'

# Function to get the AWS connection
def get_aws_client():
    aws_connection = BaseHook.get_connection('aws_default')
    return boto3.client(
        's3',
        aws_access_key_id=aws_connection.login,
        aws_secret_access_key=aws_connection.password
    )

# Python Function to read data from a local CSV file in batches and upload to S3
def read_and_upload_csv(**kwargs):
    ti = kwargs['ti']
    
    # Get the last processed line from Airflow Variable
    last_processed_line = int(Variable.get(LAST_PROCESSED_LINE_KEY, default_var=0))
    print(f"Last processed line: {last_processed_line}")
    print(f"Batch size: {BATCH_SIZE}")
    
    # Read the next batch of 1000 lines
    chunk = pd.read_csv(FILE_PATH, skiprows=range(1, last_processed_line + 1), nrows=BATCH_SIZE)
    print(f"Read {len(chunk)} lines")
    
    # Update the last processed line
    new_last_processed_line = last_processed_line + len(chunk)
    Variable.set(LAST_PROCESSED_LINE_KEY, new_last_processed_line)
    
    # Convert the chunk DataFrame to CSV
    csv_buffer = StringIO()
    chunk.to_csv(csv_buffer, index=False)
    
    # Generate the new S3 key with the current timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_key = f'data/chunk_Data_{timestamp}.csv'
    
    # Push the CSV content and the S3 key to XCom
    ti.xcom_push(key='batch_data', value=csv_buffer.getvalue())
    ti.xcom_push(key='s3_key', value=s3_key)

# Create a task to upload the CSV to S3
def upload_to_s3(ti):
    s3_client = get_aws_client()
    data = ti.xcom_pull(task_ids='Read-Local-CSV', key='batch_data')
    s3_key = ti.xcom_pull(task_ids='Read-Local-CSV', key='s3_key')
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=data
    )

with DAG(
    dag_id="local2s3_with_timestamp",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False
) as dag:

    read_csv_task = PythonOperator(
        task_id='Read-Local-CSV',
        python_callable=read_and_upload_csv,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id='Upload-to-S3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

    read_csv_task >> upload_task

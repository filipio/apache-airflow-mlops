import os
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import boto3

BUCKET_NAME = 'azdzpiechaczek'
DATA_FILE_NAME = 'athena/Unsaved/2023/11/13/0633d6c1-17a8-426d-8d71-9d19a9d5ced9.txt' # TODO change this to how the file will be called in S3

@task(task_id="download_data_from_s3")
def download_data():
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=os.environ['AWS_SESSION_TOKEN'],
    )

    s3 = session.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)

    data_str = ''
    for fileobj in bucket.objects.filter(Prefix=DATA_FILE_NAME):
        data_str = fileobj.get()['Body'].read().decode('utf-8')

    with open(f'/mnt/shared/data.csv', 'w') as f:
        f.write(data_str)


with DAG(
        dag_id="s3_extract",
        start_date=datetime(2023, 12, 11),
        schedule_interval=None,
        catchup=False,
) as dag:
    download_task = download_data()
    download_task

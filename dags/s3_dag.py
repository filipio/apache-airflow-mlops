import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

# TODO: sth is wrong with token in connection, currently below dag is not working
AWS_S3_CONN_ID = "my_aws_connection" # name of connection, the same as in setup.py and the secret


def list_keys():
    print(AWS_S3_CONN_ID)
    hook = S3Hook(aws_conn_id=AWS_S3_CONN_ID)
    bucket = "awesome-airflow-bucket"
    prefix = ""
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")


with DAG(
        dag_id="s3_extract",
        start_date=datetime(2023, 12, 11),
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:
    t1 = PythonOperator(task_id="s3_list_keys", python_callable=list_keys)

    t1

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import boto3
import logging
import time

BUCKET_NAME = 'ab-aws-de-labs'
AWS_REGION = 'ap-south-1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

def check_files_in_s3(prefix):
    s3 = boto3.client('s3', region_name=AWS_REGION)
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    for obj in response.get('Contents', []):
        if obj['Size'] > 0:
            logging.info(f"File found: {obj['Key']}")
            return True

    return False

def check_all_files():
    if (
        check_files_in_s3('spotify_data/user-streams/') and
        check_files_in_s3('spotify_data/songs/') and
        check_files_in_s3('spotify_data/users/')
    ):
        return 'trigger_spark_job'
    return 'skip_execution'

def wait_for_glue_job_completion(job_name, poll_interval=60):
    client = boto3.client('glue', region_name=AWS_REGION)

    while True:
        response = client.get_job_runs(JobName=job_name, MaxResults=1)
        runs = response.get('JobRuns', [])

        if not runs:
            logging.info(f"No previous runs for {job_name}")
            return

        state = runs[0]['JobRunState']
        if state in ['STARTING', 'RUNNING', 'STOPPING']:
            logging.info(f"{job_name} still running...")
            time.sleep(poll_interval)
        else:
            logging.info(f"{job_name} finished with state: {state}")
            return

def trigger_glue_job(job_name):
    client = boto3.client('glue', region_name=AWS_REGION)
    wait_for_glue_job_completion(job_name)
    client.start_job_run(JobName=job_name)
    logging.info(f"Triggered Glue job: {job_name}")

def move_files_to_archived():
    s3 = boto3.client('s3', region_name=AWS_REGION)
    source_prefix = 'spotify_data/user-streams/'
    dest_prefix = 'spotify_data/user-streams-archived/'

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=source_prefix)

    for obj in response.get('Contents', []):
        src = obj['Key']
        dst = src.replace(source_prefix, dest_prefix)

        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource={'Bucket': BUCKET_NAME, 'Key': src},
            Key=dst
        )
        s3.delete_object(Bucket=BUCKET_NAME, Key=src)

with DAG(
    dag_id='process-songs-metrics',
    start_date=datetime(2024, 1, 1),
    schedule='*/5 * * * *',
    catchup=False,
    default_args=default_args,
    description='Trigger Glue jobs when new files arrive and archive processed data'
) as dag:

    check_files = BranchPythonOperator(
        task_id='check_files',
        python_callable=check_all_files
    )

    trigger_spark_job = PythonOperator(
        task_id='trigger_spark_job',
        python_callable=trigger_glue_job,
        op_args=['calculate_metrics_etl1']
    )

    wait_spark = PythonOperator(
        task_id='wait_for_spark_job',
        python_callable=lambda: wait_for_glue_job_completion('calculate_metrics_etl1')
    )

    trigger_python_job = PythonOperator(
        task_id='trigger_python_job',
        python_callable=trigger_glue_job,
        op_args=['insert_metrics_dynamo1']
    )

    wait_python = PythonOperator(
        task_id='wait_for_python_job',
        python_callable=lambda: wait_for_glue_job_completion('insert_metrics_dynamo1')
    )

    move_files = PythonOperator(
        task_id='move_files_to_archived',
        python_callable=move_files_to_archived,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    skip_execution = EmptyOperator(
        task_id='skip_execution'
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    check_files >> [trigger_spark_job, skip_execution]
    trigger_spark_job >> wait_spark >> trigger_python_job >> wait_python >> move_files >> end
    skip_execution >> end

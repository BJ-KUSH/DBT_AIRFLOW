from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
##This used to load a script from a different directory
##In our use case the script to load data into snowflake is located in a subfolder inside the dags folder
import configparser
import os
import sys
sys.path.append('/home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/airflow-code/dags')
from source_load.load_data import run_script
from alerting.slack_alert import task_success_slack_alert
from alerting.slack_alert import task_fail_slack_alert
import boto3


def get_aws_credentials():
    # Path to the AWS credentials file
    aws_credentials_path = os.path.expanduser("~/.aws/credentials")

    # Check if the file exists
    if not os.path.exists(aws_credentials_path):
        raise FileNotFoundError("AWS credentials file not found.")

    # Parse the AWS credentials file
    config = configparser.ConfigParser()
    config.read(aws_credentials_path)

    # Retrieve the AWS credentials
    aws_access_key_id = config.get("default", "aws_access_key_id")
    aws_secret_access_key = config.get("default", "aws_secret_access_key")

    return aws_access_key_id, aws_secret_access_key

aws_access_key_id, aws_secret_access_key = get_aws_credentials()

def send_sns_message(context):
    sns_client = boto3.client('sns',region_name='us-east-1')

    # Publish the message
    response = sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:449102749440:Netflix_failure:214b6d60-de0d-41ad-a0cd-a56cfe8f57fc',
        Message="Netflix_Data_Analytics DAG failed"
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 12),
    'email': ['brijenderkumar752@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback':task_fail_slack_alert
    #'on_failure_callback': send_sns_message
}

dag = DAG(
    dag_id='Netflix_Data_Analytics',
    default_args=default_args,
    description='This dag runs data analytics on top of netflix datasets',
    schedule_interval=timedelta(days=1),
)

credits_sensor = S3KeySensor(
    task_id='credits_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/credits.csv',
    wildcard_match=True,
    bucket_name='netflix-data-analytics-bj',
    aws_conn_id='bj_aws',
    dag=dag
)

titles_sensor = S3KeySensor(
    task_id='titles_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/titles.csv',
    wildcard_match=True,
    bucket_name='netflix-data-analytics-bj',
    aws_conn_id='bj_aws',
    dag=dag
)
load_data_snowflake = PythonOperator(task_id='Load_Data_Snowflake'
    ,python_callable=run_script,
    dag=dag)

run_stage_models = BashOperator(
    task_id='run_stage_models',
    bash_command='/home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/test_dbt_env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/netflix_project --profile Netflix --target dev',
    dag=dag
)

run_fact_dim_models = BashOperator(
    task_id='run_fact_dim_models',
    bash_command='/home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/test_dbt_env/bin/dbt run --model tag:"FACT" --project-dir /home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/netflix_project --profile Netflix --target prod',
    dag=dag
)

run_test_cases = BashOperator(
    task_id='run_test_cases',
    bash_command='/home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/test_dbt_env/bin/dbt test --model tag:"TEST" --project-dir /home/brijenderkushwaha/DBT/DBT_PROJECTS/DBT_AIRFLOW_PROJECT/dbt-code/netflix_project --profile Netflix --target prod',
    dag=dag
)

slack_success_alert=task_success_slack_alert(dag=dag)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> credits_sensor >> titles_sensor >> load_data_snowflake >> run_stage_models>> run_fact_dim_models >> run_test_cases >> slack_success_alert >> end_task

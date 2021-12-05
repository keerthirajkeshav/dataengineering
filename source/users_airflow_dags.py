from datetime import datetime, timedelta, date
import os

import airflow
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.exceptions import AirflowException


DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2021,10,10),
    'email': ['keerthirajkeshav@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Users',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='* 8 * * *',
    max_active_runs = 1,
    concurrency=1
)

run_this = BashOperator(
    task_id = 'Users',
    bash_command = 'python3 /opt/airflow/users.py ' + os.environ["ENV"],
    dag=dag)

run_this
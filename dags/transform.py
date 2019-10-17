import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.custom_plugin import S3DataExistsOperator
from airflow.operators import S3DataExistsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 5),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

s3data = 's3://psp-capstone/raw/'
s3bucket = 'psp-capstone'
lookup_prefix = 'psp-capstone/raw/'

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('dag_cluster',schedule_interval='@monthly', default_args=default_args)

#Variable.set("dag_analytics_state", "na")
#Variable.set("dag_transform_state", "na")

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


tip_transform = BashOperator(
    task_id='tip_transform',
    bash_command='python /Users/pranavprajapati/Desktop/Projects_2019/Capstone_Pipeline/dags/transform/tip.py',
    dag=dag)



start_operator >> tip_transform

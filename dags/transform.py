import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 5),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

#Edit according to your bucket and paths
s3data = 's3://psp-capstone/raw/'
s3bucket = 'psp-capstone'
lookup_prefix = 'psp-capstone/raw/'

dag = DAG('trans_parquet',schedule_interval='@monthly', default_args=default_args)

done_operator = DummyOperator(task_id='Done_execution',  dag=dag)

# All Bash Operators- add file directory
review_transform = BashOperator(
    task_id='review_transform',
    bash_command='python /airflow/dags/transform/review.py',
    dag=dag)

business_transform = BashOperator(
    task_id='business_transform',
    bash_command='python /airflow/dags/transform/business.py',
    dag=dag)

restaurant_create = BashOperator(
    task_id='restaurant_create',
    bash_command='python /airflow/dags/transform/restaurant.py',
    dag=dag)

quality_transform2 = BashOperator(
    task_id='transform_quality_check2',
    bash_command='python /airflow/dags/transform/second_quality_check.py',
    dag=dag)

business_transform >>restaurant_create >>review_transform >>quality_transform2 >>done_operator

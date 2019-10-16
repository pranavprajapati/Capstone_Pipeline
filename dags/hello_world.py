import datetime
from datetime import datetime, timedelta
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def hello_world():
    logging.info("Hello World")

#
# TODO: Add a daily `schedule_interval` argument to the following DAG
#
dag = DAG(
        "test",
        default_args=default_args,
        schedule_interval = '@daily')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)

start_operator >> task

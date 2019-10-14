import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ClusterCheckSensor
from airflow.operators import ClusterCheckSensor
from airflow import AirflowException



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('transform_dag', concurrency=2, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

#
def submit_to_emr(**kwargs):
    cluster_id = Variable.get("cluster_id")
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url, kwargs['params']['file'])
    logs = emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)
    if kwargs['params']['log']:
        for line in logs:
            logging.info(line)
            if 'FAIL' in str(line):
                logging.info(line)
                raise AirflowException("Normalize data Quality check Fail!")

#
def dag_done(**kwargs):
    Variable.set("dag_transform_state", "done")


transform_business = PythonOperator(
    task_id='transform_business',
    python_callable = submit_to_emr,
    params={
        "file" : '/root/airflow/dags/transform/business.py',
        "log" : False
    },
    dag=dag
)

transform_checkin = PythonOperator(
    task_id='transform_checkin',
    python_callable = submit_to_emr,
    params={
        "file" : '/root/airflow/dags/transform/checkin.py',
        "log" : False
    },
    dag=dag
)

transform_restaurants = PythonOperator(
    task_id='transform_restaurants',
    python_callable=submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/restaurants.py', "log" : False},
    dag=dag)

transform_review = PythonOperator(
    task_id='transform_review',
    python_callable=submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/review.py', "log" : False},
    dag=dag)


transform_tip = PythonOperator(
    task_id='transform_tip',
    python_callable = submit_to_emr,
    params={"file" : '/root/airflow/dags/transform/tip.py', "log" : False},
    dag=dag
)

quality_check = PythonOperator(
    task_id = 'transform_quality_check',
    python_callable = submit_to_emr,
    params = {"file" : '/root/airflow/dags/transform/transform_quality_check.py', "log" : True},
    dag=dag
)

cluster_check_task = ClusterCheckSensor(
    task_id='cluster_check',
    poke_interval=60,
    dag=dag)

done = PythonOperator(
    task_id = "done",
    python_callable=dag_done,
    dag=dag
)

#
cluster_check_task >> transform_city >> transform_codes
#
transform_codes >> transform_demographics >> transform_airport_weather
transform_codes >> transform_weather >> transform_airport_weather
transform_airport_weather >> quality_check >> done

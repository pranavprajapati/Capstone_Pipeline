import airflowlib.emr_lib as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.custom_s3 import ######
from airflow.operators.custom_plugin import S3DataExistsOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

s3data = 's3://psp-capstone/raw/'
s3bucket = 'psp-capstone'
yelp_prefix = 'raw/'

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('dag_cluster', concurrency=2, schedule_interval=None, default_args=default_args)
region = emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
# Also need to set Variable since other dags will require its reference
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='cluster', num_core_nodes=2)
    Variable.set("cluster_id", cluster_id)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id
    Variable.set("cluster_id", "na")
    Variable.set("dag_transform_state", "na")

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# Check all files exist on S3

check_business_s3  = S3DataExistsOperator(
    task_id='check_business_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_business.csv'
)

check_checkin_s3  = S3DataExistsOperator(
    task_id='check_checkin_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_checkin.csv'
)

check_review_s3  = S3DataExistsOperator(
    task_id='check_review_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_review.json'
)

check_tip_s3  = S3DataExistsOperator(
    task_id='check_tip_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_tip.csv'
)

check_user_s3  = S3DataExistsOperator(
    task_id='check_user_on_s3',
    dag=dag,
    aws_conn_id="s3_conn",
    bucket=s3bucket,
    prefix=lookup_prefix,
    key = 'yelp_academic_dataset_user.json'
)

start_operator >> check_business_s3 >> create_cluster >> wait_for_cluster_completion  >> >> terminate_cluster
start_operator >> check_checkin_s3 >> create_cluster >> wait_for_cluster_completion  >> >> terminate_cluster
start_operator >> check_review_s3 >> create_cluster >> wait_for_cluster_completion  >> >> terminate_cluster
start_operator >> check_tip_s3 >> create_cluster >> wait_for_cluster_completion  >> >> terminate_cluster
start_operator >> check_user_s3 >> create_cluster >> wait_for_cluster_completion  >> >> terminate_cluster

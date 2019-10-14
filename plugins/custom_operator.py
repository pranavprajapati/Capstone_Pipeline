
import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
import boto3, json, pprint, requests, textwrap, time, requests, os
from datetime import datetime

def get_region():
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response_json = r.json()
    return response_json.get('region')

def client(region_name):
    return boto3.client('emr', region_name=region_name)

def get_cluster_status(emr, cluster_id):
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['Status']['State']

region = get_region()
emr = client(region)



# Sensor operators keep executing at a time interval and succeed when
# a criteria is met and fail if and when they time out.

# Check if cluster is up and available
class ClusterCheckSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        return super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ti = context['ti']
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            logging.info(status)
            if status == 'WAITING':
                return True
            else:
                return False
        except Exception as e:
            logging.info(e)
            return False

# Check if the required files exist in the S3 bucket
class S3DataExistsOperator(BaseOperator):

    template_fields = ("prefix",)

    @apply_defaults
    def __init__(self,
                aws_conn_id="",
                bucket=None,
                prefix=None,
                key=None,
                wildcard_key = None,
                 *args, **kwargs):

        super(S3DataExistsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket=bucket
        self.prefix=prefix
        self.key=key
        self.wildcard_key=wildcard_key

    def execute(self, context):
        self.log.info(f'S3DataExistsOperator')
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        rendered_prefix = self.prefix.format(**context)

        success = s3_hook.check_for_bucket(self.bucket)
        if success:
            self.log.info("Found the bucket: {}".format(self.bucket))
        else:
            self.log.info("Invalid bucket: {}".format(self.bucket))
            raise FileNotFoundError("No S3 bucket named {}".format(self.bucket))

        success = s3_hook.check_for_prefix(prefix=rendered_prefix,
                                        delimiter='/',
                                        bucket_name=self.bucket)
        if success:
            self.log.info("Found the prefix: {}".format(rendered_prefix))
        else:
            self.log.info("Invalid prefix: {}".format(rendered_prefix))
            raise FileNotFoundError("No prefix named {}/{} ".format(self.bucket, rendered_prefix))


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [ClusterCheckSensor,S3DataExistsOperator]

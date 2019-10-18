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


# Check if the required files exist in the S3 bucket
class S3DataExistsOperator(BaseOperator):

    ui_color = '#A6E6A6'
    template_fields = ("prefix",)

    @apply_defaults
    def __init__(self,
                aws_conn_id="",
                bucket=None,
                prefix=None,
                key=None,
                 *args, **kwargs):

        super(S3DataExistsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket=bucket
        self.prefix=prefix
        self.key=key

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
    operators = [S3DataExistsOperator]
